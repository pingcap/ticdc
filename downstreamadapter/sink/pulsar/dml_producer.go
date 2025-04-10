// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package pulsar

import (
	"context"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	lru "github.com/hashicorp/golang-lru"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter"
	"github.com/pingcap/ticdc/downstreamadapter/sink/topicmanager"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/columnselector"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	// batchSize is the maximum size of the number of messages in a batch.
	batchSize = 2048
	// batchInterval is the interval of the worker to collect a batch of messages.
	// It shouldn't be too large, otherwise it will lead to a high latency.
	batchInterval = 15 * time.Millisecond
)

// dmlProducers is used to send messages to pulsar.
type dmlProducers struct {
	changefeedID commonType.ChangeFeedID
	// We hold the client to make close operation faster.
	// Please see the comment of Close().
	client pulsar.Client
	// producers is used to send messages to pulsar.
	// One topic only use one producer , so we want to have many topics but use less memory,
	// lru is a good idea to solve this question.
	// support multiple topics
	producers *lru.Cache

	// closedMu is used to protect `closed`.
	// We need to ensure that closed producers are never written to.
	closedMu sync.RWMutex
	// closed is used to indicate whether the producer is closed.
	// We also use it to guard against double closes.
	closed bool

	// failpointCh is used to inject failpoints to the run loop.
	// Only used in test.
	failpointCh chan error

	pConfig   *config.PulsarConfig
	eventChan chan *commonEvent.DMLEvent
	rowChan   chan *commonEvent.MQRowEvent

	statistics *metrics.Statistics

	encoderGroup codec.EncoderGroup
	protocol     config.Protocol

	// eventRouter used to route events to the right topic and partition.
	eventRouter *eventrouter.EventRouter
	// topicManager used to manage topics.
	// It is also responsible for creating topics.
	topicManager topicmanager.TopicManager

	columnSelector *columnselector.ColumnSelectors
}

// newDMLProducers creates a new pulsar producer.
func newDMLProducers(
	changefeedID commonType.ChangeFeedID,
	client pulsar.Client,
	sinkConfig *config.SinkConfig,
	failpointCh chan error,
) (*dmlProducers, error) {
	log.Info("Creating pulsar DML producer ...",
		zap.String("namespace", changefeedID.Namespace()),
		zap.String("changefeed", changefeedID.ID().String()))
	start := time.Now()

	var pulsarConfig *config.PulsarConfig
	if sinkConfig.PulsarConfig == nil {
		log.Error("new pulsar DML producer fail,sink:pulsar config is empty")
		return nil, cerror.ErrPulsarInvalidConfig.
			GenWithStackByArgs("pulsar config is empty")
	}

	pulsarConfig = sinkConfig.PulsarConfig
	defaultTopicName := pulsarConfig.GetDefaultTopicName()
	defaultProducer, err := newProducer(pulsarConfig, client, defaultTopicName)
	if err != nil {
		go client.Close()
		return nil, cerror.WrapError(cerror.ErrPulsarNewProducer, err)
	}
	producerCacheSize := config.DefaultPulsarProducerCacheSize
	if pulsarConfig != nil && pulsarConfig.PulsarProducerCacheSize != nil {
		producerCacheSize = int(*pulsarConfig.PulsarProducerCacheSize)
	}

	producers, err := lru.NewWithEvict(producerCacheSize, func(key interface{}, value interface{}) {
		// this is call when lru Remove producer or auto remove producer
		pulsarProducer, ok := value.(pulsar.Producer)
		if ok && pulsarProducer != nil {
			pulsarProducer.Close()
		}
	})
	if err != nil {
		go client.Close()
		return nil, cerror.WrapError(cerror.ErrPulsarNewProducer, err)
	}

	producers.Add(defaultTopicName, defaultProducer)

	p := &dmlProducers{
		changefeedID: changefeedID,
		client:       client,
		producers:    producers,
		pConfig:      pulsarConfig,
		closed:       false,
		failpointCh:  failpointCh,
	}
	log.Info("Pulsar DML producer created", zap.Stringer("changefeed", p.changefeedID),
		zap.Duration("duration", time.Since(start)))
	return p, nil
}

// AsyncSendMessage  Async send one message
func (p *dmlProducers) AsyncSendMessage(
	ctx context.Context, topic string,
	partition int32, message *common.Message,
) error {
	// wrapperSchemaAndTopic(message)

	// We have to hold the lock to avoid writing to a closed producer.
	// Close may be blocked for a long time.
	p.closedMu.RLock()
	defer p.closedMu.RUnlock()

	// If producers are closed, we should skip the message and return an error.
	if p.closed {
		return cerror.ErrPulsarProducerClosed.GenWithStackByArgs()
	}
	failpoint.Inject("PulsarSinkAsyncSendError", func() {
		// simulate sending message to input channel successfully but flushing
		// message to Pulsar meets error
		log.Info("PulsarSinkAsyncSendError error injected", zap.String("namespace", p.changefeedID.Namespace()),
			zap.String("changefeed", p.changefeedID.ID().String()))
		p.failpointCh <- errors.New("pulsar sink injected error")
		failpoint.Return(nil)
	})
	data := &pulsar.ProducerMessage{
		Payload: message.Value,
		Key:     message.GetPartitionKey(),
	}

	producer, err := p.getProducerByTopic(topic)
	if err != nil {
		return err
	}

	// if for stress test record , add count to message callback function

	producer.SendAsync(ctx, data,
		func(id pulsar.MessageID, m *pulsar.ProducerMessage, err error) {
			// fail
			if err != nil {
				e := cerror.WrapError(cerror.ErrPulsarAsyncSendMessage, err)
				log.Error("Pulsar DML producer async send error",
					zap.String("namespace", p.changefeedID.Namespace()),
					zap.String("changefeed", p.changefeedID.ID().String()),
					zap.Int("messageSize", len(m.Payload)),
					zap.String("topic", topic),
					zap.Error(err))
				// mq.IncPublishedDMLFail(topic, p.id.ID().String(), message.GetSchema())
				// use this select to avoid send error to a closed channel
				// the ctx will always be called before the errChan is closed
				select {
				case <-ctx.Done():
					return
				default:
					if e != nil {
					}
					log.Warn("Error channel is full in pulsar DML producer",
						zap.Stringer("changefeed", p.changefeedID), zap.Error(e))
				}
			} else if message.Callback != nil {
				// success
				message.Callback()
				// mq.IncPublishedDMLSuccess(topic, p.id.ID().String(), message.GetSchema())
			}
		})

	// mq.IncPublishedDMLCount(topic, p.id.ID().String(), message.GetSchema())

	return nil
}

func (p *dmlProducers) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return p.producer.Run(ctx)
	})

	g.Go(func() error {
		return p.calculateKeyPartitions(ctx)
	})

	g.Go(func() error {
		return p.encoderGroup.Run(ctx)
	})

	g.Go(func() error {
		if p.protocol.IsBatchEncode() {
			return p.batchEncodeRun(ctx)
		}
		return p.nonBatchEncodeRun(ctx)
	})

	g.Go(func() error {
		return p.sendMessages(ctx)
	})
	return g.Wait()
}

func (p *dmlProducers) calculateKeyPartitions(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case event := <-p.eventChan:
			topic := p.eventRouter.GetTopicForRowChange(event.TableInfo)
			partitionNum, err := p.topicManager.GetPartitionNum(ctx, topic)
			if err != nil {
				return errors.Trace(err)
			}

			schema := event.TableInfo.GetSchemaName()
			table := event.TableInfo.GetTableName()
			partitionGenerator := p.eventRouter.GetPartitionGenerator(schema, table)
			selector := p.columnSelector.GetSelector(schema, table)
			toRowCallback := func(postTxnFlushed []func(), totalCount uint64) func() {
				var calledCount atomic.Uint64
				// The callback of the last row will trigger the callback of the txn.
				return func() {
					if calledCount.Inc() == totalCount {
						for _, callback := range postTxnFlushed {
							callback()
						}
					}
				}
			}

			rowsCount := uint64(event.Len())
			rowCallback := toRowCallback(event.PostTxnFlushed, rowsCount)

			for {
				row, ok := event.GetNextRow()
				if !ok {
					break
				}

				index, key, err := partitionGenerator.GeneratePartitionIndexAndKey(&row, partitionNum, event.TableInfo, event.CommitTs)
				if err != nil {
					return errors.Trace(err)
				}

				mqEvent := &commonEvent.MQRowEvent{
					Key: model.TopicPartitionKey{
						Topic:          topic,
						Partition:      index,
						PartitionKey:   key,
						TotalPartition: partitionNum,
					},
					RowEvent: commonEvent.RowEvent{
						PhysicalTableID: event.PhysicalTableID,
						TableInfo:       event.TableInfo,
						CommitTs:        event.CommitTs,
						Event:           row,
						Callback:        rowCallback,
						ColumnSelector:  selector,
					},
				}
				p.addMQRowEvent(mqEvent)
			}
		}
	}
}

func (p *dmlProducers) addMQRowEvent(event *commonEvent.MQRowEvent) {
	p.rowChan <- event
}

// batchEncodeRun collect messages into batch and add them to the encoder group.
func (p *dmlProducers) batchEncodeRun(ctx context.Context) error {
	log.Info("MQ sink batch worker started",
		zap.String("namespace", p.changefeedID.Namespace()),
		zap.String("changefeed", p.changefeedID.Name()),
		zap.String("protocol", p.protocol.String()),
	)

	namespace, changefeed := p.changefeedID.Namespace(), p.changefeedID.Name()
	metricBatchDuration := metrics.WorkerBatchDuration.WithLabelValues(namespace, changefeed)
	metricBatchSize := metrics.WorkerBatchSize.WithLabelValues(namespace, changefeed)
	defer func() {
		metrics.WorkerBatchDuration.DeleteLabelValues(namespace, changefeed)
		metrics.WorkerBatchSize.DeleteLabelValues(namespace, changefeed)
	}()

	ticker := time.NewTicker(batchInterval)
	defer ticker.Stop()
	msgsBuf := make([]*commonEvent.MQRowEvent, batchSize)
	for {
		start := time.Now()
		msgCount, err := p.batch(ctx, msgsBuf, ticker)
		if err != nil {
			log.Error("MQ dml worker batch failed",
				zap.String("namespace", p.changefeedID.Namespace()),
				zap.String("changefeed", p.changefeedID.Name()),
				zap.Error(err))
			return errors.Trace(err)
		}
		if msgCount == 0 {
			continue
		}

		metricBatchSize.Observe(float64(msgCount))
		metricBatchDuration.Observe(time.Since(start).Seconds())

		msgs := msgsBuf[:msgCount]
		// Group messages by its TopicPartitionKey before adding them to the encoder group.
		groupedMsgs := p.group(msgs)
		for key, msg := range groupedMsgs {
			if err = p.encoderGroup.AddEvents(ctx, key, msg...); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

// batch collects a batch of messages from w.msgChan into buffer.
// It returns the number of messages collected.
// Note: It will block until at least one message is received.
func (p *dmlProducers) batch(ctx context.Context, buffer []*commonEvent.MQRowEvent, ticker *time.Ticker) (int, error) {
	msgCount := 0
	maxBatchSize := len(buffer)
	// We need to receive at least one message or be interrupted,
	// otherwise it will lead to idling.
	select {
	case <-ctx.Done():
		return msgCount, ctx.Err()
	case msg, ok := <-p.rowChan:
		if !ok {
			log.Warn("MQ sink flush worker channel closed")
			return msgCount, nil
		}

		buffer[msgCount] = msg
		msgCount++
	}

	// Reset the ticker to start a new batching.
	// We need to stop batching when the interval is reached.
	ticker.Reset(batchInterval)
	for {
		select {
		case <-ctx.Done():
			return msgCount, ctx.Err()
		case msg, ok := <-p.rowChan:
			if !ok {
				log.Warn("MQ sink flush worker channel closed")
				return msgCount, nil
			}

			buffer[msgCount] = msg
			msgCount++

			if msgCount >= maxBatchSize {
				return msgCount, nil
			}
		case <-ticker.C:
			return msgCount, nil
		}
	}
}

// group groups messages by its key.
func (p *dmlProducers) group(msgs []*commonEvent.MQRowEvent) map[model.TopicPartitionKey][]*commonEvent.RowEvent {
	groupedMsgs := make(map[model.TopicPartitionKey][]*commonEvent.RowEvent)
	for _, msg := range msgs {
		if _, ok := groupedMsgs[msg.Key]; !ok {
			groupedMsgs[msg.Key] = make([]*commonEvent.RowEvent, 0)
		}
		groupedMsgs[msg.Key] = append(groupedMsgs[msg.Key], &msg.RowEvent)
	}
	return groupedMsgs
}

// nonBatchEncodeRun add events to the encoder group immediately.
func (p *dmlProducers) nonBatchEncodeRun(ctx context.Context) error {
	log.Info("MQ sink non batch worker started",
		zap.String("namespace", p.changefeedID.Namespace()),
		zap.String("changefeed", p.changefeedID.Name()),
		zap.String("protocol", p.protocol.String()),
	)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case event, ok := <-p.rowChan:
			if !ok {
				log.Warn("MQ sink flush worker channel closed",
					zap.String("namespace", p.changefeedID.Namespace()),
					zap.String("changefeed", p.changefeedID.Name()))
				return nil
			}
			if err := p.encoderGroup.AddEvents(ctx, event.Key, &event.RowEvent); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (p *dmlProducers) sendMessages(ctx context.Context) error {
	metricSendMessageDuration := metrics.WorkerSendMessageDuration.WithLabelValues(p.changefeedID.Namespace(), p.changefeedID.Name())
	defer metrics.WorkerSendMessageDuration.DeleteLabelValues(p.changefeedID.Namespace(), p.changefeedID.Name())

	var err error
	outCh := p.encoderGroup.Output()
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case future, ok := <-outCh:
			if !ok {
				log.Warn("MQ sink encoder's output channel closed",
					zap.String("namespace", p.changefeedID.Namespace()),
					zap.String("changefeed", p.changefeedID.Name()))
				return nil
			}
			if err = future.Ready(ctx); err != nil {
				return errors.Trace(err)
			}
			for _, message := range future.Messages {
				start := time.Now()
				if err = p.statistics.RecordBatchExecution(func() (int, int64, error) {
					message.SetPartitionKey(future.Key.PartitionKey)
					if err = p.producer.AsyncSendMessage(
						ctx,
						future.Key.Topic,
						future.Key.Partition,
						message); err != nil {
						return 0, 0, err
					}
					return message.GetRowsCount(), int64(message.Length()), nil
				}); err != nil {
					return errors.Trace(err)
				}
				metricSendMessageDuration.Observe(time.Since(start).Seconds())
			}
		}
	}
}

func (p *dmlProducers) Close() { // We have to hold the lock to synchronize closing with writing.
	p.closedMu.Lock()
	defer p.closedMu.Unlock()
	// If the producer has already been closed, we should skip this close operation.
	if p.closed {
		// We need to guard against double closing the clients,
		// which could lead to panic.
		log.Warn("Pulsar DML producer already closed",
			zap.String("namespace", p.changefeedID.Namespace()),
			zap.String("changefeed", p.changefeedID.ID().String()))
		return
	}
	close(p.failpointCh)
	p.closed = true
	start := time.Now()
	keys := p.producers.Keys()
	for _, topic := range keys {
		p.producers.Remove(topic) // callback func will be called
		topicName, _ := topic.(string)
		log.Info("Async client closed in pulsar DML producer",
			zap.Duration("duration", time.Since(start)),
			zap.String("namespace", p.changefeedID.Namespace()),
			zap.String("changefeed", p.changefeedID.ID().String()), zap.String("topic", topicName))
	}
	p.client.Close()
}

func (p *dmlProducers) getProducer(topic string) (pulsar.Producer, bool) {
	target, ok := p.producers.Get(topic)
	if ok {
		producer, ok := target.(pulsar.Producer)
		if ok {
			return producer, true
		}
	}
	return nil, false
}

// getProducerByTopic get producer by topicName,
// if not exist, it will create a producer with topicName, and set in LRU cache
// more meta info at dmlProducers's producers
func (p *dmlProducers) getProducerByTopic(topicName string) (producer pulsar.Producer, err error) {
	getProducer, ok := p.getProducer(topicName)
	if ok && getProducer != nil {
		return getProducer, nil
	}

	if !ok { // create a new producer for the topicName
		producer, err = newProducer(p.pConfig, p.client, topicName)
		if err != nil {
			return nil, err
		}
		p.producers.Add(topicName, producer)
	}

	return producer, nil
}

func (p *dmlProducers) AddDMLEvent(event *commonEvent.DMLEvent) {
	p.eventChan <- event
}

// wrapperSchemaAndTopic wrapper schema and topic
// func wrapperSchemaAndTopic(m *common.Message) {
// 	if m.Schema == nil {
// 		if m.Protocol == config.ProtocolMaxwell {
// 			mx := &maxwellMessage{}
// 			err := json.Unmarshal(m.Value, mx)
// 			if err != nil {
// 				log.Error("unmarshal maxwell message failed", zap.Error(err))
// 				return
// 			}
// 			if len(mx.Database) > 0 {
// 				m.Schema = &mx.Database
// 			}
// 			if len(mx.Table) > 0 {
// 				m.Table = &mx.Table
// 			}
// 		}
// 		if m.Protocol == config.ProtocolCanal { // canal protocol set multi schemas in one topic
// 			m.Schema = str2Pointer("multi_schema")
// 		}
// 	}
// }

// // maxwellMessage is the message format of maxwell
// type maxwellMessage struct {
// 	Database string `json:"database"`
// 	Table    string `json:"table"`
// }

// // str2Pointer returns the pointer of the string.
// func str2Pointer(str string) *string {
// 	return &str
// }
