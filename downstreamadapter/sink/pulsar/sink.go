// Copyright 2024 PingCAP, Inc.
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
	"net/url"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type sink struct {
	changefeedID commonType.ChangeFeedID

	dmlProducer dmlProducer
	ddlProducer ddlProducer

	comp       component
	statistics *metrics.Statistics

	protocol      config.Protocol
	partitionRule helper.DDLDispatchRule

	// isNormal indicate whether the sink is in the normal state.
	isNormal *atomic.Bool
	ctx      context.Context

	tableSchemaStore *commonEvent.TableSchemaStore
	checkpointTsChan chan uint64
	eventChan        chan *commonEvent.DMLEvent
	rowChan          chan *commonEvent.MQRowEvent
}

func (s *sink) SinkType() commonType.SinkType {
	return commonType.PulsarSinkType
}

func Verify(ctx context.Context, changefeedID commonType.ChangeFeedID, uri *url.URL, sinkConfig *config.SinkConfig) error {
	comp, _, err := newPulsarSinkComponent(ctx, changefeedID, uri, sinkConfig)
	defer comp.close()
	return err
}

func New(
	ctx context.Context, changefeedID commonType.ChangeFeedID, sinkURI *url.URL, sinkConfig *config.SinkConfig,
) (*sink, error) {
	comp, protocol, err := newPulsarSinkComponent(ctx, changefeedID, sinkURI, sinkConfig)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			comp.close()
		}
	}()

	failpointCh := make(chan error, 1)
	statistics := metrics.NewStatistics(changefeedID, "pulsar")
	dmlProducer, err := newDMLProducers(changefeedID, comp, failpointCh)
	if err != nil {
		return nil, err
	}

	ddlProducer, err := newDDLProducers(changefeedID, comp, sinkConfig)
	if err != nil {
		return nil, err
	}

	return &sink{
		changefeedID: changefeedID,
		dmlProducer:  dmlProducer,
		ddlProducer:  ddlProducer,

		checkpointTsChan: make(chan uint64, 16),
		eventChan:        make(chan *commonEvent.DMLEvent, 32),
		rowChan:          make(chan *commonEvent.MQRowEvent, 32),

		protocol:      protocol,
		partitionRule: helper.GetDDLDispatchRule(protocol),
		comp:          comp,
		statistics:    statistics,
		isNormal:      atomic.NewBool(true),
		ctx:           ctx,
	}, nil
}

func (s *sink) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return s.sendDMLEvent(ctx)
	})
	g.Go(func() error {
		return s.sendCheckpoint(ctx)
	})
	err := g.Wait()
	s.isNormal.Store(false)
	return errors.Trace(err)
}

func (s *sink) IsNormal() bool {
	return s.isNormal.Load()
}

func (s *sink) AddDMLEvent(event *commonEvent.DMLEvent) {
	s.eventChan <- event
}

func (s *sink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	var err error
	switch v := event.(type) {
	case *commonEvent.DDLEvent:
		err = s.sendDDLEvent(v)
	default:
		log.Error("pulsar sink doesn't support this type of block event",
			zap.String("namespace", s.changefeedID.Keyspace()),
			zap.String("changefeed", s.changefeedID.Name()),
			zap.String("eventType", commonEvent.TypeToString(event.GetType())))
		return errors.ErrInvalidEventType.GenWithStackByArgs(commonEvent.TypeToString(event.GetType()))
	}
	if err != nil {
		s.isNormal.Store(false)
		return err
	}
	event.PostFlush()
	return nil
}

func (s *sink) sendDDLEvent(event *commonEvent.DDLEvent) error {
	for _, e := range event.GetEvents() {
		message, err := s.comp.encoder.EncodeDDLEvent(e)
		if err != nil {
			return err
		}
		topic := s.comp.eventRouter.GetTopicForDDL(e)
		// Notice: We must call GetPartitionNum here,
		// which will be responsible for automatically creating topics when they don't exist.
		// If it is not called here and kafka has `auto.create.topics.enable` turned on,
		// then the auto-created topic will not be created as configured by ticdc.
		_, err = s.comp.topicManager.GetPartitionNum(s.ctx, topic)
		if err != nil {
			return err
		}
		if s.partitionRule == helper.PartitionAll {
			err = s.statistics.RecordDDLExecution(func() error {
				return s.ddlProducer.syncBroadcastMessage(s.ctx, topic, message)
			})
		} else {
			err = s.statistics.RecordDDLExecution(func() error {
				return s.ddlProducer.syncSendMessage(s.ctx, topic, message)
			})
		}
		if err != nil {
			return err
		}
	}
	log.Info("pulsar sink send DDL event",
		zap.String("keyspace", s.changefeedID.Keyspace()), zap.String("changefeed", s.changefeedID.Name()),
		zap.Any("commitTs", event.GetCommitTs()), zap.Any("event", event.GetDDLQuery()))
	return nil
}

func (s *sink) AddCheckpointTs(ts uint64) {
	select {
	case s.checkpointTsChan <- ts:
	case <-s.ctx.Done():
		return
		// We can just drop the checkpoint ts if the channel is full to avoid blocking since the  checkpointTs will come indefinitely
	default:
	}
}

func (s *sink) SetTableSchemaStore(tableSchemaStore *commonEvent.TableSchemaStore) {
	s.tableSchemaStore = tableSchemaStore
}

func (s *sink) sendCheckpoint(ctx context.Context) error {
	checkpointTsMessageDuration := metrics.CheckpointTsMessageDuration.WithLabelValues(s.changefeedID.Keyspace(), s.changefeedID.Name())
	checkpointTsMessageCount := metrics.CheckpointTsMessageCount.WithLabelValues(s.changefeedID.Keyspace(), s.changefeedID.Name())

	defer func() {
		metrics.CheckpointTsMessageDuration.DeleteLabelValues(s.changefeedID.Keyspace(), s.changefeedID.Name())
		metrics.CheckpointTsMessageCount.DeleteLabelValues(s.changefeedID.Keyspace(), s.changefeedID.Name())
	}()
	var (
		msg *common.Message
		err error
	)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case ts, ok := <-s.checkpointTsChan:
			if !ok {
				log.Info("pulsar sink checkpoint channel closed",
					zap.String("keyspace", s.changefeedID.Keyspace()),
					zap.String("changefeed", s.changefeedID.Name()))
				return nil
			}

			start := time.Now()
			msg, err = s.comp.encoder.EncodeCheckpointEvent(ts)
			if err != nil {
				return errors.Trace(err)
			}

			if msg == nil {
				continue
			}

			tableNames := s.getAllTableNames(ts)
			// NOTICE: When there are no tables to replicate,
			// we need to send checkpoint ts to the default topic.
			// This will be compatible with the old behavior.
			if len(tableNames) == 0 {
				topic := s.comp.eventRouter.GetDefaultTopic()
				_, err = s.comp.topicManager.GetPartitionNum(ctx, topic)
				if err != nil {
					return errors.Trace(err)
				}
				err = s.ddlProducer.syncBroadcastMessage(ctx, topic, msg)
				if err != nil {
					return errors.Trace(err)
				}
			} else {
				topics := s.comp.eventRouter.GetActiveTopics(tableNames)
				for _, topic := range topics {
					_, err = s.comp.topicManager.GetPartitionNum(ctx, topic)
					if err != nil {
						return errors.Trace(err)
					}
					err = s.ddlProducer.syncBroadcastMessage(ctx, topic, msg)
					if err != nil {
						return errors.Trace(err)
					}
				}
			}

			checkpointTsMessageCount.Inc()
			checkpointTsMessageDuration.Observe(time.Since(start).Seconds())
		}
	}
}

func (s *sink) sendDMLEvent(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return s.calculateKeyPartitions(ctx)
	})
	g.Go(func() error {
		return s.comp.encoderGroup.Run(ctx)
	})
	g.Go(func() error {
		if s.protocol.IsBatchEncode() {
			return s.batchEncodeRun(ctx)
		}
		return s.nonBatchEncodeRun(ctx)
	})
	g.Go(func() error {
		return s.sendMessages(ctx)
	})
	g.Go(func() error {
		return s.dmlProducer.run(ctx)
	})
	return g.Wait()
}

func (s *sink) calculateKeyPartitions(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case event := <-s.eventChan:
			schema := event.TableInfo.GetSchemaName()
			table := event.TableInfo.GetTableName()
			topic := s.comp.eventRouter.GetTopicForRowChange(schema, table)
			partitionNum, err := s.comp.topicManager.GetPartitionNum(ctx, topic)
			if err != nil {
				return errors.Trace(err)
			}

			partitionGenerator := s.comp.eventRouter.GetPartitionGenerator(schema, table)
			selector := s.comp.columnSelector.Get(schema, table)
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
					event.Rewind()
					break
				}

				index, key, err := partitionGenerator.GeneratePartitionIndexAndKey(&row, partitionNum, event.TableInfo, event.CommitTs)
				if err != nil {
					return errors.Trace(err)
				}

				mqEvent := &commonEvent.MQRowEvent{
					Key: commonEvent.TopicPartitionKey{
						Topic:          topic,
						Partition:      index,
						PartitionKey:   key,
						TotalPartition: partitionNum,
					},
					RowEvent: commonEvent.RowEvent{
						PhysicalTableID: event.PhysicalTableID,
						TableInfo:       event.TableInfo,
						StartTs:         event.StartTs,
						CommitTs:        event.CommitTs,
						Event:           row,
						Callback:        rowCallback,
						ColumnSelector:  selector,
						Checksum:        row.Checksum,
					},
				}
				s.rowChan <- mqEvent
			}
		}
	}
}

const (
	// batchSize is the maximum size of the number of messages in a batch.
	batchSize = 2048
	// batchInterval is the interval of the worker to collect a batch of messages.
	// It shouldn't be too large, otherwise it will lead to a high latency.
	batchInterval = 15 * time.Millisecond
)

// batchEncodeRun collect messages into batch and add them to the encoder group.
func (s *sink) batchEncodeRun(ctx context.Context) error {
	keyspace, changefeed := s.changefeedID.Keyspace(), s.changefeedID.Name()
	metricBatchDuration := metrics.WorkerBatchDuration.WithLabelValues(keyspace, changefeed)
	metricBatchSize := metrics.WorkerBatchSize.WithLabelValues(keyspace, changefeed)
	defer func() {
		metrics.WorkerBatchDuration.DeleteLabelValues(keyspace, changefeed)
		metrics.WorkerBatchSize.DeleteLabelValues(keyspace, changefeed)
	}()

	ticker := time.NewTicker(batchInterval)
	defer ticker.Stop()
	msgsBuf := make([]*commonEvent.MQRowEvent, batchSize)
	for {
		start := time.Now()
		msgCount, err := s.batch(ctx, msgsBuf, ticker)
		if err != nil {
			log.Error("pulsar sink batch dml events failed",
				zap.String("keyspace", s.changefeedID.Keyspace()),
				zap.String("changefeed", s.changefeedID.Name()),
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
		groupedMsgs := s.group(msgs)
		for key, msg := range groupedMsgs {
			if err = s.comp.encoderGroup.AddEvents(ctx, key, msg...); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

// batch collects a batch of messages from w.msgChan into buffer.
// It returns the number of messages collected.
// Note: It will block until at least one message is received.
func (s *sink) batch(ctx context.Context, buffer []*commonEvent.MQRowEvent, ticker *time.Ticker) (int, error) {
	msgCount := 0
	maxBatchSize := len(buffer)
	// We need to receive at least one message or be interrupted,
	// otherwise it will lead to idling.
	select {
	case <-ctx.Done():
		return msgCount, ctx.Err()
	case msg, ok := <-s.rowChan:
		if !ok {
			log.Info("pulsar sink row event channel closed",
				zap.String("keyspace", s.changefeedID.Keyspace()),
				zap.String("changefeed", s.changefeedID.Name()))
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
		case msg, ok := <-s.rowChan:
			if !ok {
				log.Info("pulsar sink row event channel closed",
					zap.String("keyspace", s.changefeedID.Keyspace()),
					zap.String("changefeed", s.changefeedID.Name()))
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
func (s *sink) group(msgs []*commonEvent.MQRowEvent) map[commonEvent.TopicPartitionKey][]*commonEvent.RowEvent {
	groupedMsgs := make(map[commonEvent.TopicPartitionKey][]*commonEvent.RowEvent)
	for _, msg := range msgs {
		if _, ok := groupedMsgs[msg.Key]; !ok {
			groupedMsgs[msg.Key] = make([]*commonEvent.RowEvent, 0)
		}
		groupedMsgs[msg.Key] = append(groupedMsgs[msg.Key], &msg.RowEvent)
	}
	return groupedMsgs
}

// nonBatchEncodeRun add events to the encoder group immediately.
func (s *sink) nonBatchEncodeRun(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case event, ok := <-s.rowChan:
			if !ok {
				log.Info("pulsar sink row event channel closed",
					zap.String("keyspace", s.changefeedID.Keyspace()),
					zap.String("changefeed", s.changefeedID.Name()))
				return nil
			}
			if err := s.comp.encoderGroup.AddEvents(ctx, event.Key, &event.RowEvent); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (s *sink) sendMessages(ctx context.Context) error {
	metricSendMessageDuration := metrics.WorkerSendMessageDuration.WithLabelValues(s.changefeedID.Keyspace(), s.changefeedID.Name())
	defer metrics.WorkerSendMessageDuration.DeleteLabelValues(s.changefeedID.Keyspace(), s.changefeedID.Name())

	var err error
	outCh := s.comp.encoderGroup.Output()
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case future, ok := <-outCh:
			if !ok {
				log.Info("pulsar sink encoder group output channel closed",
					zap.String("keyspace", s.changefeedID.Keyspace()),
					zap.String("changefeed", s.changefeedID.Name()))
				return nil
			}
			if err = future.Ready(ctx); err != nil {
				return errors.Trace(err)
			}
			for _, message := range future.Messages {
				start := time.Now()
				if err = s.statistics.RecordBatchExecution(func() (int, int64, error) {
					message.SetPartitionKey(future.Key.PartitionKey)
					if err = s.dmlProducer.asyncSendMessage(ctx, future.Key.Topic, message); err != nil {
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

func (s *sink) getAllTableNames(ts uint64) []*commonEvent.SchemaTableName {
	if s.tableSchemaStore == nil {
		log.Warn("kafka sink table schema store is not set",
			zap.String("keyspace", s.changefeedID.Keyspace()),
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Uint64("ts", ts))
		return nil
	}
	return s.tableSchemaStore.GetAllTableNames(ts)
}

func (s *sink) Close(_ bool) {
	s.ddlProducer.close()
	s.dmlProducer.close()
	s.comp.close()
	s.statistics.Close()
}
