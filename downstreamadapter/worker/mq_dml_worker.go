// Copyright 2022 PingCAP, Inc.
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

package worker

import (
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper/eventrouter"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper/topicmanager"
	"github.com/pingcap/ticdc/downstreamadapter/worker/producer"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/columnselector"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/tiflow/cdc/model"
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

// MQDMLWorker worker will send messages to the DML producer on a batch basis.
type MQDMLWorker struct {
	changeFeedID common.ChangeFeedID
	protocol     config.Protocol

	eventChan chan *commonEvent.DMLEvent
	rowChan   chan *commonEvent.MQRowEvent

	columnSelector *columnselector.ColumnSelectors
	// eventRouter used to route events to the right topic and partition.
	eventRouter *eventrouter.EventRouter
	// topicManager used to manage topics.
	// It is also responsible for creating topics.
	topicManager topicmanager.TopicManager
	encoderGroup codec.EncoderGroup

	// producer is used to send the messages to the MQ broker.
	producer producer.DMLProducer

	// statistics is used to record DML metrics.
	statistics *metrics.Statistics
}

// NewMQDMLWorker creates a dml flush worker for MQ
func NewMQDMLWorker(
	id common.ChangeFeedID,
	protocol config.Protocol,
	producer producer.DMLProducer,
	encoderGroup codec.EncoderGroup,
	columnSelector *columnselector.ColumnSelectors,
	eventRouter *eventrouter.EventRouter,
	topicManager topicmanager.TopicManager,
	statistics *metrics.Statistics,
) *MQDMLWorker {
	return &MQDMLWorker{
		changeFeedID:   id,
		protocol:       protocol,
		eventChan:      make(chan *commonEvent.DMLEvent, 32),
		rowChan:        make(chan *commonEvent.MQRowEvent, 32),
		encoderGroup:   encoderGroup,
		columnSelector: columnSelector,
		eventRouter:    eventRouter,
		topicManager:   topicManager,
		producer:       producer,
		statistics:     statistics,
	}
}

func (w *MQDMLWorker) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return w.producer.Run(ctx)
	})

	g.Go(func() error {
		return w.calculateKeyPartitions(ctx)
	})

	g.Go(func() error {
		return w.encoderGroup.Run(ctx)
	})

	g.Go(func() error {
		if w.protocol.IsBatchEncode() {
			return w.batchEncodeRun(ctx)
		}
		return w.nonBatchEncodeRun(ctx)
	})

	g.Go(func() error {
		return w.sendMessages(ctx)
	})
	return g.Wait()
}

func (w *MQDMLWorker) calculateKeyPartitions(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case event := <-w.eventChan:
			topic := w.eventRouter.GetTopicForRowChange(event.TableInfo)
			partitionNum, err := w.topicManager.GetPartitionNum(ctx, topic)
			if err != nil {
				return errors.Trace(err)
			}
			partitionGenerator := w.eventRouter.GetPartitionGenerator(event.TableInfo)
			selector := w.columnSelector.GetSelector(event.TableInfo.TableName.Schema, event.TableInfo.TableName.Table)
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
						TableInfo:      event.TableInfo,
						CommitTs:       event.CommitTs,
						Event:          row,
						Callback:       rowCallback,
						ColumnSelector: selector,
					},
				}
				w.addMQRowEvent(mqEvent)
			}
		}
	}
}

func (w *MQDMLWorker) AddDMLEvent(event *commonEvent.DMLEvent) {
	w.eventChan <- event
}

func (w *MQDMLWorker) addMQRowEvent(event *commonEvent.MQRowEvent) {
	w.rowChan <- event
}

// nonBatchEncodeRun add events to the encoder group immediately.
func (w *MQDMLWorker) nonBatchEncodeRun(ctx context.Context) error {
	log.Info("MQ sink non batch worker started",
		zap.String("namespace", w.changeFeedID.Namespace()),
		zap.String("changefeed", w.changeFeedID.Name()),
		zap.String("protocol", w.protocol.String()),
	)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case event, ok := <-w.rowChan:
			if !ok {
				log.Warn("MQ sink flush worker channel closed",
					zap.String("namespace", w.changeFeedID.Namespace()),
					zap.String("changefeed", w.changeFeedID.Name()))
				return nil
			}
			if err := w.encoderGroup.AddEvents(ctx, event.Key, &event.RowEvent); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

// batchEncodeRun collect messages into batch and add them to the encoder group.
func (w *MQDMLWorker) batchEncodeRun(ctx context.Context) error {
	log.Info("MQ sink batch worker started",
		zap.String("namespace", w.changeFeedID.Namespace()),
		zap.String("changefeed", w.changeFeedID.Name()),
		zap.String("protocol", w.protocol.String()),
	)

	namespace, changefeed := w.changeFeedID.Namespace(), w.changeFeedID.Name()
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
		msgCount, err := w.batch(ctx, msgsBuf, ticker)
		if err != nil {
			log.Error("MQ dml worker batch failed",
				zap.String("namespace", w.changeFeedID.Namespace()),
				zap.String("changefeed", w.changeFeedID.Name()),
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
		groupedMsgs := w.group(msgs)
		for key, msg := range groupedMsgs {
			if err = w.encoderGroup.AddEvents(ctx, key, msg...); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

// batch collects a batch of messages from w.msgChan into buffer.
// It returns the number of messages collected.
// Note: It will block until at least one message is received.
func (w *MQDMLWorker) batch(ctx context.Context, buffer []*commonEvent.MQRowEvent, ticker *time.Ticker) (int, error) {
	msgCount := 0
	maxBatchSize := len(buffer)
	// We need to receive at least one message or be interrupted,
	// otherwise it will lead to idling.
	select {
	case <-ctx.Done():
		return msgCount, ctx.Err()
	case msg, ok := <-w.rowChan:
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
		case msg, ok := <-w.rowChan:
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
func (w *MQDMLWorker) group(msgs []*commonEvent.MQRowEvent) map[model.TopicPartitionKey][]*commonEvent.RowEvent {
	groupedMsgs := make(map[model.TopicPartitionKey][]*commonEvent.RowEvent)
	for _, msg := range msgs {
		if _, ok := groupedMsgs[msg.Key]; !ok {
			groupedMsgs[msg.Key] = make([]*commonEvent.RowEvent, 0)
		}
		groupedMsgs[msg.Key] = append(groupedMsgs[msg.Key], &msg.RowEvent)
	}
	return groupedMsgs
}

func (w *MQDMLWorker) sendMessages(ctx context.Context) error {
	metricSendMessageDuration := metrics.WorkerSendMessageDuration.WithLabelValues(w.changeFeedID.Namespace(), w.changeFeedID.Name())
	defer metrics.WorkerSendMessageDuration.DeleteLabelValues(w.changeFeedID.Namespace(), w.changeFeedID.Name())

	var err error
	outCh := w.encoderGroup.Output()
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case future, ok := <-outCh:
			if !ok {
				log.Warn("MQ sink encoder's output channel closed",
					zap.String("namespace", w.changeFeedID.Namespace()),
					zap.String("changefeed", w.changeFeedID.Name()))
				return nil
			}
			if err = future.Ready(ctx); err != nil {
				return errors.Trace(err)
			}
			for _, message := range future.Messages {
				start := time.Now()
				if err = w.statistics.RecordBatchExecution(func() (int, int64, error) {
					message.SetPartitionKey(future.Key.PartitionKey)
					if err = w.producer.AsyncSendMessage(
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

func (w *MQDMLWorker) Close() {
	w.producer.Close()
}
