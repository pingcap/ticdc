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

package kafka

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
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"github.com/pingcap/ticdc/utils/chann"
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

type sink struct {
	changefeedID commonType.ChangeFeedID

	dmlProducer      kafka.AsyncProducer
	ddlProducer      kafka.SyncProducer
	metricsCollector kafka.MetricsCollector

	comp       components
	statistics *metrics.Statistics

	protocol      config.Protocol
	partitionRule helper.DDLDispatchRule

	checkpointChan   chan uint64
	tableSchemaStore *util.TableSchemaStore

	eventChan *chann.UnlimitedChannel[*commonEvent.DMLEvent, any]
	rowChan   *chann.UnlimitedChannel[*commonEvent.MQRowEvent, any]

	// isNormal indicate whether the sink is in the normal state.
	isNormal *atomic.Bool
	ctx      context.Context
}

func (s *sink) SinkType() commonType.SinkType {
	return commonType.KafkaSinkType
}

func Verify(ctx context.Context, changefeedID commonType.ChangeFeedID, uri *url.URL, sinkConfig *config.SinkConfig) error {
	comp, _, err := newKafkaSinkComponent(ctx, changefeedID, uri, sinkConfig)
	defer comp.close()
	return err
}

func New(
	ctx context.Context, changefeedID commonType.ChangeFeedID, sinkURI *url.URL, sinkConfig *config.SinkConfig,
) (*sink, error) {
	comp, protocol, err := newKafkaSinkComponent(ctx, changefeedID, sinkURI, sinkConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		if err != nil {
			comp.close()
		}
	}()

	statistics := metrics.NewStatistics(changefeedID, "sink")
	asyncProducer, err := comp.factory.AsyncProducer()
	if err != nil {
		return nil, err
	}

	syncProducer, err := comp.factory.SyncProducer()
	if err != nil {
		return nil, err
	}

	return &sink{
		changefeedID:     changefeedID,
		dmlProducer:      asyncProducer,
		ddlProducer:      syncProducer,
		metricsCollector: comp.factory.MetricsCollector(comp.adminClient),

		partitionRule: helper.GetDDLDispatchRule(protocol),
		protocol:      protocol,
		comp:          comp,
		statistics:    statistics,

		checkpointChan: make(chan uint64, 16),
		eventChan:      chann.NewUnlimitedChannelDefault[*commonEvent.DMLEvent](),
		rowChan:        chann.NewUnlimitedChannelDefault[*commonEvent.MQRowEvent](),

		isNormal: atomic.NewBool(true),
		ctx:      ctx,
	}, nil
}

func (s *sink) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return s.sendCheckpoint(ctx)
	})
	g.Go(func() error {
		return s.dmlProducer.AsyncRunCallback(ctx)
	})
	g.Go(func() error {
		return s.sendDMLEvent(ctx)
	})
	g.Go(func() error {
		s.metricsCollector.Run(ctx)
		return nil
	})
	err := g.Wait()
	s.isNormal.Store(false)
	return errors.Trace(err)
}

func (s *sink) IsNormal() bool {
	return s.isNormal.Load()
}

func (s *sink) AddDMLEvent(event *commonEvent.DMLEvent) {
	s.eventChan.Push(event)
}

func (s *sink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	var err error
	switch v := event.(type) {
	case *commonEvent.DDLEvent:
		err = s.sendDDLEvent(v)
	default:
		log.Panic("kafka sink doesn't support this type of block event",
			zap.String("namespace", s.changefeedID.Namespace()),
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Any("eventType", event.GetType()))
	}
	if err != nil {
		s.isNormal.Store(false)
		return err
	}
	event.PostFlush()
	return nil
}

func (s *sink) close() {
	s.eventChan.Close()
	s.rowChan.Close()
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
		// UnlimitedChannel will block when there is no event, they cannot dirrectly find ctx.Done()
		// Thus, we need to close the channel when the context is done
		defer s.close()
		return s.sendMessages(ctx)
	})
	return g.Wait()
}

func (s *sink) calculateKeyPartitions(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		default:
			event, ok := s.eventChan.Get()
			if !ok {
				log.Info("kafka sink event channel closed",
					zap.String("namespace", s.changefeedID.Namespace()),
					zap.String("changefeed", s.changefeedID.Name()))
				return nil
			}
			schema := event.TableInfo.GetSchemaName()
			table := event.TableInfo.GetTableName()
			topic := s.comp.eventRouter.GetTopicForRowChange(schema, table)
			partitionNum, err := s.comp.topicManager.GetPartitionNum(ctx, topic)
			if err != nil {
				return err
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
						CommitTs:        event.CommitTs,
						Event:           row,
						Callback:        rowCallback,
						ColumnSelector:  selector,
						Checksum:        row.Checksum,
					},
				}
				s.rowChan.Push(mqEvent)
			}
		}
	}
}

func (s *sink) nonBatchEncodeRun(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		default:
			event, ok := s.rowChan.Get()
			if !ok {
				log.Info("kafka sink event channel closed",
					zap.String("namespace", s.changefeedID.Namespace()),
					zap.String("changefeed", s.changefeedID.Name()))
				return nil
			}
			if err := s.comp.encoderGroup.AddEvents(ctx, event.Key, &event.RowEvent); err != nil {
				return err
			}
		}
	}
}

func (s *sink) batchEncodeRun(ctx context.Context) error {
	metricBatchDuration := metrics.WorkerBatchDuration.WithLabelValues(s.changefeedID.Namespace(), s.changefeedID.Name())
	metricBatchSize := metrics.WorkerBatchSize.WithLabelValues(s.changefeedID.Namespace(), s.changefeedID.Name())
	defer func() {
		metrics.WorkerBatchDuration.DeleteLabelValues(s.changefeedID.Namespace(), s.changefeedID.Name())
		metrics.WorkerBatchSize.DeleteLabelValues(s.changefeedID.Namespace(), s.changefeedID.Name())
	}()

	msgsBuf := make([]*commonEvent.MQRowEvent, 0, batchSize)
	for {
		start := time.Now()
		msgs, err := s.batch(ctx, msgsBuf)
		if err != nil {
			log.Error("kafka sink batch dml events failed",
				zap.String("namespace", s.changefeedID.Namespace()),
				zap.String("changefeed", s.changefeedID.Name()),
				zap.Error(err))
			return err
		}
		if len(msgs) == 0 {
			continue
		}

		metricBatchSize.Observe(float64(len(msgs)))
		metricBatchDuration.Observe(time.Since(start).Seconds())

		// Group messages by its TopicPartitionKey before adding them to the encoder group.
		groupedMsgs := s.group(msgs)
		for key, msg := range groupedMsgs {
			if err = s.comp.encoderGroup.AddEvents(ctx, key, msg...); err != nil {
				return err
			}
		}
	}
}

// batch collects a batch of messages from w.msgChan into buffer.
// It returns the number of messages collected.
// Note: It will block until at least one message is received.
func (s *sink) batch(ctx context.Context, buffer []*commonEvent.MQRowEvent) ([]*commonEvent.MQRowEvent, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		msgs, ok := s.rowChan.GetMultipleNoGroup(buffer)
		if !ok {
			log.Info("kafka sink event channel closed",
				zap.String("namespace", s.changefeedID.Namespace()),
				zap.String("changefeed", s.changefeedID.Name()))
			return nil, nil
		}
		return msgs, nil
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

func (s *sink) sendMessages(ctx context.Context) error {
	metricSendMessageDuration := metrics.WorkerSendMessageDuration.WithLabelValues(s.changefeedID.Namespace(), s.changefeedID.Name())
	defer metrics.WorkerSendMessageDuration.DeleteLabelValues(s.changefeedID.Namespace(), s.changefeedID.Name())

	var err error
	outCh := s.comp.encoderGroup.Output()
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case future, ok := <-outCh:
			if !ok {
				log.Info("kafka sink encoder's output channel closed",
					zap.String("namespace", s.changefeedID.Namespace()),
					zap.String("changefeed", s.changefeedID.Name()))
				return nil
			}
			if err = future.Ready(ctx); err != nil {
				return err
			}
			for _, message := range future.Messages {
				start := time.Now()
				if err = s.statistics.RecordBatchExecution(func() (int, int64, error) {
					message.SetPartitionKey(future.Key.PartitionKey)
					log.Debug("send message to kafka", zap.String("messageKey", string(message.Key)), zap.String("messageValue", string(message.Value)))
					if err = s.dmlProducer.AsyncSend(
						ctx,
						future.Key.Topic,
						future.Key.Partition,
						message); err != nil {
						return 0, 0, err
					}
					return message.GetRowsCount(), int64(message.Length()), nil
				}); err != nil {
					return err
				}
				metricSendMessageDuration.Observe(time.Since(start).Seconds())
			}
		}
	}
}

func (s *sink) sendDDLEvent(event *commonEvent.DDLEvent) error {
	for _, e := range event.GetEvents() {
		message, err := s.comp.encoder.EncodeDDLEvent(e)
		if err != nil {
			return err
		}
		if message == nil {
			log.Info("Skip ddl event", zap.Uint64("commitTs", e.GetCommitTs()),
				zap.String("query", e.Query),
				zap.Stringer("changefeed", s.changefeedID))
			continue
		}
		topic := s.comp.eventRouter.GetTopicForDDL(e)
		// Notice: We must call GetPartitionNum here,
		// which will be responsible for automatically creating topics when they don't exist.
		// If it is not called here and kafka has `auto.create.topics.enable` turned on,
		// then the auto-created topic will not be created as configured by ticdc.
		partitionNum, err := s.comp.topicManager.GetPartitionNum(s.ctx, topic)
		if err != nil {
			return err
		}
		if s.partitionRule == helper.PartitionAll {
			err = s.statistics.RecordDDLExecution(func() error {
				return s.ddlProducer.SendMessages(s.ctx, topic, partitionNum, message)
			})
		} else {
			err = s.statistics.RecordDDLExecution(func() error {
				return s.ddlProducer.SendMessage(s.ctx, topic, 0, message)
			})
		}
		if err != nil {
			return err
		}
	}
	log.Info("kafka sink send DDL event",
		zap.String("namespace", s.changefeedID.Namespace()), zap.String("changefeed", s.changefeedID.Name()),
		zap.Any("commitTs", event.GetCommitTs()), zap.Any("event", event.GetDDLQuery()),
		zap.String("schema", event.GetSchemaName()), zap.String("table", event.GetTableName()))
	return nil
}

func (s *sink) AddCheckpointTs(ts uint64) {
	s.checkpointChan <- ts
}

func (s *sink) sendCheckpoint(ctx context.Context) error {
	checkpointTsMessageDuration := metrics.CheckpointTsMessageDuration.WithLabelValues(s.changefeedID.Namespace(), s.changefeedID.Name())
	checkpointTsMessageCount := metrics.CheckpointTsMessageCount.WithLabelValues(s.changefeedID.Namespace(), s.changefeedID.Name())
	defer func() {
		metrics.CheckpointTsMessageDuration.DeleteLabelValues(s.changefeedID.Namespace(), s.changefeedID.Name())
		metrics.CheckpointTsMessageCount.DeleteLabelValues(s.changefeedID.Namespace(), s.changefeedID.Name())
	}()

	var (
		msg          *common.Message
		partitionNum int32
		err          error
	)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case ts, ok := <-s.checkpointChan:
			if !ok {
				log.Warn("kafka sink checkpoint channel closed",
					zap.String("namespace", s.changefeedID.Namespace()),
					zap.String("changefeed", s.changefeedID.Name()))
				return nil
			}

			start := time.Now()
			msg, err = s.comp.encoder.EncodeCheckpointEvent(ts)
			if err != nil {
				return err
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
				partitionNum, err = s.comp.topicManager.GetPartitionNum(ctx, topic)
				if err != nil {
					return err
				}
				err = s.ddlProducer.SendMessages(ctx, topic, partitionNum, msg)
				if err != nil {
					return err
				}
			} else {
				topics := s.comp.eventRouter.GetActiveTopics(tableNames)
				for _, topic := range topics {
					partitionNum, err = s.comp.topicManager.GetPartitionNum(ctx, topic)
					if err != nil {
						return err
					}
					err = s.ddlProducer.SendMessages(ctx, topic, partitionNum, msg)
					if err != nil {
						return err
					}
				}
			}
			checkpointTsMessageCount.Inc()
			checkpointTsMessageDuration.Observe(time.Since(start).Seconds())
		}
	}
}

func (s *sink) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
	s.tableSchemaStore = tableSchemaStore
}

func (s *sink) getAllTableNames(ts uint64) []*commonEvent.SchemaTableName {
	if s.tableSchemaStore == nil {
		log.Warn("kafka sink table schema store is not set",
			zap.String("namespace", s.changefeedID.Namespace()),
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Uint64("ts", ts))
		return nil
	}
	return s.tableSchemaStore.GetAllTableNames(ts)
}

func (s *sink) Close(_ bool) {
	s.ddlProducer.Close()
	s.dmlProducer.Close()
	s.comp.close()
	s.statistics.Close()
}
