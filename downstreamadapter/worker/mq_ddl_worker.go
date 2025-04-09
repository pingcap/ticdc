// Copyright 2025 PingCAP, Inc.
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
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"go.uber.org/zap"
)

// MQDDLWorker handle DDL and checkpoint event
type MQDDLWorker struct {
	// changeFeedID indicates this sink belongs to which processor(changefeed).
	changeFeedID commonType.ChangeFeedID

	checkpointTsChan chan uint64
	encoder          common.EventEncoder
	// eventRouter used to route events to the right topic and partition.
	eventRouter *eventrouter.EventRouter
	// topicManager used to manage topics.
	// It is also responsible for creating topics.
	topicManager topicmanager.TopicManager

	// producer is used to send the messages to the MQ broker.
	producer producer.DDLProducer

	tableSchemaStore *util.TableSchemaStore

	statistics    *metrics.Statistics
	partitionRule DDLDispatchRule
}

// DDLDispatchRule is the dispatch rule for DDL event.
type DDLDispatchRule int

const (
	// PartitionZero means the DDL event will be dispatched to partition 0.
	// NOTICE: Only for canal and canal-json protocol.
	PartitionZero DDLDispatchRule = iota
	// PartitionAll means the DDL event will be broadcast to all the partitions.
	PartitionAll
)

func getDDLDispatchRule(protocol config.Protocol) DDLDispatchRule {
	switch protocol {
	case config.ProtocolCanal, config.ProtocolCanalJSON:
		return PartitionZero
	default:
	}
	return PartitionAll
}

// NewMQDDLWorker return a ddl worker instance.
func NewMQDDLWorker(
	id commonType.ChangeFeedID,
	protocol config.Protocol,
	producer producer.DDLProducer,
	encoder common.EventEncoder,
	eventRouter *eventrouter.EventRouter,
	topicManager topicmanager.TopicManager,
	statistics *metrics.Statistics,
) *MQDDLWorker {
	return &MQDDLWorker{
		changeFeedID:     id,
		encoder:          encoder,
		producer:         producer,
		eventRouter:      eventRouter,
		topicManager:     topicManager,
		statistics:       statistics,
		partitionRule:    getDDLDispatchRule(protocol),
		checkpointTsChan: make(chan uint64, 16),
	}
}

func (w *MQDDLWorker) Run(ctx context.Context) error {
	return w.encodeAndSendCheckpointEvents(ctx)
}

func (w *MQDDLWorker) AddCheckpoint(ts uint64) {
	w.checkpointTsChan <- ts
}

func (w *MQDDLWorker) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
	w.tableSchemaStore = tableSchemaStore
}

func (w *MQDDLWorker) WriteBlockEvent(ctx context.Context, event *event.DDLEvent) error {
	for _, e := range event.GetEvents() {
		message, err := w.encoder.EncodeDDLEvent(e)
		if err != nil {
			return errors.Trace(err)
		}
		topic := w.eventRouter.GetTopicForDDL(e)
		// Notice: We must call GetPartitionNum here,
		// which will be responsible for automatically creating topics when they don't exist.
		// If it is not called here and kafka has `auto.create.topics.enable` turned on,
		// then the auto-created topic will not be created as configured by ticdc.
		partitionNum, err := w.topicManager.GetPartitionNum(ctx, topic)
		if err != nil {
			return errors.Trace(err)
		}
		if w.partitionRule == PartitionAll {
			err = w.statistics.RecordDDLExecution(func() error {
				return w.producer.SyncBroadcastMessage(ctx, topic, partitionNum, message)
			})
		} else {
			err = w.statistics.RecordDDLExecution(func() error {
				return w.producer.SyncSendMessage(ctx, topic, 0, message)
			})
		}
		if err != nil {
			return errors.Trace(err)
		}
	}
	log.Info("MQ ddl worker send block event", zap.Any("event", event))
	// after flush all the ddl event, we call the callback function.
	event.PostFlush()
	return nil
}

func (w *MQDDLWorker) encodeAndSendCheckpointEvents(ctx context.Context) error {
	checkpointTsMessageDuration := metrics.CheckpointTsMessageDuration.WithLabelValues(w.changeFeedID.Namespace(), w.changeFeedID.Name())
	checkpointTsMessageCount := metrics.CheckpointTsMessageCount.WithLabelValues(w.changeFeedID.Namespace(), w.changeFeedID.Name())

	defer func() {
		metrics.CheckpointTsMessageDuration.DeleteLabelValues(w.changeFeedID.Namespace(), w.changeFeedID.Name())
		metrics.CheckpointTsMessageCount.DeleteLabelValues(w.changeFeedID.Namespace(), w.changeFeedID.Name())
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
		case ts, ok := <-w.checkpointTsChan:
			if !ok {
				log.Warn("MQ sink flush worker channel closed",
					zap.String("namespace", w.changeFeedID.Namespace()),
					zap.String("changefeed", w.changeFeedID.Name()))
				return nil
			}

			start := time.Now()
			msg, err = w.encoder.EncodeCheckpointEvent(ts)
			if err != nil {
				return errors.Trace(err)
			}

			if msg == nil {
				continue
			}
			tableNames := w.tableSchemaStore.GetAllTableNames(ts)
			// NOTICE: When there are no tables to replicate,
			// we need to send checkpoint ts to the default topic.
			// This will be compatible with the old behavior.
			if len(tableNames) == 0 {
				topic := w.eventRouter.GetDefaultTopic()
				partitionNum, err = w.topicManager.GetPartitionNum(ctx, topic)
				if err != nil {
					return errors.Trace(err)
				}
				log.Debug("Emit checkpointTs to default topic",
					zap.String("topic", topic), zap.Uint64("checkpointTs", ts), zap.Any("partitionNum", partitionNum))
				err = w.producer.SyncBroadcastMessage(ctx, topic, partitionNum, msg)
				if err != nil {
					return errors.Trace(err)
				}
			} else {
				topics := w.eventRouter.GetActiveTopics(tableNames)
				for _, topic := range topics {
					partitionNum, err = w.topicManager.GetPartitionNum(ctx, topic)
					if err != nil {
						return errors.Trace(err)
					}
					err = w.producer.SyncBroadcastMessage(ctx, topic, partitionNum, msg)
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

func (w *MQDDLWorker) Close() {
	w.producer.Close()
}
