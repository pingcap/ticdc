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
	"github.com/hashicorp/golang-lru"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"go.uber.org/zap"
)

// ddlProducers is a producer for pulsar
type ddlProducers struct {
	changefeedID     commonType.ChangeFeedID
	defaultTopicName string
	// support multiple topics
	producers      *lru.Cache
	producersMutex sync.RWMutex

	checkpointTsChan chan uint64

	partitionRule    helper.DDLDispatchRule
	statistics       *metrics.Statistics
	tableSchemaStore *util.TableSchemaStore
	comp             component
}

// newDDLProducers creates a pulsar producer
func newDDLProducers(
	changefeedID commonType.ChangeFeedID,
	comp component,
	statistics *metrics.Statistics,
	protocol config.Protocol,
	sinkConfig *config.SinkConfig,
) (*ddlProducers, error) {
	topicName, err := helper.GetTopic(comp.Config.SinkURI)
	if err != nil {
		return nil, err
	}

	defaultProducer, err := newProducer(comp.Config, comp.client, topicName)
	if err != nil {
		return nil, err
	}

	producerCacheSize := config.DefaultPulsarProducerCacheSize
	if sinkConfig.PulsarConfig != nil && sinkConfig.PulsarConfig.PulsarProducerCacheSize != nil {
		producerCacheSize = int(*sinkConfig.PulsarConfig.PulsarProducerCacheSize)
	}

	producers, err := lru.NewWithEvict(producerCacheSize, func(key interface{}, value interface{}) {
		// remove producer
		pulsarProducer, ok := value.(pulsar.Producer)
		if ok && pulsarProducer != nil {
			pulsarProducer.Close()
		}
	})
	if err != nil {
		return nil, err
	}

	producers.Add(topicName, defaultProducer)
	return &ddlProducers{
		producers:        producers,
		defaultTopicName: topicName,
		changefeedID:     changefeedID,
		comp:             comp,
		statistics:       statistics,
		partitionRule:    helper.GetDDLDispatchRule(protocol),
		checkpointTsChan: make(chan uint64, 16),
	}, nil
}

// SyncBroadcastMessage pulsar consume all partitions
// totalPartitionsNum is not used
func (p *ddlProducers) SyncBroadcastMessage(ctx context.Context, topic string,
	totalPartitionsNum int32, message *common.Message,
) error {
	// call SyncSendMessage
	// pulsar consumer all partitions
	return p.SyncSendMessage(ctx, topic, totalPartitionsNum, message)
}

// SyncSendMessage sends a message
// partitionNum is not used, pulsar consume all partitions
func (p *ddlProducers) SyncSendMessage(ctx context.Context, topic string,
	partitionNum int32, message *common.Message,
) error {
	// TODO
	// wrapperSchemaAndTopic(message)
	// mq.IncPublishedDDLCount(topic, p.id.ID().String(), message)

	producer, err := p.getProducerByTopic(topic)
	if err != nil {
		log.Error("ddl SyncSendMessage GetProducerByTopic fail", zap.Error(err))
		return err
	}

	data := &pulsar.ProducerMessage{
		Payload: message.Value,
		Key:     message.GetPartitionKey(),
	}
	mID, err := producer.Send(ctx, data)
	if err != nil {
		log.Error("ddl producer send fail", zap.Error(err))
		// mq.IncPublishedDDLFail(topic, p.id.ID().String(), message)
		return err
	}

	log.Debug("ddlProducers SyncSendMessage success",
		zap.Any("mID", mID), zap.String("topic", topic))

	// mq.IncPublishedDDLSuccess(topic, p.id.ID().String(), message)
	return nil
}

func (p *ddlProducers) getProducer(topic string) (pulsar.Producer, bool) {
	target, ok := p.producers.Get(topic)
	if ok {
		producer, ok := target.(pulsar.Producer)
		if ok {
			return producer, true
		}
	}
	return nil, false
}

// getProducerByTopic get producer by topicName
func (p *ddlProducers) getProducerByTopic(topicName string) (producer pulsar.Producer, err error) {
	getProducer, ok := p.getProducer(topicName)
	if ok && getProducer != nil {
		return getProducer, nil
	}

	if !ok { // create a new producer for the topicName
		producer, err = newProducer(p.comp.Config, p.comp.client, topicName)
		if err != nil {
			return nil, err
		}
		p.producers.Add(topicName, producer)
	}

	return producer, nil
}

// Close close all producers
func (p *ddlProducers) Close() {
	keys := p.producers.Keys()

	p.producersMutex.Lock()
	defer p.producersMutex.Unlock()
	for _, topic := range keys {
		p.producers.Remove(topic) // callback func will be called
	}
}

func (p *ddlProducers) Run(ctx context.Context) error {
	checkpointTsMessageDuration := metrics.CheckpointTsMessageDuration.WithLabelValues(p.changefeedID.Namespace(), p.changefeedID.Name())
	checkpointTsMessageCount := metrics.CheckpointTsMessageCount.WithLabelValues(p.changefeedID.Namespace(), p.changefeedID.Name())

	defer func() {
		metrics.CheckpointTsMessageDuration.DeleteLabelValues(p.changefeedID.Namespace(), p.changefeedID.Name())
		metrics.CheckpointTsMessageCount.DeleteLabelValues(p.changefeedID.Namespace(), p.changefeedID.Name())
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
		case ts, ok := <-p.checkpointTsChan:
			if !ok {
				log.Warn("MQ sink flush worker channel closed",
					zap.String("namespace", p.changefeedID.Namespace()),
					zap.String("changefeed", p.changefeedID.Name()))
				return nil
			}

			start := time.Now()
			msg, err = p.comp.Encoder.EncodeCheckpointEvent(ts)
			if err != nil {
				return errors.Trace(err)
			}

			if msg == nil {
				continue
			}
			tableNames := p.tableSchemaStore.GetAllTableNames(ts)
			// NOTICE: When there are no tables to replicate,
			// we need to send checkpoint ts to the default topic.
			// This will be compatible with the old behavior.
			if len(tableNames) == 0 {
				topic := p.comp.EventRouter.GetDefaultTopic()
				partitionNum, err = p.comp.TopicManager.GetPartitionNum(ctx, topic)
				if err != nil {
					return errors.Trace(err)
				}
				log.Debug("Emit checkpointTs to default topic",
					zap.String("topic", topic), zap.Uint64("checkpointTs", ts), zap.Any("partitionNum", partitionNum))
				err = p.SyncBroadcastMessage(ctx, topic, partitionNum, msg)
				if err != nil {
					return errors.Trace(err)
				}
			} else {
				topics := p.comp.EventRouter.GetActiveTopics(tableNames)
				for _, topic := range topics {
					partitionNum, err = p.comp.TopicManager.GetPartitionNum(ctx, topic)
					if err != nil {
						return errors.Trace(err)
					}
					err = p.SyncBroadcastMessage(ctx, topic, partitionNum, msg)
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

func (p *ddlProducers) SetTableSchemaStore(store *util.TableSchemaStore) {
	p.tableSchemaStore = store
}

func (p *ddlProducers) AddCheckpoint(ts uint64) {
	p.checkpointTsChan <- ts
}

func (p *ddlProducers) WriteBlockEvent(ctx context.Context, event *commonEvent.DDLEvent) error {
	for _, e := range event.GetEvents() {
		message, err := p.comp.Encoder.EncodeDDLEvent(e)
		if err != nil {
			return errors.Trace(err)
		}
		topic := p.comp.EventRouter.GetTopicForDDL(e)
		// Notice: We must call GetPartitionNum here,
		// which will be responsible for automatically creating topics when they don't exist.
		// If it is not called here and kafka has `auto.create.topics.enable` turned on,
		// then the auto-created topic will not be created as configured by ticdc.
		partitionNum, err := p.comp.TopicManager.GetPartitionNum(ctx, topic)
		if err != nil {
			return errors.Trace(err)
		}
		if p.partitionRule == helper.PartitionAll {
			err = p.statistics.RecordDDLExecution(func() error {
				return p.SyncBroadcastMessage(ctx, topic, partitionNum, message)
			})
		} else {
			err = p.statistics.RecordDDLExecution(func() error {
				return p.SyncSendMessage(ctx, topic, 0, message)
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

// maxwellMessage is the message format of maxwell
type maxwellMessage struct {
	Database string `json:"database"`
	Table    string `json:"table"`
}

// str2Pointer returns the pointer of the string.
func str2Pointer(str string) *string {
	return &str
}

// newProducer creates a pulsar producer
// One topic is used by one producer
func newProducer(
	pConfig *config.PulsarConfig,
	client pulsar.Client,
	topicName string,
) (pulsar.Producer, error) {
	maxReconnectToBroker := uint(config.DefaultMaxReconnectToPulsarBroker)
	option := pulsar.ProducerOptions{
		Topic:                topicName,
		MaxReconnectToBroker: &maxReconnectToBroker,
	}
	if pConfig.BatchingMaxMessages != nil {
		option.BatchingMaxMessages = *pConfig.BatchingMaxMessages
	}
	if pConfig.BatchingMaxPublishDelay != nil {
		option.BatchingMaxPublishDelay = pConfig.BatchingMaxPublishDelay.Duration()
	}
	if pConfig.CompressionType != nil {
		option.CompressionType = pConfig.CompressionType.Value()
		option.CompressionLevel = pulsar.Default
	}
	if pConfig.SendTimeout != nil {
		option.SendTimeout = pConfig.SendTimeout.Duration()
	}

	producer, err := client.CreateProducer(option)
	if err != nil {
		return nil, err
	}

	log.Info("create pulsar producer success", zap.String("topic", topicName))

	return producer, nil
}
