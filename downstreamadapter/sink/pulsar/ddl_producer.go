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

	"github.com/apache/pulsar-client-go/pulsar"
	lru "github.com/hashicorp/golang-lru"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"go.uber.org/zap"
)

// ddlProducer is the interface for the pulsar DDL message producer.
type ddlProducer interface {
	// syncBroadcastMessage broadcasts a message synchronously.
	syncBroadcastMessage(
		ctx context.Context, topic string, message *common.Message,
	) error
	// syncSendMessage sends a message for a partition synchronously.
	syncSendMessage(
		ctx context.Context, topic string, message *common.Message,
	) error
	// close closes the producer.
	close()
}

// ddlProducers is a producer for pulsar
type ddlProducers struct {
	changefeedID     commonType.ChangeFeedID
	defaultTopicName string
	// support multiple topics
	producers      *lru.Cache
	producersMutex sync.RWMutex
	comp           component
}

// newDDLProducers creates a pulsar producer
func newDDLProducers(
	changefeedID commonType.ChangeFeedID,
	comp component,
	sinkConfig *config.SinkConfig,
) (*ddlProducers, error) {
	topicName, err := helper.GetTopic(comp.config.SinkURI)
	if err != nil {
		return nil, err
	}

	defaultProducer, err := newProducer(comp.config, comp.client, topicName)
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
	}, nil
}

// SyncBroadcastMessage pulsar consume all partitions
// totalPartitionsNum is not used
func (p *ddlProducers) syncBroadcastMessage(ctx context.Context, topic string, message *common.Message,
) error {
	// call SyncSendMessage
	// pulsar consumer all partitions
	return p.syncSendMessage(ctx, topic, message)
}

// SyncSendMessage sends a message
// partitionNum is not used, pulsar consume all partitions
func (p *ddlProducers) syncSendMessage(ctx context.Context, topic string, message *common.Message) error {
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
		producer, err = newProducer(p.comp.config, p.comp.client, topicName)
		if err != nil {
			return nil, err
		}
		p.producers.Add(topicName, producer)
	}

	return producer, nil
}

// Close all producers
func (p *ddlProducers) close() {
	keys := p.producers.Keys()

	p.producersMutex.Lock()
	defer p.producersMutex.Unlock()
	for _, topic := range keys {
		p.producers.Remove(topic) // callback func will be called
	}
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
