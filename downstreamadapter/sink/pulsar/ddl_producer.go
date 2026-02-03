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

	pulsarClient "github.com/apache/pulsar-client-go/pulsar"
	lru "github.com/hashicorp/golang-lru"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/pulsar"
	"go.uber.org/zap"
)

// ddlProducer is the interface for the pulsar DDL message producer.
type ddlProducer interface {
	// syncBroadcastMessage broadcasts a message synchronously.
	syncBroadcastMessage(
		ctx context.Context, topic string, message *common.Message, messageType common.MessageType,
	) error
	// syncSendMessage sends a message for a partition synchronously.
	syncSendMessage(
		ctx context.Context, topic string, message *common.Message, messageType common.MessageType,
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
		pulsarProducer, ok := value.(pulsarClient.Producer)
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
func (p *ddlProducers) syncBroadcastMessage(ctx context.Context, topic string, message *common.Message, messageType common.MessageType,
) error {
	// call SyncSendMessage
	// pulsar consumer all partitions
	return p.syncSendMessage(ctx, topic, message, messageType)
}

// SyncSendMessage sends a message
// partitionNum is not used, pulsar consume all partitions
func (p *ddlProducers) syncSendMessage(ctx context.Context, topic string, message *common.Message, messageType common.MessageType) error {
	pulsar.IncPublishedDDLCount(topic, p.changefeedID.String(), messageType)
	producer, err := p.getProducerByTopic(topic)
	if err != nil {
		log.Error("ddl SyncSendMessage GetProducerByTopic fail", zap.Error(err))
		return err
	}

	data := &pulsarClient.ProducerMessage{
		Payload: message.Value,
		Key:     message.GetPartitionKey(),
	}
	mID, err := producer.Send(ctx, data)
	if err != nil {
		log.Error("ddl producer send fail", zap.Error(err))
		pulsar.IncPublishedDDLFail(topic, p.changefeedID.String(), messageType)
		return err
	}

	log.Debug("ddlProducers SyncSendMessage success",
		zap.Any("mID", mID), zap.String("topic", topic))

	pulsar.IncPublishedDDLSuccess(topic, p.changefeedID.String(), messageType)
	return nil
}

func (p *ddlProducers) getProducer(topic string) (pulsarClient.Producer, bool) {
	target, ok := p.producers.Get(topic)
	if ok {
		producer, ok := target.(pulsarClient.Producer)
		if ok {
			return producer, true
		}
	}
	return nil, false
}

// getProducerByTopic get producer by topicName
func (p *ddlProducers) getProducerByTopic(topicName string) (producer pulsarClient.Producer, err error) {
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
