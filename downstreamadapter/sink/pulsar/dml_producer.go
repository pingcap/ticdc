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

	pulsarClient "github.com/apache/pulsar-client-go/pulsar"
	lru "github.com/hashicorp/golang-lru"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/pulsar"
	"go.uber.org/zap"
)

// dmlProducer is the interface for the pulsar DML message producer.
type dmlProducer interface {
	// AsyncSendMessage sends a message asynchronously.
	asyncSendMessage(
		ctx context.Context, topic string, message *common.Message,
	) error

	run(ctx context.Context) error

	close()
}

// dmlProducers is used to send messages to pulsar.
type dmlProducers struct {
	changefeedID commonType.ChangeFeedID
	// producers is used to send messages to pulsar.
	// One topic only use one producer , so we want to have many topics but use less memory,
	// lru is a good idea to solve this question.
	// support multiple topics
	producers *lru.Cache

	comp component

	// closedMu is used to protect `closed`.
	// We need to ensure that closed producers are never written to.
	closedMu sync.RWMutex
	// closed is used to indicate whether the producer is closed.
	// We also use it to guard against double closes.
	closed bool

	// failpointCh is used to inject failpoints to the run loop.
	// Only used in test.
	failpointCh chan error
	// closeCh is send error
	errChan chan error
}

// newDMLProducers creates a new pulsar producer.
func newDMLProducers(
	changefeedID commonType.ChangeFeedID,
	comp component,
	failpointCh chan error,
) (*dmlProducers, error) {
	log.Info("Creating pulsar DML producer ...",
		zap.String("keyspace", changefeedID.Keyspace()),
		zap.String("changefeed", changefeedID.ID().String()))
	start := time.Now()

	defaultTopicName := comp.config.GetDefaultTopicName()
	defaultProducer, err := newProducer(comp.config, comp.client, defaultTopicName)
	if err != nil {
		return nil, errors.WrapError(errors.ErrPulsarNewProducer, err)
	}
	producerCacheSize := config.DefaultPulsarProducerCacheSize
	if comp.config != nil && comp.config.PulsarProducerCacheSize != nil {
		producerCacheSize = int(*comp.config.PulsarProducerCacheSize)
	}

	producers, err := lru.NewWithEvict(producerCacheSize, func(key interface{}, value interface{}) {
		// this is call when lru Remove producer or auto remove producer
		pulsarProducer, ok := value.(pulsarClient.Producer)
		if ok && pulsarProducer != nil {
			pulsarProducer.Close()
		}
	})
	if err != nil {
		return nil, errors.WrapError(errors.ErrPulsarNewProducer, err)
	}

	producers.Add(defaultTopicName, defaultProducer)

	p := &dmlProducers{
		changefeedID: changefeedID,
		comp:         comp,
		producers:    producers,
		closed:       false,
		failpointCh:  failpointCh,
		errChan:      make(chan error, 1),
	}
	log.Info("Pulsar DML producer created", zap.Stringer("changefeed", p.changefeedID),
		zap.Duration("duration", time.Since(start)))
	return p, nil
}

func (p *dmlProducers) run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-p.failpointCh:
			return errors.Trace(err)
		case err := <-p.errChan:
			return errors.Trace(err)
		}
	}
}

// asyncSendMessage  Async send one message
func (p *dmlProducers) asyncSendMessage(
	ctx context.Context, topic string, message *common.Message,
) error {
	// We have to hold the lock to avoid writing to a closed producer.
	// Close may be blocked for a long time.
	p.closedMu.RLock()
	defer p.closedMu.RUnlock()

	// If producers are closed, we should skip the message and return an error.
	if p.closed {
		return errors.ErrPulsarProducerClosed.GenWithStackByArgs()
	}
	failpoint.Inject("PulsarSinkAsyncSendError", func() {
		// simulate sending message to input channel successfully but flushing
		// message to Pulsar meets error
		log.Info("PulsarSinkAsyncSendError error injected", zap.String("keyspace", p.changefeedID.Keyspace()),
			zap.String("changefeed", p.changefeedID.ID().String()))
		p.failpointCh <- errors.New("pulsar sink injected error")
		failpoint.Return(nil)
	})
	data := &pulsarClient.ProducerMessage{
		Payload: message.Value,
		Key:     message.GetPartitionKey(),
	}

	producer, err := p.getProducerByTopic(topic)
	if err != nil {
		return err
	}

	// if for stress test record , add count to message callback function

	producer.SendAsync(ctx, data,
		func(id pulsarClient.MessageID, m *pulsarClient.ProducerMessage, err error) {
			// fail
			if err != nil {
				e := errors.WrapError(errors.ErrPulsarAsyncSendMessage, err)
				log.Error("Pulsar DML producer async send error",
					zap.String("keyspace", p.changefeedID.Keyspace()),
					zap.String("changefeed", p.changefeedID.ID().String()),
					zap.Int("messageSize", len(m.Payload)),
					zap.String("topic", topic),
					zap.Error(err))
				pulsar.IncPublishedDMLFail(topic, p.changefeedID.String())
				// use this select to avoid send error to a closed channel
				// the ctx will always be called before the errChan is closed
				select {
				case <-ctx.Done():
					return
				case p.errChan <- e:
				default:
					log.Warn("Error channel is full in pulsar DML producer",
						zap.Stringer("changefeed", p.changefeedID), zap.Error(e))
				}
			} else if message.Callback != nil {
				// success
				message.Callback()
				pulsar.IncPublishedDMLSuccess(topic, p.changefeedID.String())
			}
		})

	pulsar.IncPublishedDMLCount(topic, p.changefeedID.String())

	return nil
}

func (p *dmlProducers) close() { // We have to hold the lock to synchronize closing with writing.
	p.closedMu.Lock()
	defer p.closedMu.Unlock()
	// If the producer has already been closed, we should skip this close operation.
	if p.closed {
		// We need to guard against double closing the clients,
		// which could lead to panic.
		log.Warn("Pulsar DML producer already closed",
			zap.String("keyspace", p.changefeedID.Keyspace()),
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
			zap.String("keyspace", p.changefeedID.Keyspace()),
			zap.String("changefeed", p.changefeedID.ID().String()), zap.String("topic", topicName))
	}
}

func (p *dmlProducers) getProducer(topic string) (pulsarClient.Producer, bool) {
	target, ok := p.producers.Get(topic)
	if ok {
		producer, ok := target.(pulsarClient.Producer)
		if ok {
			return producer, true
		}
	}
	return nil, false
}

// getProducerByTopic get producer by topicName,
// if not exist, it will create a producer with topicName, and set in LRU cache
// more meta info at dmlProducers's producers
func (p *dmlProducers) getProducerByTopic(topicName string) (producer pulsarClient.Producer, err error) {
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
