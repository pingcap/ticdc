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

package kafka

import (
	"context"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"go.uber.org/zap"
)

type factory struct {
	config       *kafka.ConfigMap
	changefeedID commonType.ChangeFeedID
}

// NewFactory returns a factory implemented based on kafka-go
func NewFactory(
	options *Options,
	changefeedID commonType.ChangeFeedID,
) (Factory, error) {
	config := NewConfig(options)
	return &factory{
		config:       config,
		changefeedID: changefeedID,
	}, nil
}

func (f *factory) AdminClient() (ClusterAdminClient, error) {
	client, err := kafka.NewAdminClient(f.config)
	if err != nil {
		return nil, err
	}
	return newClusterAdminClient(client, f.changefeedID), nil
}

// SyncProducer creates a sync producer to Producer message to kafka
func (f *factory) SyncProducer() (SyncProducer, error) {
	p, err := kafka.NewProducer(f.config)
	if err != nil {
		return nil, err
	}
	return &syncProducer{changefeedID: f.changefeedID, p: p, deliveryChan: make(chan kafka.Event)}, err
}

// AsyncProducer creates an async producer to Producer message to kafka
func (f *factory) AsyncProducer() (AsyncProducer, error) {
	p, err := kafka.NewProducer(f.config)
	if err != nil {
		return nil, err
	}
	return &asyncProducer{changefeedID: f.changefeedID, p: p}, err
}

// MetricsCollector returns the kafka metrics collector
func (f *factory) MetricsCollector() MetricsCollector {
	return NewMetricsCollector(f.changefeedID, f.config)
}

type syncProducer struct {
	changefeedID commonType.ChangeFeedID
	p            *kafka.Producer
	deliveryChan chan kafka.Event
}

func (s *syncProducer) SendMessage(
	ctx context.Context,
	topic string, partition int32,
	message *common.Message,
) error {
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: partition},
		Key:            message.Key,
		Value:          message.Value,
		Opaque:         message.Callback,
	}
	err := s.p.Produce(msg, s.deliveryChan)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case event := <-s.deliveryChan:
		switch e := event.(type) {
		case *kafka.Error:
			return e
		}
	}
	return nil
}

// SendMessages produces a given set of messages, and returns only when all
// messages in the set have either succeeded or failed. Note that messages
// can succeed and fail individually; if some succeed and some fail,
// SendMessages will return an error.
func (s *syncProducer) SendMessages(ctx context.Context, topic string, partitionNum int32, message *common.Message) error {
	var err error
	for i := 0; i < int(partitionNum); i++ {
		e := s.SendMessage(ctx, topic, int32(i), message)
		if e != nil {
			err = e
		}
	}
	return err
}

// Close shuts down the producer; you must call this function before a producer
// object passes out of scope, as it may otherwise leak memory.
// You must call this before calling Close on the underlying client.
func (s *syncProducer) Close() {
	log.Info("kafka sync producer start closing",
		zap.String("namespace", s.changefeedID.Namespace()),
		zap.String("changefeed", s.changefeedID.Name()))
	s.p.Close()
	close(s.deliveryChan)
}

type asyncProducer struct {
	p            *kafka.Producer
	changefeedID commonType.ChangeFeedID
}

// Close shuts down the producer and waits for any buffered messages to be
// flushed. You must call this function before a producer object passes out of
// scope, as it may otherwise leak memory. You must call this before process
// shutting down, or you may lose messages. You must call this before calling
// Close on the underlying client.
func (a *asyncProducer) Close() {
	log.Info("kafka async producer start closing",
		zap.String("namespace", a.changefeedID.Namespace()),
		zap.String("changefeed", a.changefeedID.Name()))
	go func() {
		start := time.Now()
		a.p.Close()
		log.Info("Close kafka async producer success",
			zap.String("namespace", a.changefeedID.Namespace()),
			zap.String("changefeed", a.changefeedID.Name()),
			zap.Duration("duration", time.Since(start)))
	}()
}

// AsyncRunCallback process the messages that has sent to kafka,
// and run tha attached callback. the caller should call this
// method in a background goroutine
func (a *asyncProducer) AsyncRunCallback(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case event := <-a.p.Events():
			switch e := event.(type) {
			case *kafka.Message:
				callback := e.Opaque.(func())
				if callback != nil {
					callback()
				}
			case *kafka.Error:
				return errors.WrapError(errors.ErrKafkaAsyncSendMessage, e)
			}
		}
	}
}

// AsyncSend is the input channel for the user to write messages to that they
// wish to send.
func (a *asyncProducer) AsyncSend(ctx context.Context, topic string, partition int32, message *common.Message) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}
	return a.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: partition},
		Key:            message.Key,
		Value:          message.Value,
		Opaque:         message.Callback,
	}, nil)
}
