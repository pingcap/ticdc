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

package main

import (
	"context"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type consumer struct {
	client *kafka.Consumer
	writer *writer
}

func newConsumer(ctx context.Context, o *option) *consumer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":        strings.Join(o.address, ","),
		"group.id":                 o.groupID,
		"auto.offset.reset":        "earliest",
		"enable.auto.offset.store": false,
		"enable.auto.commit":       false,
	}
	if len(o.ca) != 0 {
		_ = configMap.SetKey("security.protocol", "SSL")
		_ = configMap.SetKey("ssl.ca.location", o.ca)
		_ = configMap.SetKey("ssl.key.location", o.key)
		_ = configMap.SetKey("ssl.certificate.location", o.cert)
	}
	client, err := kafka.NewConsumer(configMap)
	if err != nil {
		log.Panic("create kafka consumer failed", zap.Error(err))
	}

	topics := strings.Split(o.topic, ",")
	err = client.SubscribeTopics(topics, nil)
	if err != nil {
		log.Panic("subscribe topics failed", zap.Strings("topics", topics), zap.Error(err))
	}
	return &consumer{
		writer: newWriter(ctx, o),
		client: client,
	}
}

func (c *consumer) readMessage(ctx context.Context) error {
	defer func() {
		if err := c.client.Close(); err != nil {
			log.Warn("close kafka consumer failed", zap.Error(err))
		}
	}()
	for {
		select {
		case <-ctx.Done():
			log.Info("consumer exist: context cancelled")
			return errors.Trace(ctx.Err())
		default:
		}
		msg, err := c.client.ReadMessage(-1)
		if err != nil {
			log.Error("read message failed, just continue to retry", zap.Error(err))
			continue
		}
		needCommit := c.writer.WriteMessage(ctx, msg)
		if !needCommit {
			continue
		}
		topicPartition, err := c.client.CommitMessage(msg)
		if err != nil {
			log.Error("commit message failed, just continue",
				zap.String("topic", *msg.TopicPartition.Topic), zap.Int32("partition", msg.TopicPartition.Partition),
				zap.Any("offset", msg.TopicPartition.Offset), zap.Error(err))
			continue
		}
		log.Debug("commit message success",
			zap.String("topic", topicPartition[0].String()), zap.Int32("partition", topicPartition[0].Partition),
			zap.Any("offset", topicPartition[0].Offset))
	}
}

func (c *consumer) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return c.writer.run(ctx)
	})
	g.Go(func() error {
		return c.readMessage(ctx)
	})
	return g.Wait()
}
