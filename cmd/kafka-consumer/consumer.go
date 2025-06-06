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

package main

import (
	"context"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
)

func getPartitionNum(o *option) (int32, error) {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(o.address, ","),
	}
	if len(o.ca) != 0 {
		_ = configMap.SetKey("security.protocol", "SSL")
		_ = configMap.SetKey("ssl.ca.location", o.ca)
		_ = configMap.SetKey("ssl.key.location", o.key)
		_ = configMap.SetKey("ssl.certificate.location", o.cert)
	}
	admin, err := kafka.NewAdminClient(configMap)
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer admin.Close()

	timeout := 3000
	for i := 0; i <= 30; i++ {
		resp, err := admin.GetMetadata(&o.topic, false, timeout)
		if err != nil {
			if err.(kafka.Error).Code() == kafka.ErrTransport {
				log.Info("retry get partition number", zap.Int("retryTime", i), zap.Int("timeout", timeout))
				timeout += 100
				continue
			}
			return 0, errors.Trace(err)
		}
		if topicDetail, ok := resp.Topics[o.topic]; ok {
			numPartitions := int32(len(topicDetail.Partitions))
			log.Info("get partition number of topic",
				zap.String("topic", o.topic),
				zap.Int32("partitionNum", numPartitions))
			return numPartitions, nil
		}
		log.Info("retry get partition number", zap.String("topic", o.topic))
		time.Sleep(1 * time.Second)
	}
	return 0, errors.Errorf("get partition number(%s) timeout", o.topic)
}

type consumer struct {
	client *kafka.Consumer
	writer *writer
}

// newConsumer will create a consumer client.
func newConsumer(ctx context.Context, o *option) *consumer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(o.address, ","),
		"group.id":          o.groupID,
		// Start reading from the first message of each assigned
		// partition if there are no previously committed offsets
		// for this group.
		"auto.offset.reset": "earliest",
		// Whether we store offsets automatically.
		"enable.auto.offset.store": false,
		"enable.auto.commit":       false,
	}
	if len(o.ca) != 0 {
		_ = configMap.SetKey("security.protocol", "SSL")
		_ = configMap.SetKey("ssl.ca.location", o.ca)
		_ = configMap.SetKey("ssl.key.location", o.key)
		_ = configMap.SetKey("ssl.certificate.location", o.cert)
	}
	if level, err := zapcore.ParseLevel(logLevel); err == nil && level == zapcore.DebugLevel {
		if err = configMap.SetKey("debug", "all"); err != nil {
			log.Error("set kafka debug log failed", zap.Error(err))
		}
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

// Run the consumer, read data and write to the downstream target.
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
