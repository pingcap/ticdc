// Copyright 2026 PingCAP, Inc.
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
			if kerr, ok := err.(kafka.Error); ok && kerr.Code() == kafka.ErrTransport {
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
		time.Sleep(time.Second)
	}
	return 0, errors.Errorf("get partition number(%s) timeout", o.topic)
}

type consumer struct {
	client *kafka.Consumer
	engine *replayEngine
}

func newConsumer(ctx context.Context, o *option) (*consumer, error) {
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
	if level, err := zapcore.ParseLevel(logLevel); err == nil && level == zapcore.DebugLevel {
		if err = configMap.SetKey("debug", "all"); err != nil {
			log.Error("set kafka debug log failed", zap.Error(err))
		}
	}

	client, err := kafka.NewConsumer(configMap)
	if err != nil {
		return nil, errors.Trace(err)
	}
	topics := strings.Split(o.topic, ",")
	if err = client.SubscribeTopics(topics, nil); err != nil {
		client.Close()
		return nil, errors.Trace(err)
	}

	engine, err := newReplayEngine(ctx, o)
	if err != nil {
		client.Close()
		return nil, errors.Trace(err)
	}
	return &consumer{
		client: client,
		engine: engine,
	}, nil
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
			log.Info("consumer exits: context cancelled")
			return errors.Trace(ctx.Err())
		default:
		}

		msg, err := c.client.ReadMessage(100 * time.Millisecond)
		if err != nil {
			if kerr, ok := err.(kafka.Error); ok && kerr.Code() == kafka.ErrTimedOut {
				continue
			}
			log.Warn("read kafka message failed, retry later", zap.Error(err))
			continue
		}

		committable, err := c.engine.HandleMessage(ctx, msg)
		if err != nil {
			return errors.Trace(err)
		}
		if len(committable) == 0 {
			continue
		}
		committed, err := c.client.CommitOffsets(committable)
		if err != nil {
			log.Warn("commit kafka offsets failed; messages may be replayed",
				zap.Any("offsets", committable), zap.Error(err))
			continue
		}
		log.Debug("commit kafka offsets success", zap.Any("offsets", committed))
	}
}

func (c *consumer) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return c.engine.Run(ctx)
	})
	g.Go(func() error {
		return c.readMessage(ctx)
	})
	return g.Wait()
}
