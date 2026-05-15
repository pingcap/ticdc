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
	"sort"
	"strings"
	"sync"
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

const (
	kafkaReadTimeout            = 100 * time.Millisecond
	offsetCommitInterval        = time.Second
	offsetCommitUpdateThreshold = 1024
	partitionMessageBufferSize  = 4096
)

type workerResult struct {
	committable []kafka.TopicPartition
	err         error
}

type offsetCommitBuffer struct {
	offsets     map[offsetKey]kafka.TopicPartition
	updates     int
	lastAttempt time.Time
}

func newOffsetCommitBuffer(now time.Time) *offsetCommitBuffer {
	return &offsetCommitBuffer{
		offsets:     make(map[offsetKey]kafka.TopicPartition),
		lastAttempt: now,
	}
}

func (b *offsetCommitBuffer) Add(offsets []kafka.TopicPartition) {
	if len(offsets) == 0 {
		return
	}
	for _, offset := range offsets {
		if offset.Topic == nil {
			continue
		}
		key := offsetKey{topic: *offset.Topic, partition: offset.Partition}
		if previous, ok := b.offsets[key]; ok && previous.Offset >= offset.Offset {
			continue
		}
		topic := *offset.Topic
		offset.Topic = &topic
		b.offsets[key] = offset
		b.updates++
	}
}

func (b *offsetCommitBuffer) ShouldFlush(now time.Time) bool {
	if len(b.offsets) == 0 {
		return false
	}
	return b.updates >= offsetCommitUpdateThreshold || now.Sub(b.lastAttempt) >= offsetCommitInterval
}

func (b *offsetCommitBuffer) MarkAttempt(now time.Time) {
	b.lastAttempt = now
	b.updates = 0
}

func (b *offsetCommitBuffer) MarkCommitted(now time.Time) {
	clear(b.offsets)
	b.updates = 0
	b.lastAttempt = now
}

func (b *offsetCommitBuffer) Offsets() []kafka.TopicPartition {
	if len(b.offsets) == 0 {
		return nil
	}
	keys := make([]offsetKey, 0, len(b.offsets))
	for key := range b.offsets {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].topic != keys[j].topic {
			return keys[i].topic < keys[j].topic
		}
		return keys[i].partition < keys[j].partition
	})

	result := make([]kafka.TopicPartition, 0, len(keys))
	for _, key := range keys {
		offset := b.offsets[key]
		topic := *offset.Topic
		offset.Topic = &topic
		result = append(result, offset)
	}
	return result
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
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	queues, results, wg := c.startPartitionWorkers(workerCtx)
	commitBuffer := newOffsetCommitBuffer(time.Now())
	defer func() {
		cancelWorkers()
		for _, queue := range queues {
			close(queue)
		}
		wg.Wait()
		_ = c.drainWorkerResults(results, commitBuffer)
		commitBuffer.Add(c.engine.DrainCommittableOffsets())
		c.flushPendingOffsets(commitBuffer, true)
		if err := c.client.Close(); err != nil {
			log.Warn("close kafka consumer failed", zap.Error(err))
		}
	}()

	log.Info("kafka consumer workers started",
		zap.Int("partitionWorkers", len(queues)),
		zap.Duration("offsetCommitInterval", offsetCommitInterval),
		zap.Int("offsetCommitUpdateThreshold", offsetCommitUpdateThreshold))

	for {
		if err := c.drainWorkerResults(results, commitBuffer); err != nil {
			return errors.Trace(err)
		}
		commitBuffer.Add(c.engine.DrainCommittableOffsets())
		c.flushPendingOffsets(commitBuffer, false)

		select {
		case <-ctx.Done():
			log.Info("consumer exits: context cancelled")
			return errors.Trace(ctx.Err())
		default:
		}

		msg, err := c.client.ReadMessage(kafkaReadTimeout)
		if err != nil {
			if kerr, ok := err.(kafka.Error); ok && kerr.Code() == kafka.ErrTimedOut {
				continue
			}
			log.Warn("read kafka message failed, retry later", zap.Error(err))
			continue
		}

		partition := msg.TopicPartition.Partition
		if partition < 0 || int(partition) >= len(queues) {
			return errors.Errorf("received message from unexpected partition %d, configured partitions %d",
				partition, len(queues))
		}
		if err := c.enqueueMessage(ctx, queues[partition], results, commitBuffer, msg); err != nil {
			return errors.Trace(err)
		}
	}
}

func (c *consumer) startPartitionWorkers(ctx context.Context) ([]chan *kafka.Message, chan workerResult, *sync.WaitGroup) {
	partitionNum := len(c.engine.partitions)
	queues := make([]chan *kafka.Message, partitionNum)
	results := make(chan workerResult, partitionNum*partitionMessageBufferSize)
	wg := &sync.WaitGroup{}
	for i := range queues {
		queues[i] = make(chan *kafka.Message, partitionMessageBufferSize)
		wg.Add(1)
		go c.partitionWorker(ctx, queues[i], results, wg)
	}
	return queues, results, wg
}

func (c *consumer) partitionWorker(
	ctx context.Context, messages <-chan *kafka.Message, results chan<- workerResult, wg *sync.WaitGroup,
) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-messages:
			if !ok {
				return
			}
			committable, err := c.engine.HandleMessage(ctx, msg)
			result := workerResult{committable: committable, err: err}
			select {
			case results <- result:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}
}

func (c *consumer) enqueueMessage(
	ctx context.Context,
	queue chan<- *kafka.Message,
	results <-chan workerResult,
	commitBuffer *offsetCommitBuffer,
	msg *kafka.Message,
) error {
	for {
		select {
		case queue <- msg:
			return nil
		case result := <-results:
			if err := c.handleWorkerResult(result, commitBuffer); err != nil {
				return errors.Trace(err)
			}
			commitBuffer.Add(c.engine.DrainCommittableOffsets())
			c.flushPendingOffsets(commitBuffer, false)
		case <-ctx.Done():
			log.Info("consumer exits: context cancelled")
			return errors.Trace(ctx.Err())
		}
	}
}

func (c *consumer) drainWorkerResults(results <-chan workerResult, commitBuffer *offsetCommitBuffer) error {
	for {
		select {
		case result := <-results:
			if err := c.handleWorkerResult(result, commitBuffer); err != nil {
				return errors.Trace(err)
			}
		default:
			return nil
		}
	}
}

func (c *consumer) handleWorkerResult(result workerResult, commitBuffer *offsetCommitBuffer) error {
	if result.err != nil {
		return errors.Trace(result.err)
	}
	commitBuffer.Add(result.committable)
	return nil
}

func (c *consumer) flushPendingOffsets(commitBuffer *offsetCommitBuffer, force bool) {
	now := time.Now()
	if !force && !commitBuffer.ShouldFlush(now) {
		return
	}
	offsets := commitBuffer.Offsets()
	if len(offsets) == 0 {
		return
	}
	commitBuffer.MarkAttempt(now)
	committed, err := c.client.CommitOffsets(offsets)
	if err != nil {
		log.Warn("commit kafka offsets failed; messages may be replayed",
			zap.Any("offsets", offsets), zap.Error(err))
		return
	}
	commitBuffer.MarkCommitted(time.Now())
	log.Debug("commit kafka offsets success", zap.Any("offsets", committed))
}

func (c *consumer) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return c.engine.Run(ctx)
	})
	g.Go(func() error {
		return c.engine.runLatencyReporter(ctx)
	})
	g.Go(func() error {
		return c.readMessage(ctx)
	})
	return g.Wait()
}
