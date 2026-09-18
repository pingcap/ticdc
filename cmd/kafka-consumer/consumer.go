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
	"crypto/tls"
	"crypto/x509"
	"os"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
)

// consumerTopics is the configured topic list, without the blank entries a
// trailing comma leaves behind.
func consumerTopics(o *option) []string {
	topics := make([]string, 0, 1)
	for topic := range strings.SplitSeq(o.topic, ",") {
		if topic = strings.TrimSpace(topic); topic != "" {
			topics = append(topics, topic)
		}
	}
	return topics
}

// kafkaOptions builds the options shared by the metadata lookup and the
// consumer client itself.
func kafkaOptions(o *option) ([]kgo.Opt, error) {
	opts := []kgo.Opt{kgo.SeedBrokers(o.address...)}
	if len(o.ca) != 0 || len(o.cert) != 0 || len(o.key) != 0 {
		tlsConfig, err := newTLSConfig(o)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}
	if level, err := zapcore.ParseLevel(logLevel); err == nil && level == zapcore.DebugLevel {
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, nil)))
	}
	return opts, nil
}

// newTLSConfig builds the SSL setup the librdkafka options used to describe:
// the given CA file, plus the client certificate when one is configured. A
// configuration without a CA file uses the system roots.
func newTLSConfig(o *option) (*tls.Config, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if len(o.ca) != 0 {
		pem, err := os.ReadFile(o.ca)
		if err != nil {
			return nil, errors.Trace(err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.Errorf("no certificate found in %s", o.ca)
		}
		tlsConfig.RootCAs = pool
	}
	if len(o.cert) != 0 || len(o.key) != 0 {
		certificate, err := tls.LoadX509KeyPair(o.cert, o.key)
		if err != nil {
			return nil, errors.Trace(err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}
	return tlsConfig, nil
}

// getPartitionNum asks the cluster for the partition number of every consumed
// topic, retrying while the topic is not there yet.
func getPartitionNum(o *option) (int32, error) {
	opts, err := kafkaOptions(o)
	if err != nil {
		return 0, err
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)

	maxPartitionNum := int32(0)
	for _, topic := range consumerTopics(o) {
		found := false
		for i := range 31 {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			metadata, err := admin.Metadata(ctx, topic)
			cancel()
			if err == nil {
				if detail, ok := metadata.Topics[topic]; ok && detail.Err == nil {
					numPartitions := int32(len(detail.Partitions))
					log.Info("get partition number of topic",
						zap.String("topic", topic),
						zap.Int32("partitionNum", numPartitions))
					maxPartitionNum = max(maxPartitionNum, numPartitions)
					found = true
					break
				}
			}
			log.Info("retry get partition number", zap.String("topic", topic), zap.Int("retryTime", i))
			time.Sleep(time.Second)
		}
		if !found {
			return 0, errors.Errorf("get partition number(%s) timeout", topic)
		}
	}
	if maxPartitionNum == 0 {
		return 0, errors.Errorf("get partition number(%s) timeout", o.topic)
	}
	return maxPartitionNum, nil
}

type consumer struct {
	client *kgo.Client
	writer *writer

	// committedOffsets is the highest offset handed to the group coordinator for
	// each topic-partition. A commit replaces the stored offset instead of taking
	// its maximum, and a resolved message can be committed after records that
	// follow it, so the read loop must not send a commit that moves an offset
	// backwards: the group would replay records after a restart. Only the read
	// loop touches the map.
	committedOffsets map[topicPartition]int64
}

// topicPartition identifies one partition of one topic.
type topicPartition struct {
	topic     string
	partition int32
}

// newConsumer creates a consumer client. Offsets are committed from the read
// loop only, the client never commits on its own, and a group without committed
// offsets starts from the first message of every assigned partition, which are
// the librdkafka settings the consumer used before.
func newConsumer(ctx context.Context, o *option) *consumer {
	opts, err := kafkaOptions(o)
	if err != nil {
		log.Panic("create kafka consumer failed", zap.Error(err))
	}
	opts = append(opts,
		kgo.ConsumerGroup(o.groupID),
		kgo.ConsumeTopics(consumerTopics(o)...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		// Keep the eager range assignment the librdkafka default used, so a
		// consumer group that still has a member on the old client keeps
		// working while it rolls.
		kgo.Balancers(kgo.RangeBalancer()),
	)
	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Panic("create kafka consumer failed", zap.Error(err))
	}
	return &consumer{
		writer:           newWriter(ctx, o),
		client:           client,
		committedOffsets: make(map[topicPartition]int64),
	}
}

func (c *consumer) readMessage(ctx context.Context) error {
	defer c.client.Close()
	for {
		select {
		case <-ctx.Done():
			log.Info("consumer exist: context cancelled")
			return errors.Trace(ctx.Err())
		default:
		}

		fetches := c.client.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			log.Info("consumer exist: context cancelled")
			return errors.Trace(err)
		}
		// A fetch can carry records and errors at the same time, so a failed
		// partition is only logged: whatever did arrive is still processed.
		for _, fetchErr := range fetches.Errors() {
			log.Error("read message failed, just continue to retry",
				zap.String("topic", fetchErr.Topic), zap.Int32("partition", fetchErr.Partition),
				zap.Error(fetchErr.Err))
		}

		var writeErr error
		fetches.EachRecord(func(record *kgo.Record) {
			if writeErr != nil {
				return
			}
			needCommit, err := c.writer.WriteMessage(ctx, record)
			if err != nil {
				writeErr = err
				return
			}
			if needCommit {
				c.commitMessage(ctx, record)
			}
		})
		if writeErr != nil {
			return writeErr
		}
		// Resolved messages of the parallel resolve path are committed once
		// their events reached the downstream.
		for _, pending := range c.writer.takeCommittableMessages() {
			c.commitMessage(ctx, pending)
		}
	}
}

// commitMessage marks the offset after the message as processed. The commit is
// asynchronous, the read loop does not wait for the coordinator, which is what
// the librdkafka consumer did as well.
func (c *consumer) commitMessage(ctx context.Context, record *kgo.Record) {
	tp := topicPartition{topic: record.Topic, partition: record.Partition}
	offset := record.Offset + 1
	if offset <= c.committedOffsets[tp] {
		return
	}
	c.committedOffsets[tp] = offset
	offsets := map[string]map[int32]kgo.EpochOffset{
		record.Topic: {
			record.Partition: {Epoch: record.LeaderEpoch, Offset: offset},
		},
	}
	c.client.CommitOffsets(ctx, offsets,
		func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
			if err != nil {
				log.Error("commit message failed, just continue",
					zap.String("topic", record.Topic), zap.Int32("partition", record.Partition),
					zap.Int64("offset", record.Offset), zap.Error(err))
				return
			}
			log.Debug("commit message success",
				zap.String("topic", record.Topic), zap.Int32("partition", record.Partition),
				zap.Int64("offset", record.Offset))
		})
}

// Run the consumer, read data and write to the downstream target.
func (c *consumer) Run(ctx context.Context) (err error) {
	defer func() {
		c.writer.pipeline.stop()
		if cleanupErr := c.writer.cleanupEventsGroups(); err == nil && cleanupErr != nil {
			err = cleanupErr
		}
	}()

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return c.writer.run(ctx)
	})
	g.Go(func() error {
		return c.readMessage(ctx)
	})
	return g.Wait()
}
