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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"crypto/tls"
	"math"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

type kafkaReader struct {
	client       *kgo.Client
	topic        string
	memory       *memoryUsage
	progress     map[int32]*readProgress
	mu           sync.Mutex
	offsets      map[*ack]int64
	records      map[int32][]*ack
	polled       map[int32][]*kgo.Record
	paused       map[int32]bool
	sequences    map[int32]uint64
	readSequence uint64
}

func newKafkaReader(ctx context.Context, upstreamURI *url.URL, consumerID string, memory *memoryUsage) (*kafkaReader, error) {
	if upstreamURI.Host == "" {
		return nil, errors.ErrInvalidReplicaConfig.FastGenByArgs("kafka upstream-uri must include an endpoint")
	}
	topic := strings.Trim(upstreamURI.Path, "/")
	if topic == "" {
		return nil, errors.ErrInvalidReplicaConfig.FastGenByArgs("kafka upstream-uri must include a topic")
	}
	if strings.Contains(topic, ",") {
		return nil, errors.ErrKafkaInvalidConfig.FastGenByArgs("cdc_consumer accepts one Kafka topic")
	}
	kafkaOptions := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(upstreamURI.Host, ",")...),
		kgo.ConsumerGroup(consumerID),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.Balancers(kgo.RangeBalancer()),
		kgo.FetchMaxBytes(4 << 20),
		kgo.FetchMaxPartitionBytes(1 << 20),
		kgo.FetchMaxWait(100 * time.Millisecond),
		kgo.MaxConcurrentFetches(1),
		kgo.BrokerMaxReadBytes(32 << 20),
	}
	if upstreamURI.Scheme == config.KafkaSSLScheme {
		kafkaOptions = append(kafkaOptions, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}
	client, err := kgo.NewClient(kafkaOptions...)
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaInvalidConfig, err)
	}

	metadata, err := kadm.NewClient(client).Metadata(ctx, topic)
	if err != nil {
		client.Close()
		return nil, errors.WrapError(errors.ErrKafkaAdminAPI, err, "get metadata", topic)
	}
	topicMetadata, ok := metadata.Topics[topic]
	if !ok || topicMetadata.Err != nil || len(topicMetadata.Partitions) == 0 {
		client.Close()
		if ok && topicMetadata.Err != nil {
			return nil, errors.WrapError(errors.ErrKafkaAdminAPI, topicMetadata.Err, "get metadata", topic)
		}
		return nil, errors.ErrKafkaAdminAPI.GenWithStackByArgs("get metadata", topic)
	}

	progress := make(map[int32]*readProgress, len(topicMetadata.Partitions))
	for partitionID := range topicMetadata.Partitions {
		progress[partitionID] = &readProgress{}
	}

	memory.externalBytes = client.BufferedFetchBytes
	log.Info("Kafka reader initialized", zap.String("topic", topic), zap.Int("partitionCount", len(progress)))
	return &kafkaReader{
		client: client, topic: topic, memory: memory, progress: progress,
		offsets: make(map[*ack]int64), records: make(map[int32][]*ack),
		polled: make(map[int32][]*kgo.Record), paused: make(map[int32]bool), sequences: make(map[int32]uint64),
	}, nil
}

func (c *kafkaReader) Read(ctx context.Context) (*readData, error) {
	for {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		c.limitReads()
		partitionID := c.nextPartition()
		if partitionID < 0 {
			fetches := c.client.PollRecords(ctx, 128)
			if err := context.Cause(ctx); err != nil {
				return nil, err
			}
			if fetches.IsClientClosed() {
				return nil, errors.ErrKafkaSinkClosed.GenWithStackByArgs()
			}
			for _, fetchError := range fetches.Errors() {
				return nil, errors.WrapError(errors.ErrInternalCheckFailed, fetchError.Err, "read Kafka partition")
			}
			bytes := int64(0)
			for iterator := fetches.RecordIter(); !iterator.Done(); {
				record := iterator.Next()
				if _, ok := c.progress[record.Partition]; !ok {
					return nil, errors.ErrInternalCheckFailed.FastGenByArgs("Kafka record belongs to an unknown partition")
				}
				c.polled[record.Partition] = append(c.polled[record.Partition], record)
				bytes += int64(len(record.Key) + len(record.Value) + 128)
			}
			if err := c.memory.reserve(ctx, bytes); err != nil {
				return nil, err
			}
			continue
		}
		queue := c.polled[partitionID]
		record := queue[0]
		queue[0] = nil
		if len(queue) == 1 {
			delete(c.polled, partitionID)
		} else {
			c.polled[partitionID] = queue[1:]
		}
		c.readSequence++
		c.sequences[partitionID] = c.readSequence
		bytes := int64(len(record.Key) + len(record.Value) + 128)
		c.memory.release(bytes)
		state, err := c.memory.newAck(ctx, bytes)
		if err != nil {
			return nil, err
		}
		c.mu.Lock()
		c.offsets[state] = record.Offset
		c.records[partitionID] = append(c.records[partitionID], state)
		c.mu.Unlock()
		return &readData{key: record.Key, value: record.Value, partition: partitionID, record: state}, nil
	}
}

func (c *kafkaReader) Confirm(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	records := make([]*kgo.Record, 0, len(c.progress))
	counts := make(map[int32]int, len(c.progress))
	for partitionID, inputs := range c.records {
		for index, record := range inputs {
			refs := record.refs.Load()
			if refs < 0 {
				return errors.ErrInternalCheckFailed.FastGenByArgs("Kafka input completed more than once")
			}
			if refs != 0 {
				break
			}
			counts[partitionID] = index + 1
		}
		if count := counts[partitionID]; count != 0 {
			records = append(records, &kgo.Record{Topic: c.topic, Partition: partitionID, Offset: c.offsets[inputs[count-1]]})
		}
	}
	if len(records) == 0 {
		return nil
	}
	if err := c.client.CommitRecords(ctx, records...); err != nil {
		return errors.WrapError(errors.ErrInternalCheckFailed, err, "commit Kafka offsets")
	}
	for partitionID, count := range counts {
		inputs := c.records[partitionID]
		for _, record := range inputs[:count] {
			c.memory.confirm(record)
			delete(c.offsets, record)
		}
		c.records[partitionID] = slices.Delete(inputs, 0, count)
	}
	return nil
}

func (c *kafkaReader) limitReads() {
	var slowest uint64 = math.MaxUint64
	for _, partition := range c.progress {
		if !partition.hasWatermark {
			slowest = 0
			break
		}
		slowest = min(slowest, partition.watermark)
	}
	for partitionID, partition := range c.progress {
		// Fetch the partitions holding back progress first. Missing schema
		// bootstrap messages must remain readable regardless of their watermark.
		pause := partition.hasWatermark && partition.watermark > slowest && !partition.needsMoreInput
		if pause == c.paused[partitionID] {
			continue
		}
		partitions := map[string][]int32{c.topic: {partitionID}}
		if pause {
			c.client.PauseFetchPartitions(partitions)
		} else {
			c.client.ResumeFetchPartitions(partitions)
		}
		c.paused[partitionID] = pause
	}
}

func (c *kafkaReader) nextPartition() int32 {
	selected := int32(-1)
	var progress, sequence uint64
	for partitionID, records := range c.polled {
		p := c.progress[partitionID]
		if len(records) == 0 || c.paused[partitionID] {
			continue
		}
		watermark := p.watermark
		if !p.hasWatermark || p.needsMoreInput {
			watermark = 0
		}
		if selected < 0 || watermark < progress || (watermark == progress && (c.sequences[partitionID] < sequence || (c.sequences[partitionID] == sequence && partitionID < selected))) {
			selected, progress, sequence = partitionID, watermark, c.sequences[partitionID]
		}
	}
	return selected
}

func (c *kafkaReader) Close() error {
	c.client.Close()
	return nil
}
