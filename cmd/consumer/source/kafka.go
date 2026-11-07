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

package source

import (
	"cmp"
	"context"
	"crypto/tls"
	"crypto/x509"
	"math"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/consumer/budget"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter/partition"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

type kafkaPartition struct {
	fetchPaused bool
	targetTs    uint64 // Event progress still needed for the current boundary; zero means complete.
}

type kafkaReader struct {
	protocol        config.Protocol
	router          *eventrouter.EventRouter
	maxMessageBytes int
	maxBatchSize    int
	client          *kgo.Client
	topic           string
	memory          *budget.Budget
	continuing      bool
	prefetchedBytes int64 // SDK bytes already included in the shared budget.
	partitions      map[int32]*kafkaPartition
	tables          []event.SchemaTableName
	selected        []int32 // Cached routing for tables; nil selects every partition.
}

func newKafkaReader(ctx context.Context, opts Options) *kafkaReader {
	upstreamURI := opts.UpstreamURI
	consumerID := opts.ConsumerID
	replicaConfig := opts.ReplicaConfig
	codecConfig := opts.CodecConfig
	memory := opts.Budget
	topic := strings.Trim(upstreamURI.Path, "/")
	if strings.Contains(topic, ",") {
		log.Panic("consumer accepts one Kafka topic", zap.String("topic", topic))
	}
	router, err := eventrouter.NewEventRouter(replicaConfig.Sink, util.GetOrZero(replicaConfig.CaseSensitive), topic, false, codecConfig.Protocol == config.ProtocolAvro)
	if err != nil {
		log.Panic("Kafka event routing configuration is invalid", zap.Error(err))
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
		kgo.BrokerMaxReadBytes(32 << 20),
	}
	query := upstreamURI.Query()
	maxMessageBytes, maxBatchSize := math.MaxInt, math.MaxInt
	if query.Has("max-message-bytes") {
		maxMessageBytes = codecConfig.MaxMessageBytes
	}
	if query.Has("max-batch-size") {
		maxBatchSize = codecConfig.MaxBatchSize
	}
	ca, cert, key := query.Get("ca"), query.Get("cert"), query.Get("key")
	if upstreamURI.Scheme == config.KafkaSSLScheme || cmp.Or(ca, cert, key) != "" {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
		if ca != "" {
			pem, err := os.ReadFile(ca)
			if err != nil {
				log.Panic("Kafka CA certificate loading failed", zap.Error(errors.WrapError(errors.ErrKafkaInvalidConfig, err)))
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				log.Panic("Kafka CA certificate is invalid", zap.Error(errors.ErrKafkaInvalidConfig.GenWithStack("no certificate found in %s", ca)))
			}
			tlsConfig.RootCAs = pool
		}
		if cert != "" || key != "" {
			certificate, err := tls.LoadX509KeyPair(cert, key)
			if err != nil {
				log.Panic("Kafka client certificate loading failed", zap.Error(errors.WrapError(errors.ErrKafkaInvalidConfig, err)))
			}
			tlsConfig.Certificates = []tls.Certificate{
				certificate,
			}
		}
		kafkaOptions = append(kafkaOptions, kgo.DialTLSConfig(tlsConfig))
	}
	client, err := kgo.NewClient(kafkaOptions...)
	if err != nil {
		log.Panic("Kafka client initialization failed", zap.Error(err))
	}
	admin := kadm.NewClient(client)
	metadata, err := admin.Metadata(ctx, topic)
	if err != nil {
		log.Panic("Kafka topic metadata lookup failed", zap.String("topic", topic), zap.Error(err))
	}
	topicMetadata, ok := metadata.Topics[topic]
	if !ok || topicMetadata.Err != nil || len(topicMetadata.Partitions) == 0 {
		log.Panic("Kafka topic metadata is unavailable", zap.String("topic", topic), zap.Error(topicMetadata.Err), zap.Int("partitionCount", len(topicMetadata.Partitions)))
	}

	partitions := make(map[int32]*kafkaPartition, len(topicMetadata.Partitions))
	for partitionID := range topicMetadata.Partitions {
		partitions[partitionID] = &kafkaPartition{}
	}
	log.Info("kafka reader initialized", zap.String("topic", topic), zap.Int("partitionCount", len(partitions)))
	return &kafkaReader{
		protocol:        codecConfig.Protocol,
		router:          router,
		maxMessageBytes: maxMessageBytes,
		maxBatchSize:    maxBatchSize,
		client:          client,
		topic:           topic,
		memory:          memory,
		continuing:      true,
		partitions:      partitions,
	}
}

func (c *kafkaReader) Streams() []Stream {
	streams := make([]Stream, 0, len(c.partitions))
	for partitionID := range c.partitions {
		streams = append(streams, Stream{
			ID:           partitionID,
			Control:      partitionID == 0,
			Topic:        c.topic,
			DDLBroadcast: helper.GetDDLDispatchRule(c.protocol) == helper.PartitionAll,
		})
	}
	return streams
}

func (c *kafkaReader) Read(ctx context.Context) ([]*Input, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		default:
		}
		prefetched := c.client.BufferedFetchBytes()
		if prefetched > c.prefetchedBytes {
			c.memory.ForceAcquire(prefetched - c.prefetchedBytes)
		}
		if prefetched < c.prefetchedBytes {
			c.memory.Release(c.prefetchedBytes - prefetched)
		}
		c.prefetchedBytes = prefetched
		fetches := c.client.PollRecords(ctx, readBatchRows)
		if err := fetches.Err(); err != nil {
			if cause := context.Cause(ctx); cause != nil {
				return nil, cause
			}
			if fetches.IsClientClosed() {
				return nil, errors.ErrKafkaSinkClosed.GenWithStackByArgs()
			}
			return nil, errors.WrapError(errors.ErrInternalCheckFailed, err, "read Kafka partition")
		}

		var (
			batch []*Input
			bytes int64
		)
		for iterator := fetches.RecordIter(); !iterator.Done(); {
			record := iterator.Next()
			position := kgo.Record{
				Topic:     c.topic,
				Partition: record.Partition,
				Offset:    record.Offset,
			}
			size := int64(len(record.Key) + len(record.Value))
			batch = append(batch, &Input{
				Stream: record.Partition,
				Key:    record.Key,
				Value:  record.Value,
				Bytes:  size,
				Offset: record.Offset,
				Ack: func(ctx context.Context) error {
					return c.confirmPosition(ctx, &position)
				},
			})
			bytes += size
		}
		if len(batch) == 0 {
			continue
		}
		// Transfer already-accounted SDK bytes to the returned inputs.
		accounted := min(bytes, c.prefetchedBytes)
		c.prefetchedBytes -= accounted
		c.memory.ForceAcquire(bytes - accounted)
		return batch, nil
	}
}

func (c *kafkaReader) Semantics() Semantics {
	return Semantics{CommitOrdered: true}
}

func (c *kafkaReader) Advance(progress Progress) bool {
	if (progress.Scope == nil) != (c.tables == nil) || !slices.Equal(progress.Scope, c.tables) {
		c.selected = c.tablePartitions(progress.Scope)
		c.tables = slices.Clone(progress.Scope)
	}
	if progress.Retarget {
		for _, part := range c.partitions {
			part.targetTs = progress.Target
		}
	}
	for _, watermark := range progress.Watermarks {
		part, ok := c.partitions[watermark.Stream]
		if !ok {
			continue
		}
		if watermark.Timestamp >= part.targetTs ||
			(c.selected != nil && !slices.Contains(c.selected, watermark.Stream)) {
			part.targetTs = 0
		}
	}
	for _, stream := range progress.Proven {
		if part, ok := c.partitions[stream]; ok {
			part.targetTs = 0
		}
	}
	reached := true
	for _, part := range c.partitions {
		reached = reached && part.targetTs == 0
	}
	c.limitReads()
	return reached
}

func (c *kafkaReader) ContinueReading(enabled bool) {
	c.continuing = enabled
	c.limitReads()
}

func (c *kafkaReader) limitReads() {
	boundaryPending := false
	for _, part := range c.partitions {
		boundaryPending = boundaryPending || part.targetTs != 0
	}
	underPressure := c.memory.Used() >= c.memory.Limit()
	for partitionID, part := range c.partitions {
		pause := underPressure && (!c.continuing || (boundaryPending && part.targetTs == 0))
		if pause == part.fetchPaused {
			continue
		}
		partitions := map[string][]int32{
			c.topic: {
				partitionID,
			},
		}
		switch {
		case pause:
			c.client.PauseFetchPartitions(partitions)
		default:
			c.client.ResumeFetchPartitions(partitions)
		}
		part.fetchPaused = pause
	}
}

func (c *kafkaReader) Close() {
	c.client.Close()
	c.memory.Release(c.prefetchedBytes)
}

func (c *kafkaReader) confirmPosition(ctx context.Context, position *kgo.Record) error {
	if err := c.client.CommitRecords(ctx, position); err != nil {
		return errors.WrapError(errors.ErrInternalCheckFailed, err, "commit Kafka offsets")
	}
	return nil
}

// CheckMessage validates an input after decoding has revealed its DML count.
// A single oversized row is allowed by the Kafka encoder.
func (c *kafkaReader) CheckInput(origin Origin, rows int) {
	if rows > c.maxBatchSize {
		log.Panic("Kafka message batch size limit exceeded", zap.String("topic", c.topic), zap.Int32("partition", origin.Stream), zap.Int64("offset", origin.Offset), zap.Int("maxBatchSize", c.maxBatchSize), zap.Int("actualBatchSize", rows))
	}
	if rows > 1 && origin.Bytes > int64(c.maxMessageBytes) {
		log.Panic("Kafka message byte size limit exceeded", zap.String("topic", c.topic), zap.Int32("partition", origin.Stream), zap.Int64("offset", origin.Offset), zap.Int("maxMessageBytes", c.maxMessageBytes), zap.Int64("receivedBytes", origin.Bytes))
	}
}

func (c *kafkaReader) CheckDML(dml *event.DMLEvent, part int32, offset int64) {
	skipKeyOnlyDelete := c.protocol == config.ProtocolAvro && (len(dml.RowTypes) == 2 && dml.RowTypes[0] == common.RowTypeDelete && dml.RowTypes[1] == common.RowTypeInsert)
	generator := c.router.GetPartitionGenerator(dml.TableInfo.GetSchemaName(), dml.TableInfo.GetTableName())
	partitionCount := int32(len(c.partitions))
	dml.Rewind()
	defer dml.Rewind()
	for row, ok := dml.GetNextRow(); ok; row, ok = dml.GetNextRow() {
		// Avro UPDATE without a before image produces a keyed delete and a
		// complete insert. Only the insert can reproduce routing by nonkey columns.
		if skipKeyOnlyDelete && row.RowType == common.RowTypeDelete {
			continue
		}
		expected, _, err := generator.GeneratePartitionIndexAndKey(&row, partitionCount, dml.TableInfo, dml.CommitTs)
		if err != nil {
			log.Panic("Kafka DML partition calculation failed", zap.String("topic", c.topic), zap.Int32("partition", part), zap.Int64("offset", offset), zap.String("schema", dml.TableInfo.GetSchemaName()), zap.String("table", dml.TableInfo.GetTableName()), zap.Error(err))
		}
		if expected != part {
			log.Panic("Kafka DML was dispatched to the wrong partition", zap.String("topic", c.topic), zap.Int32("partition", part), zap.Int32("expectedPartition", expected), zap.Int32("partitionCount", partitionCount), zap.Int64("offset", offset), zap.String("schema", dml.TableInfo.GetSchemaName()), zap.String("table", dml.TableInfo.GetTableName()), zap.Uint64("commitTs", dml.CommitTs))
		}
	}
}

// A nil selection covers every partition; table routing can narrow the scope.
func (c *kafkaReader) tablePartitions(names []event.SchemaTableName) []int32 {
	if names != nil && len(names) == 0 {
		return []int32{0}
	}
	if c.router == nil || len(names) == 0 {
		return nil
	}
	var partitions []int32
	for _, name := range names {
		if name.SchemaName == "" || name.TableName == "" {
			return nil
		}
		generator := c.router.GetPartitionGenerator(name.SchemaName, name.TableName)
		switch generator.(type) {
		case *partition.TablePartitionGenerator:
		default:
			return nil
		}
		table := common.NewTableInfo4Decoder(name.SchemaName, &model.TableInfo{
			Name: ast.NewCIStr(name.TableName),
		})
		partitionID, _, err := generator.GeneratePartitionIndexAndKey(nil, int32(len(c.partitions)), table, 0)
		if err != nil {
			log.Panic("Kafka table partition generation failed", zap.String("schema", name.SchemaName), zap.String("table", name.TableName), zap.Error(err))
		}
		if !slices.Contains(partitions, partitionID) {
			partitions = append(partitions, partitionID)
		}
	}
	return partitions
}
