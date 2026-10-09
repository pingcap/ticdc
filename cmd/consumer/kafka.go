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
	"database/sql"
	"math"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/simple"
	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

type kafkaReader struct {
	client                *kgo.Client
	upstreamDB            *sql.DB
	topic                 string
	buffer                *readBuffer
	mu                    sync.Mutex
	offsets               map[*inputRecord]int64
	polled                []*kgo.Record
	polledIndex           int
	ddlCopies             map[uint64][]*inputRecord
	deliveredWatermark    uint64
	hasDeliveredWatermark bool
}

func newKafkaReader(ctx context.Context, upstreamURI *url.URL, consumerID, timezone string, replicaConfig *config.ReplicaConfig, memory *bufferUsage) (*kafkaReader, error) {
	topic := strings.Trim(upstreamURI.Path, "/")
	if strings.Contains(topic, ",") {
		return nil, errors.ErrKafkaInvalidConfig.FastGenByArgs("cdc_consumer accepts one Kafka topic")
	}
	protocol, err := config.ParseSinkProtocolFromString(upstreamURI.Query().Get(config.ProtocolKey))
	if err != nil {
		return nil, err
	}
	switch protocol {
	case config.ProtocolOpen, config.ProtocolCanalJSON, config.ProtocolAvro, config.ProtocolSimple,
		config.ProtocolDebezium, config.ProtocolDebeziumAvro:
	default:
		return nil, errors.ErrKafkaInvalidConfig.FastGenByArgs("unsupported Kafka protocol " + protocol.String())
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

	codecConfig := codecCommon.NewConfig(protocol)
	if err := codecConfig.Apply(upstreamURI, replicaConfig.Sink); err != nil {
		client.Close()
		return nil, err
	}
	switch protocol {
	case config.ProtocolCanalJSON, config.ProtocolDebezium:
		if !codecConfig.EnableTiDBExtension {
			client.Close()
			return nil, errors.ErrCodecInvalidConfig.FastGenByArgs("enable-tidb-extension must be true")
		}
	case config.ProtocolAvro, config.ProtocolDebeziumAvro:
		if !codecConfig.EnableTiDBExtension || !codecConfig.AvroEnableWatermark {
			client.Close()
			return nil, errors.ErrCodecInvalidConfig.FastGenByArgs("enable-tidb-extension and avro-enable-watermark must be true")
		}
		if codecConfig.AvroConfluentSchemaRegistry == "" {
			client.Close()
			return nil, errors.ErrCodecInvalidConfig.FastGenByArgs("schema-registry is required")
		}
	}
	codecConfig.TimeZone, err = putil.GetTimezone(timezone)
	if err != nil {
		client.Close()
		return nil, err
	}
	if (protocol == config.ProtocolDebezium || protocol == config.ProtocolDebeziumAvro) && codecConfig.DebeziumDisableSchema {
		client.Close()
		return nil, errors.ErrCodecInvalidConfig.FastGenByArgs("debezium-disable-schema must be false")
	}
	var db *sql.DB
	if dsn := upstreamURI.Query().Get("upstream-tidb-dsn"); dsn != "" {
		db, err = sql.Open("mysql", dsn)
		if err != nil {
			client.Close()
			return nil, errors.WrapError(errors.ErrMySQLConnectionError, err, "open consumer upstream TiDB")
		}
		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(10)
		db.SetConnMaxLifetime(10 * time.Minute)
		pingCtx, cancelPing := context.WithTimeout(ctx, 5*time.Second)
		err = db.PingContext(pingCtx)
		cancelPing()
		if err != nil {
			_ = db.Close()
			client.Close()
			return nil, errors.WrapError(errors.ErrMySQLConnectionError, err, "ping consumer upstream TiDB")
		}
	}
	partitions := make(map[int32]*partition, len(topicMetadata.Partitions))
	for partitionID := range topicMetadata.Partitions {
		decoder, err := codec.NewEventDecoder(ctx, int(partitionID), codecConfig, topic, db)
		if err != nil {
			if db != nil {
				_ = db.Close()
			}
			client.Close()
			return nil, err
		}
		partitions[partitionID] = &partition{decoder: decoder, schemas: make(map[schemaKey]bool), schemaPointers: make(map[*commonType.TableInfo]bool)}
	}

	memory.externalBytes = client.BufferedFetchBytes
	buffer := &readBuffer{memory: memory, protocol: protocol, partitions: partitions}
	log.Info("Kafka reader initialized", zap.String("topic", topic), zap.Int("partitionCount", len(partitions)))
	return &kafkaReader{client: client, upstreamDB: db, topic: topic, buffer: buffer, offsets: make(map[*inputRecord]int64), ddlCopies: make(map[uint64][]*inputRecord)}, nil
}

func (c *kafkaReader) Read(ctx context.Context) (*readResult, error) {
	for {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		c.limitReads()
		watermark, ready := c.globalWatermark()
		if ready || len(c.buffer.pendingDDL) != 0 {
			if result := c.buffer.nextReady(watermark); result != nil {
				return result, nil
			}
		}
		if ready && (!c.hasDeliveredWatermark || watermark > c.deliveredWatermark) {
			completed := make([]*inputRecord, 0)
			for commitTs, records := range c.ddlCopies {
				if commitTs <= watermark {
					completed = append(completed, records...)
					delete(c.ddlCopies, commitTs)
				}
			}
			c.buffer.memory.readBytes.Add(-int64(len(completed)) * 128)
			c.deliveredWatermark = watermark
			c.hasDeliveredWatermark = true
			result := &readResult{watermark: watermark, hasWatermark: true}
			if len(completed) != 0 {
				// Earlier DDL results are written before this watermark is consumed.
				// Its completion also waits for DML through the same timestamp.
				result.onFlush = func() {
					for _, record := range completed {
						record.pending.Add(-1)
						c.buffer.memory.effects.Add(-1)
					}
					c.buffer.memory.bytes.Add(-int64(len(completed)) * 128)
				}
			}
			return result, nil
		}
		if c.polledIndex == len(c.polled) {
			clear(c.polled)
			c.polled = nil
			c.polledIndex = 0
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
				c.polled = append(c.polled, record)
				bytes += int64(len(record.Key) + len(record.Value) + 128)
			}
			if err := c.buffer.memory.reserve(bytes); err != nil {
				return nil, err
			}
			c.buffer.memory.readBytes.Add(bytes)
			if len(c.polled) == 0 {
				continue
			}
		}
		record := c.polled[c.polledIndex]
		c.polled[c.polledIndex] = nil
		c.polledIndex++
		bytes := int64(len(record.Key) + len(record.Value) + 128)
		c.buffer.memory.bytes.Add(-bytes)
		c.buffer.memory.readBytes.Add(-bytes)
		if err := c.processRecord(record); err != nil {
			return nil, err
		}
	}
}

func (c *kafkaReader) processRecord(record *kgo.Record) error {
	p, ok := c.buffer.partitions[record.Partition]
	if !ok {
		return errors.ErrInternalCheckFailed.FastGenByArgs("Kafka record belongs to an unknown partition")
	}
	bytes := int64(len(record.Key) + len(record.Value) + 128)
	if bytes > maxRecordBytes {
		return errors.ErrInternalCheckFailed.FastGenByArgs("Kafka record exceeds its size limit")
	}
	state, err := c.buffer.newRecord(bytes)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.offsets[state] = record.Offset
	p.records = append(p.records, state)
	c.mu.Unlock()
	p.decoder.AddKeyValue(record.Key, record.Value)
	for {
		messageType, hasNext := p.decoder.HasNext()
		if !hasNext {
			break
		}
		switch messageType {
		case codecCommon.MessageTypeRow:
			message := p.decoder.NextDMLMessage()
			if message == nil {
				if _, ok := p.decoder.(*simple.Decoder); !ok {
					return errors.ErrCodecDecode.FastGenByArgs("decoder returned an empty DML message")
				}
				state.pending.Add(1)
				if c.buffer.memory.effects.Add(1) > maxEffects {
					return errors.ErrInternalCheckFailed.FastGenByArgs("consumer cached DML exceeds its count limit")
				}
				p.cachedUnreleased++
				p.cachedRecords = append(p.cachedRecords, state)
				continue
			}
			if err := c.buffer.queueDML(message.ToDMLEvent(), []*inputRecord{state}, p); err != nil {
				return err
			}
		case codecCommon.MessageTypeDDL:
			if c.buffer.protocol == config.ProtocolCanalJSON && record.Partition != 0 {
				return errors.ErrCodecDecode.FastGenByArgs("Canal JSON DDL must come from partition 0")
			}
			ddl := p.decoder.NextDDLEvent()
			if ddl == nil {
				return errors.ErrCodecDecode.FastGenByArgs("decoder returned an empty DDL event")
			}
			if err := c.buffer.trackSchema(p, ddl.TableInfo); err != nil {
				return err
			}
			for _, info := range ddl.MultipleTableInfos {
				if err := c.buffer.trackSchema(p, info); err != nil {
					return err
				}
			}
			if c.buffer.protocol == config.ProtocolCanalJSON {
				key := schemaKey{schema: ddl.SchemaName, table: ddl.TableName, version: ddl.FinishedTs}
				if !p.schemas[key] {
					if c.buffer.schemaCount >= maxSchemas || c.buffer.schemaBytes+128 > maxSchemaBytes {
						return errors.ErrInternalCheckFailed.FastGenByArgs("consumer schema cache exceeds its resource limit")
					}
					if err := c.buffer.memory.reserve(128); err != nil {
						return err
					}
					p.schemas[key] = true
					c.buffer.schemaBytes += 128
					c.buffer.schemaCount++
					c.buffer.memory.readBytes.Add(128)
				}
			}
			if decoder, ok := p.decoder.(*simple.Decoder); ok {
				records := slices.Clone(p.cachedRecords)
				for _, message := range decoder.GetCachedMessages() {
					if p.cachedUnreleased == 0 {
						return errors.ErrInternalCheckFailed.FastGenByArgs("Simple Protocol released a DML message without its Kafka record")
					}
					p.cachedUnreleased--
					if err := c.buffer.queueDML(message.ToDMLEvent(), records, p); err != nil {
						return err
					}
				}
				if p.cachedUnreleased == 0 {
					for _, cached := range p.cachedRecords {
						cached.pending.Add(-1)
						c.buffer.memory.effects.Add(-1)
					}
					clear(p.cachedRecords)
					p.cachedRecords = nil
				}
			}
			if ddl.Query == "" {
				if c.buffer.protocol != config.ProtocolSimple {
					return errors.ErrCodecDecode.FastGenByArgs("DDL query is empty")
				}
				continue
			}
			if record.Partition != 0 {
				// Only partition zero supplies executable DDLs. Other copies wait
				// for downstream progress without matching individual statements.
				if err := c.buffer.memory.reserve(128); err != nil {
					return err
				}
				if c.buffer.memory.effects.Add(1) > maxEffects {
					return errors.ErrInternalCheckFailed.FastGenByArgs("consumer DDL copies exceed the effect limit")
				}
				state.pending.Add(1)
				c.buffer.memory.readBytes.Add(128)
				c.ddlCopies[ddl.GetCommitTs()] = append(c.ddlCopies[ddl.GetCommitTs()], state)
				continue
			}
			if err := c.buffer.queueDDL(ddl, state); err != nil {
				return err
			}
		case codecCommon.MessageTypeResolved:
			watermark := p.decoder.NextResolvedEvent()
			if !p.hasWatermark || watermark > p.watermark {
				p.watermark = watermark
				p.hasWatermark = true
			}
		default:
			return errors.ErrCodecDecode.FastGenByArgs("decoder returned an unknown message type")
		}
	}
	state.pending.Add(-1)
	c.buffer.memory.effects.Add(-1)
	return nil
}

func (c *kafkaReader) globalWatermark() (uint64, bool) {
	watermark := uint64(math.MaxUint64)
	for _, partition := range c.buffer.partitions {
		if !partition.hasWatermark || partition.cachedUnreleased != 0 {
			return 0, false
		}
		watermark = min(watermark, partition.watermark)
	}
	return watermark, true
}

func (c *kafkaReader) Confirm(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	records := make([]*kgo.Record, 0, len(c.buffer.partitions))
	counts := make(map[int32]int, len(c.buffer.partitions))
	for partitionID, p := range c.buffer.partitions {
		for index, record := range p.records {
			pending := record.pending.Load()
			if pending < 0 {
				return errors.ErrInternalCheckFailed.FastGenByArgs("Kafka input completed more than once")
			}
			if pending != 0 {
				break
			}
			counts[partitionID] = index + 1
		}
		if count := counts[partitionID]; count != 0 {
			records = append(records, &kgo.Record{Topic: c.topic, Partition: partitionID, Offset: c.offsets[p.records[count-1]]})
		}
	}
	if len(records) == 0 {
		return nil
	}
	if err := c.client.CommitRecords(ctx, records...); err != nil {
		return errors.WrapError(errors.ErrInternalCheckFailed, err, "commit Kafka offsets")
	}
	for partitionID, count := range counts {
		p := c.buffer.partitions[partitionID]
		for _, record := range p.records[:count] {
			bytes := record.bytes.Load()
			c.buffer.memory.bytes.Add(-bytes)
			c.buffer.memory.readBytes.Add(-bytes)
			c.buffer.memory.records.Add(-1)
			delete(c.offsets, record)
		}
		copy(p.records, p.records[count:])
		clear(p.records[len(p.records)-count:])
		p.records = p.records[:len(p.records)-count]
		c.buffer.memory.confirmed.Add(int64(count))
	}
	return nil
}

func (c *kafkaReader) limitReads() {
	var slowest uint64 = math.MaxUint64
	for _, partition := range c.buffer.partitions {
		if !partition.hasWatermark {
			slowest = 0
			break
		}
		slowest = min(slowest, partition.watermark)
	}
	for partitionID, partition := range c.buffer.partitions {
		// Leave the remaining 32 MiB available for the partitions that can
		// advance the boundary, including Simple schema bootstrap messages.
		pause := c.buffer.memory.bytes.Load()+c.client.BufferedFetchBytes() >= memoryHighWater && partition.hasWatermark && partition.watermark > slowest && partition.cachedUnreleased == 0
		if pause == partition.paused {
			continue
		}
		partitions := map[string][]int32{c.topic: {partitionID}}
		if pause {
			c.client.PauseFetchPartitions(partitions)
		} else {
			c.client.ResumeFetchPartitions(partitions)
		}
		partition.paused = pause
	}
}

func (c *kafkaReader) BufferedBytes() int64 {
	return c.buffer.memory.readBytes.Load() + c.client.BufferedFetchBytes()
}

func (c *kafkaReader) Close() error {
	c.client.Close()
	if c.upstreamDB != nil {
		if err := c.upstreamDB.Close(); err != nil {
			return errors.WrapError(errors.ErrMySQLConnectionError, err, "close consumer upstream TiDB")
		}
	}
	return nil
}
