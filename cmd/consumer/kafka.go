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
	"crypto/tls"
	"database/sql"
	"math"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/simple"
	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

type kafkaConsumer struct {
	client     *kgo.Client
	upstreamDB *sql.DB
	topic      string
	writer     *eventWriter
	offsets    map[*messageRecord]int64
}

func runKafkaConsumer(
	ctx context.Context,
	wg *sync.WaitGroup,
	upstreamURI *url.URL,
	downstreamURI string,
	consumerID string,
	timezone string,
	replicaConfig *config.ReplicaConfig,
) error {
	parentCtx := ctx
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	// Reading stops on the process context. The sink has a separate lifetime
	// so ready data can finish during the bounded shutdown drain.
	writeCtx, cancelWrite := context.WithCancelCause(context.WithoutCancel(ctx))
	defer cancelWrite(nil)
	wg.Go(func() {
		select {
		case <-parentCtx.Done():
			log.Info("consumer stopping", zap.Duration("shutdownTimeout", shutdownTimeout))
		case <-writeCtx.Done():
			return
		}
		// Also bound a block write already executing when the signal arrives.
		shutdownCtx, cancelShutdown := context.WithTimeout(writeCtx, shutdownTimeout)
		defer cancelShutdown()
		<-shutdownCtx.Done()
		if shutdownCtx.Err() == context.DeadlineExceeded {
			cancelWrite(errors.ErrInternalCheckFailed.FastGenByArgs("consumer shutdown drain timed out"))
		}
	})
	consumer, err := newKafkaConsumer(ctx, writeCtx, upstreamURI, downstreamURI, consumerID, timezone, replicaConfig)
	if err != nil {
		return err
	}
	defer consumer.client.Close()
	if consumer.upstreamDB != nil {
		defer consumer.upstreamDB.Close()
	}
	defer consumer.writer.downstream.Close()
	sinkDone := make(chan bool)
	defer func() {
		cancelWrite(nil)
		<-sinkDone
	}()

	wg.Go(func() {
		defer close(sinkDone)
		if err := consumer.writer.downstream.Run(writeCtx); err != nil {
			cancelWrite(err)
			cancel(err)
			return
		}
		if writeCtx.Err() == nil {
			err := errors.ErrInternalCheckFailed.FastGenByArgs("downstream sink stopped unexpectedly")
			cancelWrite(err)
			cancel(err)
		}
	})

	err = consumer.run(ctx, writeCtx)
	if writeErr := context.Cause(writeCtx); writeErr != nil {
		return writeErr
	}
	if parentCtx.Err() == nil || !errors.Is(err, context.Canceled) {
		return err
	}
	drainCtx, cancelDrain := context.WithTimeout(writeCtx, shutdownTimeout)
	defer cancelDrain()
	if watermark, ready := consumer.globalWatermark(); ready {
		if err := consumer.writer.flushReady(drainCtx, drainCtx, watermark, true); err != nil {
			return err
		}
	}
	for len(consumer.writer.inFlight) != 0 {
		if err := consumer.writer.waitBatch(drainCtx, consumer.writer.inFlight[0]); err != nil {
			return err
		}
	}
	if err := consumer.commitCompleted(drainCtx); err != nil {
		return err
	}
	return context.Cause(writeCtx)
}

func newKafkaConsumer(
	ctx context.Context,
	writeCtx context.Context,
	upstreamURI *url.URL,
	downstreamURI string,
	consumerID string,
	timezone string,
	replicaConfig *config.ReplicaConfig,
) (*kafkaConsumer, error) {
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

	codecConfig := codeccommon.NewConfig(protocol)
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
	if err := codecConfig.Validate(); err != nil {
		client.Close()
		return nil, err
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
			db.Close()
			client.Close()
			return nil, errors.WrapError(errors.ErrMySQLConnectionError, err, "ping consumer upstream TiDB")
		}
	}
	partitions := make(map[int32]*messagePartition, len(topicMetadata.Partitions))
	for partitionID := range topicMetadata.Partitions {
		decoder, err := codec.NewEventDecoder(ctx, int(partitionID), codecConfig, topic, db)
		if err != nil {
			if db != nil {
				db.Close()
			}
			client.Close()
			return nil, err
		}
		partitions[partitionID] = &messagePartition{decoder: decoder, schemas: make(map[schemaKey]bool), schemaPointers: make(map[*commonType.TableInfo]bool)}
	}

	replicaConfig.Sink.TiDBSourceID = 1
	changefeedID := commonType.NewChangeFeedIDWithName("consumer", commonType.DefaultKeyspaceName)
	downstream, err := sink.New(writeCtx, &config.ChangefeedConfig{
		ChangefeedID:           changefeedID,
		SinkURI:                downstreamURI,
		SinkConfig:             replicaConfig.Sink,
		CaseSensitive:          putil.GetOrZero(replicaConfig.CaseSensitive),
		EnableTableAcrossNodes: putil.GetOrZero(replicaConfig.Scheduler.EnableTableAcrossNodes),
	}, changefeedID, commonType.DefaultKeyspaceID)
	if err != nil {
		if db != nil {
			db.Close()
		}
		client.Close()
		return nil, err
	}

	log.Info("Kafka consumer initialized", zap.String("topic", topic), zap.Int("partitionCount", len(partitions)))
	writer := &eventWriter{
		downstream:    downstream,
		protocol:      protocol,
		partitions:    partitions,
		ddls:          make(map[ddlKey]*pendingDDL),
		mutations:     make(map[mutationKey]*mutation),
		bufferedInput: client.BufferedFetchBytes,
	}
	consumer := &kafkaConsumer{client: client, upstreamDB: db, topic: topic, writer: writer, offsets: make(map[*messageRecord]int64)}
	writer.confirm = consumer.commitCompleted
	return consumer, nil
}

func (c *kafkaConsumer) run(ctx, writeCtx context.Context) error {
	for {
		if err := c.writer.finishBatches(); err != nil {
			return err
		}
		if err := c.commitCompleted(ctx); err != nil {
			return err
		}
		if time.Since(c.writer.lastProgressLog) >= progressLogInterval {
			watermark, ready := c.globalWatermark()
			c.writer.lastProgressLog = time.Now()
			log.Info("consumer progress", zap.Uint64("watermark", watermark), zap.Bool("hasWatermark", ready),
				zap.Int64("receivedInputs", c.writer.receivedInputs), zap.Int64("decodedRows", c.writer.decodedRows),
				zap.Int64("writtenRows", c.writer.writtenRows), zap.Int64("completedInputs", c.writer.completedInputs),
				zap.Int("pendingDMLCount", len(c.writer.pendingDML)), zap.Int("pendingDDLCount", len(c.writer.pendingDDL)),
				zap.Int("inFlightBatches", len(c.writer.inFlight)), zap.Int64("inFlightBytes", c.writer.inFlightBytes),
				zap.Int("uncompletedInputs", c.writer.recordCount), zap.Int64("bufferedBytes", c.writer.bufferedBytes()))
		}
		// Poll periodically even without input to flush partial batches and
		// collect sink completions. No separate polling goroutine is needed.
		pollCtx, cancel := context.WithTimeout(ctx, batchLinger)
		fetches := c.client.PollRecords(pollCtx, 128)
		pollErr := pollCtx.Err()
		cancel()
		if err := context.Cause(ctx); err != nil {
			return err
		}
		if fetches.IsClientClosed() {
			return errors.ErrKafkaSinkClosed.GenWithStackByArgs()
		}
		for _, fetchError := range fetches.Errors() {
			if pollErr != nil && errors.Is(fetchError.Err, pollErr) {
				continue
			}
			return errors.WrapError(errors.ErrInternalCheckFailed, fetchError.Err, "read Kafka partition")
		}
		for iterator := fetches.RecordIter(); !iterator.Done(); {
			record := iterator.Next()
			c.writer.polledBytes += int64(len(record.Key) + len(record.Value) + 128)
		}
		if c.writer.bufferedBytes() > maxBufferedBytes {
			return errors.ErrInternalCheckFailed.FastGenByArgs("consumer fetched input exceeds its buffer limit; Kafka records remain uncommitted")
		}
		for iterator := fetches.RecordIter(); !iterator.Done(); {
			record := iterator.Next()
			c.writer.polledBytes -= int64(len(record.Key) + len(record.Value) + 128)
			if err := c.processRecord(ctx, record); err != nil {
				return err
			}
		}
		if err := c.writer.finishBatches(); err != nil {
			return err
		}
		if watermark, ready := c.globalWatermark(); ready {
			if err := c.writer.flushReady(ctx, writeCtx, watermark, fetches.Empty() || c.writer.bufferedBytes() >= memoryHighWater); err != nil {
				return err
			}
			c.writer.advanceReplay(watermark)
		}
		if err := c.commitCompleted(ctx); err != nil {
			return err
		}
		c.limitReads()
	}
}

func (c *kafkaConsumer) processRecord(ctx context.Context, record *kgo.Record) error {
	partition, ok := c.writer.partitions[record.Partition]
	if !ok {
		return errors.ErrInternalCheckFailed.FastGenByArgs("Kafka record belongs to an unknown partition")
	}
	bytes := int64(len(record.Key) + len(record.Value) + 128)
	if bytes > maxRecordBytes || c.writer.recordCount >= maxRecords || c.writer.bufferedBytes()+bytes > maxBufferedBytes {
		return errors.ErrInternalCheckFailed.FastGenByArgs("consumer input exceeds its buffer limit; Kafka record remains uncommitted")
	}
	state := &messageRecord{partition: record.Partition, bytes: bytes}
	c.offsets[state] = record.Offset
	c.writer.inputBytes += bytes
	c.writer.recordCount++
	c.writer.receivedInputs++
	partition.records = append(partition.records, state)
	partition.decoder.AddKeyValue(record.Key, record.Value)
	for {
		messageType, hasNext := partition.decoder.HasNext()
		if !hasNext {
			break
		}
		switch messageType {
		case codeccommon.MessageTypeRow:
			message := partition.decoder.NextDMLMessage()
			if message == nil {
				if _, ok := partition.decoder.(*simple.Decoder); !ok {
					return errors.ErrCodecDecode.FastGenByArgs("decoder returned an empty DML message")
				}
				state.remaining++
				c.writer.effectCount++
				partition.cachedUnreleased++
				partition.cachedRecords = append(partition.cachedRecords, state)
				if c.writer.effectCount > maxEffects {
					return errors.ErrInternalCheckFailed.FastGenByArgs("consumer cached DML exceeds its count limit; Kafka record remains uncommitted")
				}
				continue
			}
			state.remaining++
			c.writer.effectCount++
			if err := c.writer.queueDML(message, state, nil); err != nil {
				return err
			}
		case codeccommon.MessageTypeDDL:
			if c.writer.protocol == config.ProtocolCanalJSON && record.Partition != 0 {
				return errors.ErrCodecDecode.FastGenByArgs("Canal JSON DDL must come from partition 0")
			}
			ddl := partition.decoder.NextDDLEvent()
			if ddl == nil {
				return errors.ErrCodecDecode.FastGenByArgs("decoder returned an empty DDL event")
			}
			if err := c.writer.trackSchema(partition, ddl.TableInfo); err != nil {
				return err
			}
			for _, info := range ddl.MultipleTableInfos {
				if err := c.writer.trackSchema(partition, info); err != nil {
					return err
				}
			}
			if c.writer.protocol == config.ProtocolCanalJSON {
				// Canal retains DDL timestamps separately from its TableInfo cache.
				key := schemaKey{schema: ddl.SchemaName, table: ddl.TableName, version: ddl.FinishedTs}
				if _, known := partition.schemas[key]; !known {
					if c.writer.schemaCount >= maxSchemas || c.writer.schemaBytes+128 > maxSchemaBytes {
						return errors.ErrInternalCheckFailed.FastGenByArgs("consumer schema cache exceeds its resource limit; Kafka record remains uncommitted")
					}
					partition.schemas[key] = true
					c.writer.schemaBytes += 128
					c.writer.schemaCount++
				}
			}
			if simpleDecoder, ok := partition.decoder.(*simple.Decoder); ok {
				for _, message := range simpleDecoder.GetCachedMessages() {
					if partition.cachedUnreleased == 0 {
						return errors.ErrInternalCheckFailed.FastGenByArgs("Simple Protocol released a DML message without its Kafka record")
					}
					partition.cachedUnreleased--
					partition.cachedUnflushed++
					if err := c.writer.queueDML(message, nil, partition); err != nil {
						return err
					}
				}
			}
			if ddl.Query == "" {
				if c.writer.protocol != config.ProtocolSimple {
					return errors.ErrCodecDecode.FastGenByArgs("DDL query is empty")
				}
				continue
			}
			if err := c.writer.queueDDL(ddl, state, record.Partition, record.Partition == 0); err != nil {
				return err
			}
		case codeccommon.MessageTypeResolved:
			watermark := partition.decoder.NextResolvedEvent()
			if !partition.hasWatermark || watermark > partition.watermark {
				partition.watermark = watermark
				partition.hasWatermark = true
			}
		default:
			return errors.ErrCodecDecode.FastGenByArgs("decoder returned an unknown message type")
		}
		if c.writer.effectCount > maxEffects || c.writer.bufferedBytes() > maxBufferedBytes {
			return errors.ErrInternalCheckFailed.FastGenByArgs("consumer decoded input exceeds its buffer limit; Kafka record remains uncommitted")
		}
	}
	if state.remaining == 0 {
		state.complete = true
	}
	return nil
}

func (c *kafkaConsumer) globalWatermark() (uint64, bool) {
	watermark := uint64(math.MaxUint64)
	for _, partition := range c.writer.partitions {
		if !partition.hasWatermark || partition.cachedUnreleased != 0 {
			return 0, false
		}
		watermark = min(watermark, partition.watermark)
	}
	return watermark, true
}

func (c *kafkaConsumer) commitCompleted(ctx context.Context) error {
	records := make([]*kgo.Record, 0, len(c.writer.partitions))
	counts := make(map[int32]int, len(c.writer.partitions))
	for partitionID, partition := range c.writer.partitions {
		for index, record := range partition.records {
			if !record.complete {
				break
			}
			counts[partitionID] = index + 1
			if index+1 == len(partition.records) || !partition.records[index+1].complete {
				records = append(records, &kgo.Record{Topic: c.topic, Partition: record.partition, Offset: c.offsets[record]})
			}
		}
	}
	if len(records) == 0 {
		return nil
	}
	if err := c.client.CommitRecords(ctx, records...); err != nil {
		return errors.WrapError(errors.ErrInternalCheckFailed, err, "commit Kafka offsets")
	}
	for partitionID, count := range counts {
		partition := c.writer.partitions[partitionID]
		for _, record := range partition.records[:count] {
			c.writer.inputBytes -= record.bytes
			delete(c.offsets, record)
		}
		c.writer.recordCount -= count
		c.writer.completedInputs += int64(count)
		copy(partition.records, partition.records[count:])
		clear(partition.records[len(partition.records)-count:])
		partition.records = partition.records[:len(partition.records)-count]
	}
	return nil
}

func (c *kafkaConsumer) limitReads() {
	var slowest uint64 = math.MaxUint64
	for _, partition := range c.writer.partitions {
		if !partition.hasWatermark {
			slowest = 0
			break
		}
		slowest = min(slowest, partition.watermark)
	}
	for partitionID, partition := range c.writer.partitions {
		// Leave the remaining 32 MiB available for the partitions that can
		// advance the boundary, including Simple schema bootstrap messages.
		pause := c.writer.bufferedBytes() >= memoryHighWater && partition.hasWatermark && partition.watermark > slowest && partition.cachedUnreleased == 0
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
