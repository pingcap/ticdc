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
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

type kafkaReader struct {
	protocol        config.Protocol
	router          *eventrouter.EventRouter
	maxMessageBytes int
	client          *kgo.Client
	topic           string
	partitions      int32
}

func newKafkaReader(ctx context.Context, opts Options) *kafkaReader {
	upstreamURI := opts.UpstreamURI
	replicaConfig := opts.ReplicaConfig
	codecConfig := opts.CodecConfig
	topic := strings.Trim(upstreamURI.Path, "/")
	if strings.Contains(topic, ",") {
		log.Panic("consumer accepts one Kafka topic", zap.String("topic", topic))
	}
	isAvroLike := codecConfig.Protocol == config.ProtocolAvro || codecConfig.Protocol == config.ProtocolDebeziumAvro
	router, err := eventrouter.NewEventRouter(replicaConfig.Sink, util.GetOrZero(replicaConfig.CaseSensitive), topic, false, isAvroLike)
	if err != nil {
		log.Panic("kafka event routing configuration is invalid", zap.Error(err))
	}
	kafkaOptions := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(upstreamURI.Host, ",")...),
		kgo.ConsumerGroup(opts.ConsumerID),
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
	maxMessageBytes := math.MaxInt
	if query.Has("max-message-bytes") {
		maxMessageBytes = codecConfig.MaxMessageBytes
	}
	ca, cert, key := query.Get("ca"), query.Get("cert"), query.Get("key")
	if upstreamURI.Scheme == config.KafkaSSLScheme || cmp.Or(ca, cert, key) != "" {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
		if ca != "" {
			pem, err := os.ReadFile(ca)
			if err != nil {
				log.Panic("kafka CA certificate loading failed", zap.Error(errors.WrapError(errors.ErrKafkaInvalidConfig, err)))
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				log.Panic("kafka CA certificate is invalid", zap.Error(errors.ErrKafkaInvalidConfig.GenWithStack("no certificate found in %s", ca)))
			}
			tlsConfig.RootCAs = pool
		}
		if cert != "" || key != "" {
			certificate, err := tls.LoadX509KeyPair(cert, key)
			if err != nil {
				log.Panic("kafka client certificate loading failed", zap.Error(errors.WrapError(errors.ErrKafkaInvalidConfig, err)))
			}
			tlsConfig.Certificates = []tls.Certificate{
				certificate,
			}
		}
		kafkaOptions = append(kafkaOptions, kgo.DialTLSConfig(tlsConfig))
	}
	client, err := kgo.NewClient(kafkaOptions...)
	if err != nil {
		log.Panic("kafka client initialization failed", zap.Error(err))
	}
	metadata, err := kadm.NewClient(client).Metadata(ctx, topic)
	if err != nil {
		log.Panic("kafka topic metadata lookup failed", zap.String("topic", topic), zap.Error(err))
	}
	topicMetadata, ok := metadata.Topics[topic]
	if !ok || topicMetadata.Err != nil || len(topicMetadata.Partitions) == 0 {
		log.Panic("kafka topic metadata is unavailable", zap.String("topic", topic), zap.Error(topicMetadata.Err), zap.Int("partitionCount", len(topicMetadata.Partitions)))
	}

	partitions := int32(len(topicMetadata.Partitions))
	return &kafkaReader{
		protocol:        codecConfig.Protocol,
		router:          router,
		maxMessageBytes: maxMessageBytes,
		client:          client,
		topic:           topic,
		partitions:      partitions,
	}
}

func (c *kafkaReader) Partitions() int32 {
	return c.partitions
}

func (c *kafkaReader) Read(ctx context.Context) ([]*Payload, error) {
	select {
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	default:
	}
	fetches := c.client.PollRecords(ctx, readBatchRows)
	if err := fetches.Err(); err != nil {
		log.Error("read fetches failed", zap.Error(err))
		return nil, err
	}

	var batch []*Payload
	for iterator := fetches.RecordIter(); !iterator.Done(); {
		record := iterator.Next()
		if size := len(record.Key) + len(record.Value); size > c.maxMessageBytes {
			log.Panic("kafka message size limit exceeded", zap.String("topic", c.topic),
				zap.Int32("partition", record.Partition), zap.Int64("offset", record.Offset),
				zap.Int("maxMessageBytes", c.maxMessageBytes), zap.Int("recordBytes", size))
		}
		// CommitRecords reads only these fields. Copying them keeps the fetch
		// buffer, which Key and Value alias, out of the acknowledgement closure.
		position := kgo.Record{
			Topic:       c.topic,
			Partition:   record.Partition,
			Offset:      record.Offset,
			LeaderEpoch: record.LeaderEpoch,
		}
		batch = append(batch, &Payload{
			Partition: record.Partition,
			Key:       record.Key,
			Value:     record.Value,
			Offset:    record.Offset,
			Ack: func(ctx context.Context) error {
				if err := c.client.CommitRecords(ctx, &position); err != nil {
					return errors.WrapError(errors.ErrInternalCheckFailed, err, "commit Kafka offsets")
				}
				return nil
			},
		})
	}
	return batch, nil
}

func (c *kafkaReader) Close() {
	c.client.Close()
}

func (c *kafkaReader) CheckDML(dml *event.DMLEvent, part int32, offset int64) {
	skipKeyOnlyDelete := c.protocol == config.ProtocolAvro && (len(dml.RowTypes) == 2 && dml.RowTypes[0] == common.RowTypeDelete && dml.RowTypes[1] == common.RowTypeInsert)
	generator := c.router.GetPartitionGenerator(dml.TableInfo.GetSchemaName(), dml.TableInfo.GetTableName())
	partitionCount := c.partitions
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
			log.Panic("kafka DML partition calculation failed", zap.String("topic", c.topic), zap.Int32("partition", part), zap.Int64("offset", offset), zap.String("schema", dml.TableInfo.GetSchemaName()), zap.String("table", dml.TableInfo.GetTableName()), zap.Error(err))
		}
		if expected != part {
			log.Panic("kafka DML was dispatched to the wrong partition", zap.String("topic", c.topic), zap.Int32("partition", part), zap.Int32("expectedPartition", expected), zap.Int32("partitionCount", partitionCount), zap.Int64("offset", offset), zap.String("schema", dml.TableInfo.GetSchemaName()), zap.String("table", dml.TableInfo.GetTableName()), zap.Uint64("commitTs", dml.CommitTs))
		}
	}
}
