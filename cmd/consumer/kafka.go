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
	"cmp"
	"context"
	"crypto/tls"
	"crypto/x509"
	"iter"
	"math"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter"
	routing "github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter/partition"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	putil "github.com/pingcap/ticdc/pkg/util"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

type kafkaRecord struct {
	through uint64
	offset  int64
}

const kafkaRecordBytes = int64(unsafe.Sizeof(kafkaRecord{}))

type kafkaPartition struct {
	watermark    uint64
	hasWatermark bool
	polled       []*kgo.Record
	fetchPaused  bool
	sequence     uint64
	targetTs     uint64 // Event progress still needed for the current boundary; zero means complete.
	records      []kafkaRecord
	parsedTs     uint64     // Parser-owned maximum event timestamp in the parsed prefix.
	mu           sync.Mutex // Protects parsed positions, shared by the parser and Confirm.
}

type kafkaReader struct {
	*readLimit
	router           *eventrouter.EventRouter
	client           *kgo.Client
	topic            string
	memory           *memoryUsage
	prefetchedBytes  int64 // SDK bytes already included in the shared budget.
	confirmable      chan<- struct{}
	partitions       map[int32]*kafkaPartition
	readWatermarks   map[int32]uint64 // Parser-owned partition coverage, before conductor publication.
	watermark        uint64
	readSequence     uint64
	pausedPartitions int
	boundary         *readBoundary
	control          *readBoundary
	progressMu       sync.Mutex // Protects event progress and scheduling shared with the conductor.
}

func newKafkaReader(ctx context.Context, upstreamURI *url.URL, consumerID string, memory *memoryUsage, replicaConfig *config.ReplicaConfig, confirmable chan<- struct{}) (reader *kafkaReader) {
	if upstreamURI.Host == "" {
		log.Panic("Kafka upstream URI must include an endpoint")
	}
	topic := strings.Trim(upstreamURI.Path, "/")
	if topic == "" {
		log.Panic("Kafka upstream URI must include a topic")
	}
	if strings.Contains(topic, ",") {
		log.Panic("consumer accepts one Kafka topic", zap.String("topic", topic))
	}
	protocol, err := config.ParseSinkProtocolFromString(cmp.Or(upstreamURI.Query().Get(config.ProtocolKey), "open-protocol"))
	if err != nil {
		log.Panic("Kafka protocol is invalid", zap.Error(err))
	}
	router, err := eventrouter.NewEventRouter(replicaConfig.Sink, putil.GetOrZero(replicaConfig.CaseSensitive), topic, false, protocol == config.ProtocolAvro)
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
	ca, cert, key := query.Get("ca"), query.Get("cert"), query.Get("key")
	if upstreamURI.Scheme == config.KafkaSSLScheme || ca != "" || cert != "" || key != "" {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
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
			tlsConfig.Certificates = []tls.Certificate{certificate}
		}
		kafkaOptions = append(kafkaOptions, kgo.DialTLSConfig(tlsConfig))
	}
	client, err := kgo.NewClient(kafkaOptions...)
	if err != nil {
		log.Panic("Kafka client initialization failed", zap.Error(err))
	}
	defer func() {
		if reader == nil {
			client.Close()
		}
	}()

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

	log.Info("Kafka reader initialized", zap.String("topic", topic), zap.Int("partitionCount", len(partitions)))
	return &kafkaReader{
		readLimit:   newReadLimit(memory),
		client:      client,
		topic:       topic,
		memory:      memory,
		confirmable: confirmable,
		partitions:  partitions,
		router:      router,
		control: &readBoundary{
			reached:   helper.GetDDLDispatchRule(protocol) == helper.PartitionAll,
			watermark: &partitions[0].watermark,
		},
	}
}

type kafkaInput struct {
	message *kgo.Record
}

func (c *kafkaReader) Run(ctx context.Context, parser *inputParser, tasks chan<- []task) error {
	return readAndParse(ctx, c.Read, func(ctx context.Context, input kafkaInput) iter.Seq2[task, error] {
		return c.parse(ctx, parser, input)
	}, tasks)
}

func (c *kafkaReader) parse(ctx context.Context, parser *inputParser, input kafkaInput) iter.Seq2[task, error] {
	id := input.message.Partition
	offset := input.message.Offset
	scope := inputScope{stream: id, ddlCopy: id != 0, dmlBoundary: c.control}
	if id == 0 {
		scope.dmlBoundary = &readBoundary{reached: true, watermark: c.control.watermark}
	}
	if len(c.partitions) == 1 {
		scope.ddlBoundary = &readBoundary{reached: true}
	}
	return func(yield func(task, error) bool) {
		defer c.memory.release(int64(len(input.message.Key) + len(input.message.Value)))
		partition := c.partitions[id]
		pending := make([]task, 0, 1)
		rows := 0
		for result, err := range parser.parseMessage(ctx, scope, input.message.Key, input.message.Value) {
			if err != nil {
				yield(nil, err)
				return
			}
			through := taskTimestamp(result)
			partition.parsedTs = max(partition.parsedTs, through)
			if progress, ok := result.(*inputWatermark); ok {
				if err := c.checkWatermark(ctx, id, progress.watermark); err != nil {
					yield(nil, err)
					return
				}
			}
			if task, ok := result.(*decodeTask); ok {
				rows++
				if rows > parser.codecConfig.MaxBatchSize {
					log.Panic("Kafka message batch size limit exceeded", zap.String("topic", c.topic), zap.Int32("partition", id), zap.Int64("offset", input.message.Offset), zap.Int("maxBatchSize", parser.codecConfig.MaxBatchSize), zap.Int("actualBatchSize", rows))
				}
				if size := len(input.message.Key) + len(input.message.Value); rows > 1 && size > parser.codecConfig.MaxMessageBytes {
					log.Panic("Kafka message byte size limit exceeded", zap.String("topic", c.topic), zap.Int32("partition", id), zap.Int64("offset", input.message.Offset), zap.Int("maxMessageBytes", parser.codecConfig.MaxMessageBytes), zap.Int("receivedBytes", size))
				}
				message := task.message
				skipKeyOnlyDelete := parser.codecConfig.Protocol == config.ProtocolAvro && message.RowType == common.RowTypeUpdate
				task.message = codecCommon.NewDMLMessage(message.TableID, message.Schema, message.Table, message.GetCommitTs(), message.RowType, func() *event.DMLEvent {
					dml := message.ToDMLEvent()
					c.checkPartition(dml, id, offset, skipKeyOnlyDelete)
					return dml
				})
				task.message.ApproximateSize = message.ApproximateSize
			}
			if rows == 0 {
				if !yield(result, nil) {
					return
				}
				continue
			}
			// Validate the whole DML message before any of its rows reach the conductor.
			pending = append(pending, result)
		}
		for _, result := range pending {
			if !yield(result, nil) {
				return
			}
		}
		partition.mu.Lock()
		c.memory.reserve(kafkaRecordBytes)
		partition.records = append(partition.records, kafkaRecord{through: partition.parsedTs, offset: offset})
		partition.mu.Unlock()
		notifyConfirmation(c.confirmable)
	}
}

// Validate the complete topic scope before a new global read watermark becomes
// visible. Partition growth requires rebuilding routing and decoder state.
func (c *kafkaReader) checkWatermark(ctx context.Context, partition int32, through uint64) error {
	if c.readWatermarks == nil {
		c.readWatermarks = make(map[int32]uint64, len(c.partitions))
	}
	c.readWatermarks[partition] = max(c.readWatermarks[partition], through)
	for id := range c.partitions {
		through = min(through, c.readWatermarks[id])
	}
	if through <= c.watermark {
		return nil
	}
	metadata, err := kadm.NewClient(c.client).Metadata(ctx, c.topic)
	if err != nil {
		return errors.WrapError(errors.ErrKafkaInvalidConfig, err, "refresh Kafka partitions")
	}
	topic, ok := metadata.Topics[c.topic]
	if !ok {
		return errors.ErrKafkaInvalidConfig.GenWithStackByArgs("Kafka topic metadata is unavailable")
	}
	if topic.Err != nil {
		return errors.WrapError(errors.ErrKafkaInvalidConfig, topic.Err, "refresh Kafka topic metadata")
	}
	if len(topic.Partitions) != len(c.partitions) {
		return errors.ErrKafkaInvalidConfig.GenWithStackByArgs("Kafka partitions changed; restart the consumer")
	}
	for id := range topic.Partitions {
		if c.partitions[id] == nil {
			return errors.ErrKafkaInvalidConfig.GenWithStackByArgs("Kafka partitions changed; restart the consumer")
		}
	}
	c.watermark = through
	return nil
}

func (c *kafkaReader) checkPartition(dml *event.DMLEvent, partition int32, offset int64, skipKeyOnlyDelete bool) {
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
			log.Panic("Kafka DML partition calculation failed", zap.String("topic", c.topic), zap.Int32("partition", partition), zap.Int64("offset", offset), zap.String("schema", dml.TableInfo.GetSchemaName()), zap.String("table", dml.TableInfo.GetTableName()), zap.Error(err))
		}
		if expected != partition {
			log.Panic("Kafka DML was dispatched to the wrong partition", zap.String("topic", c.topic), zap.Int32("partition", partition), zap.Int32("expectedPartition", expected), zap.Int32("partitionCount", partitionCount), zap.Int64("offset", offset), zap.String("schema", dml.TableInfo.GetSchemaName()), zap.String("table", dml.TableInfo.GetTableName()), zap.Uint64("commitTs", dml.CommitTs))
		}
	}
}

func (c *kafkaReader) Read(ctx context.Context) ([]kafkaInput, error) {
	for {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		prefetched := c.client.BufferedFetchBytes()
		if prefetched > c.prefetchedBytes {
			c.memory.reserve(prefetched - c.prefetchedBytes)
		}
		if prefetched < c.prefetchedBytes {
			c.memory.release(c.prefetchedBytes - prefetched)
		}
		c.prefetchedBytes = prefetched
		c.limitReads()
		var (
			batch []kafkaInput
			size  int64
		)
		for len(batch) < batchRows && size < batchBytes {
			partitionID := c.nextPartition()
			if partitionID < 0 {
				break
			}
			partition := c.partitions[partitionID]
			queue := partition.polled
			record := queue[0]
			queue[0] = nil
			partition.polled = queue[1:]
			if len(partition.polled) == 0 {
				partition.polled = nil
			}
			c.readSequence++
			partition.sequence = c.readSequence
			bytes := int64(len(record.Key) + len(record.Value))
			batch = append(batch, kafkaInput{message: record})
			size += bytes
		}
		if len(batch) != 0 {
			return batch, nil
		}
		// Drain SDK buffers before waiting; those bytes are already admitted.
		// Buffered leading partitions may need a fetch from a slower one.
		buffered := c.client.BufferedFetchRecords() != 0
		for _, partition := range c.partitions {
			buffered = buffered || len(partition.polled) != 0
		}
		if !buffered {
			if err := c.wait(ctx); err != nil {
				return nil, err
			}
		}
		fetches := c.client.PollRecords(ctx, batchRows)
		if fetches.IsClientClosed() {
			return nil, errors.ErrKafkaSinkClosed.GenWithStackByArgs()
		}
		for _, fetchError := range fetches.Errors() {
			return nil, errors.WrapError(errors.ErrInternalCheckFailed, fetchError.Err, "read Kafka partition")
		}
		var bytes int64
		for iterator := fetches.RecordIter(); !iterator.Done(); {
			record := iterator.Next()
			partition := c.partitions[record.Partition]
			if partition == nil {
				return nil, errors.ErrKafkaInvalidConfig.GenWithStackByArgs("Kafka partitions changed; restart the consumer")
			}
			partition.polled = append(partition.polled, record)
			bytes += int64(len(record.Key) + len(record.Value))
		}
		c.memory.reserve(bytes)
	}
}

func (c *kafkaReader) Advance(updates []readUpdate) {
	c.progressMu.Lock()
	for _, update := range updates {
		partition := c.partitions[update.stream]
		if update.watermark != 0 {
			partition.watermark = max(partition.watermark, update.watermark)
			partition.hasWatermark = true
		}
		if update.boundary != nil && update.boundary == c.boundary {
			partition.targetTs = 0
		}
	}
	c.advanceBoundary()
	c.progressMu.Unlock()
	// Fetch can be blocked while every partition is paused. Publish admission
	// changes immediately instead of waiting for another PollRecords return.
	c.limitReads()
}

func (c *kafkaReader) ContinueReading(enabled bool) {
	c.readLimit.ContinueReading(enabled)
	c.limitReads()
}

// A nil selection covers every partition. The conductor supplies logical table
// names; the reader translates them using its transport's routing rules.
func (c *kafkaReader) tablePartitions(names []event.SchemaTableName) []int32 {
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
		case *routing.TablePartitionGenerator:
		default:
			return nil
		}
		table := common.NewTableInfo4Decoder(name.SchemaName, &timodel.TableInfo{
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

// Routing selects the inputs whose decoded events must cover commitTs. A matching
// scoped event can complete an input through Advance; watermarks cover every scope.
func (c *kafkaReader) Boundary(tables []event.SchemaTableName, commitTs uint64) *readBoundary {
	partitionIDs := c.tablePartitions(tables)
	if tables != nil && len(tables) == 0 {
		partitionIDs = []int32{0}
	}
	c.progressMu.Lock()
	defer c.progressMu.Unlock()
	for partitionID, partition := range c.partitions {
		partition.targetTs = 0
		if (partitionIDs == nil || slices.Contains(partitionIDs, partitionID)) && partition.watermark < commitTs {
			partition.targetTs = commitTs
		}
	}
	if commitTs == 0 {
		c.boundary = nil
		return nil
	}
	c.boundary = &readBoundary{}
	c.advanceBoundary()
	return c.boundary
}

// Called by the conductor while holding progressMu. Only the conductor mutates reached.
func (c *kafkaReader) advanceBoundary() {
	if c.boundary == nil || c.boundary.reached {
		return
	}
	reached := true
	for _, partition := range c.partitions {
		if partition.watermark >= partition.targetTs {
			partition.targetTs = 0
		}
		if partition.targetTs != 0 {
			reached = false
		}
	}
	c.boundary.reached = reached
}

func (c *kafkaReader) Confirm(ctx context.Context, checkpoint uint64) error {
	if checkpoint == 0 {
		return nil
	}
	records := make([]*kgo.Record, 0, len(c.partitions))
	counts := make(map[*kafkaPartition]int, len(c.partitions))
	for partitionID, partition := range c.partitions {
		partition.mu.Lock()
		inputs := partition.records
		var count int
		for _, record := range inputs {
			if record.through > checkpoint {
				break
			}
			count++
		}
		if count != 0 {
			counts[partition] = count
			records = append(records, &kgo.Record{
				Topic:     c.topic,
				Partition: partitionID,
				Offset:    inputs[count-1].offset,
			})
		}
		partition.mu.Unlock()
	}
	if len(records) == 0 {
		return nil
	}
	if err := c.client.CommitRecords(ctx, records...); err != nil {
		return errors.WrapError(errors.ErrInternalCheckFailed, err, "commit Kafka offsets")
	}
	// Read may have appended inputs while the commit was in flight.
	for partition, count := range counts {
		partition.mu.Lock()
		inputs := partition.records
		clear(inputs[:count])
		partition.records = inputs[count:]
		c.memory.release(int64(count) * kafkaRecordBytes)
		partition.mu.Unlock()
	}
	return nil
}

func (c *kafkaReader) limitReads() {
	c.progressMu.Lock()
	defer c.progressMu.Unlock()
	boundaryPending := c.boundary != nil && !c.boundary.reached
	underPressure := c.memory.used() >= c.memory.limit
	if !boundaryPending && !underPressure && c.pausedPartitions == 0 {
		return
	}
	slowest := uint64(math.MaxUint64)
	if underPressure {
		for _, partition := range c.partitions {
			if !partition.hasWatermark {
				slowest = 0
				break
			}
			slowest = min(slowest, partition.watermark)
		}
	}
	for partitionID, partition := range c.partitions {
		pause := false
		if boundaryPending {
			pause = underPressure && partition.targetTs == 0
		}
		if underPressure && !boundaryPending {
			// Retained controls still need slow partitions to advance before
			// they can release memory. Do not fetch ahead of that progress.
			pause = partition.hasWatermark && partition.watermark > slowest && !c.continuing.Load()
		}
		if pause == partition.fetchPaused {
			continue
		}
		partitions := map[string][]int32{c.topic: {partitionID}}
		switch {
		case pause:
			c.client.PauseFetchPartitions(partitions)
			c.pausedPartitions++
		default:
			c.client.ResumeFetchPartitions(partitions)
			c.pausedPartitions--
		}
		partition.fetchPaused = pause
	}
}

func (c *kafkaReader) nextPartition() int32 {
	c.progressMu.Lock()
	defer c.progressMu.Unlock()
	selected := int32(-1)
	var progress, sequence uint64
	boundaryPending := c.boundary != nil && !c.boundary.reached
	underPressure := c.memory.used() >= c.memory.limit
	for partitionID, partition := range c.partitions {
		if len(partition.polled) == 0 {
			continue
		}
		if boundaryPending && underPressure && partition.targetTs == 0 {
			continue
		}
		watermark := partition.watermark
		if !partition.hasWatermark {
			watermark = 0
		}
		if c.continuing.Load() {
			// Missing dependencies can be satisfied by any input. Fair rotation
			// lets a nearby control pass a backlog in another partition.
			watermark = 0
		}
		needed := boundaryPending && partition.targetTs != 0
		selectedNeeded := selected >= 0 && boundaryPending && c.partitions[selected].targetTs != 0
		// A busy dependency must still let other buffered streams make progress.
		// Under pressure, admission remains restricted to the unfinished scope.
		overdue := !underPressure && c.readSequence-partition.sequence >= batchRows
		selectedOverdue := selected >= 0 && !underPressure && c.readSequence-c.partitions[selected].sequence >= batchRows
		if selectedOverdue && !overdue {
			continue
		}
		if overdue && !selectedOverdue {
			selected, progress, sequence = partitionID, watermark, partition.sequence
			continue
		}
		if selectedNeeded && !needed {
			continue
		}
		earlier := watermark < progress || (watermark == progress && (partition.sequence < sequence || (partition.sequence == sequence && partitionID < selected)))
		if selected < 0 || (needed && !selectedNeeded) || earlier {
			selected, progress, sequence = partitionID, watermark, partition.sequence
		}
	}
	return selected
}

func (c *kafkaReader) Close() {
	c.client.Close()
	c.memory.release(c.prefetchedBytes)
	for _, partition := range c.partitions {
		c.memory.release(int64(len(partition.records)) * kafkaRecordBytes)
		partition.records = nil
		for _, record := range partition.polled {
			c.memory.release(int64(len(record.Key) + len(record.Value)))
		}
	}
}
