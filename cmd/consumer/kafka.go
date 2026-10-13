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
	"math"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter"
	routing "github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter/partition"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	putil "github.com/pingcap/ticdc/pkg/util"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

type kafkaRecord struct {
	ack    *ack
	offset int64
}

type kafkaPartition struct {
	watermark    uint64
	hasWatermark bool
	polled       []*kgo.Record
	fetchPaused  bool
	sequence     uint64
	position     int64 // Next offset after the last fully decoded input.
	target       int64 // -1 means this partition is outside the current boundary.
	records      []kafkaRecord
	mu           sync.Mutex // Protects records, shared by Read and Confirm.
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
	readSequence     uint64
	pausedPartitions int
	boundary         *readBoundary
	progressMu       sync.Mutex // Protects cursors and scheduling progress shared with assembly.
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
	if upstreamURI.Scheme == config.KafkaSSLScheme {
		kafkaOptions = append(kafkaOptions, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
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

	committed, err := admin.FetchOffsetsForTopics(ctx, consumerID, topic)
	if err != nil && !errors.Is(err, kerr.GroupIDNotFound) {
		log.Panic("Kafka consumer offsets lookup failed", zap.String("topic", topic), zap.String("consumerID", consumerID), zap.Error(err))
	}
	starts, err := admin.ListStartOffsets(ctx, topic)
	if err == nil {
		err = starts.Error()
	}
	if err != nil {
		log.Panic("Kafka start offsets lookup failed", zap.String("topic", topic), zap.Error(err))
	}
	ends, err := admin.ListEndOffsets(ctx, topic)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		log.Panic("Kafka end offsets lookup failed", zap.String("topic", topic), zap.Error(err))
	}
	partitions := make(map[int32]*kafkaPartition, len(topicMetadata.Partitions))
	for partitionID := range topicMetadata.Partitions {
		start := starts[topic][partitionID]
		end := ends[topic][partitionID]
		position := committed[topic][partitionID].At
		if position < start.Offset || position > end.Offset {
			position = start.Offset
		}
		partitions[partitionID] = &kafkaPartition{
			position: position,
			target:   -1,
		}
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
	}
}

func (c *kafkaReader) Read(ctx context.Context) ([]*readData, error) {
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
			batch []*readData
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
			bytes := int64(len(record.Key) + len(record.Value) + 128)
			c.memory.release(bytes)
			state := newAck(c.memory, bytes, c.confirmable)
			partition.mu.Lock()
			partition.records = append(partition.records, kafkaRecord{
				ack:    state,
				offset: record.Offset,
			})
			partition.mu.Unlock()
			data := &readData{
				key:           record.Key,
				value:         record.Value,
				stream:        partitionID,
				position:      record.Offset + 1,
				ddlCopy:       partitionID != 0,
				record:        state,
				retainedBytes: 128,
			}
			if partitionID == 0 {
				data.dmlBoundary = &readBoundary{reached: true}
			}
			if len(c.partitions) == 1 {
				data.ddlBoundary = &readBoundary{reached: true}
			}
			batch = append(batch, data)
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
			partition.polled = append(partition.polled, record)
			bytes += int64(len(record.Key) + len(record.Value) + 128)
		}
		c.memory.reserve(bytes)
	}
}

func (c *kafkaReader) Advance(updates []readUpdate) {
	c.progressMu.Lock()
	for _, update := range updates {
		data := update.data
		partition := c.partitions[data.stream]
		if update.decoded {
			partition.position = max(partition.position, data.position)
		}
		if update.progress != 0 {
			partition.watermark = max(partition.watermark, update.progress)
			partition.hasWatermark = true
		}
		if update.boundary != nil && update.boundary == c.boundary && partition.target >= 0 {
			partition.target = min(partition.target, data.position)
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

// A nil selection covers every partition. The assembler supplies logical table
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
	if !slices.Contains(partitions, int32(0)) {
		partitions = append(partitions, 0)
	}
	return partitions
}

// Capture a fixed transport window. Already decoded control positions can
// subsequently shorten it through Advance without passing DDLs to this reader.
func (c *kafkaReader) Boundary(ctx context.Context, tables []event.SchemaTableName) (*readBoundary, error) {
	if len(c.partitions) == 1 {
		return &readBoundary{
			reached: true,
		}, nil
	}
	partitionIDs := c.tablePartitions(tables)
	if tables != nil && len(tables) == 0 {
		partitionIDs = []int32{0}
	}
	req := kmsg.NewPtrListOffsetsRequest()
	req.TimeoutMillis = 10000
	topic := kmsg.NewListOffsetsRequestTopic()
	topic.Topic = c.topic
	for partitionID := range c.partitions {
		if partitionIDs != nil && !slices.Contains(partitionIDs, partitionID) {
			continue
		}
		partition := kmsg.NewListOffsetsRequestTopicPartition()
		partition.Partition, partition.Timestamp = partitionID, -1
		topic.Partitions = append(topic.Partitions, partition)
	}
	req.Topics = append(req.Topics, topic)
	targets := make(map[int32]int64, len(topic.Partitions))
	for _, shard := range c.client.RequestSharded(ctx, req) {
		if shard.Err != nil {
			return nil, errors.WrapError(errors.ErrKafkaAdminAPI, shard.Err, "get input boundary", c.topic)
		}
		for _, topic := range shard.Resp.(*kmsg.ListOffsetsResponse).Topics {
			for _, partition := range topic.Partitions {
				if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
					return nil, errors.WrapError(errors.ErrKafkaAdminAPI, err, "get input boundary", c.topic)
				}
				targets[partition.Partition] = partition.Offset
			}
		}
	}
	c.progressMu.Lock()
	defer c.progressMu.Unlock()
	for partitionID, partition := range c.partitions {
		partition.target = -1
		if target, ok := targets[partitionID]; ok {
			partition.target = target
		}
	}
	c.boundary = &readBoundary{}
	c.advanceBoundary()
	return c.boundary, nil
}

// Called by assembly while holding progressMu. Only assembly mutates reached.
func (c *kafkaReader) advanceBoundary() {
	if c.boundary == nil || c.boundary.reached {
		return
	}
	for _, partition := range c.partitions {
		if partition.target >= 0 && partition.position < partition.target {
			return
		}
	}
	c.boundary.reached = true
	for _, partition := range c.partitions {
		partition.target = -1
	}
}

func (c *kafkaReader) Confirm(ctx context.Context) error {
	records := make([]*kgo.Record, 0, len(c.partitions))
	counts := make(map[*kafkaPartition]int, len(c.partitions))
	for partitionID, partition := range c.partitions {
		partition.mu.Lock()
		inputs := partition.records
		var count int
		for _, record := range inputs {
			if record.ack.refs.Load() != 0 {
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
		for _, record := range inputs[:count] {
			record.ack.confirm()
		}
		clear(inputs[:count])
		partition.records = inputs[count:]
		partition.mu.Unlock()
	}
	return nil
}

func (c *kafkaReader) limitReads() {
	c.progressMu.Lock()
	defer c.progressMu.Unlock()
	boundaryPending := c.boundary != nil && !c.boundary.reached
	underPressure := !boundaryPending && c.memory.used() >= c.memory.limit
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
		// Fetch unfinished fixed boundaries first. Schema bootstrap messages
		// remain readable even beyond that boundary.
		pause := boundaryPending && partition.position >= partition.target && !c.continuing.Load()
		if underPressure {
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
	for partitionID, partition := range c.partitions {
		if len(partition.polled) == 0 {
			continue
		}
		// A fetch pause under memory pressure must not hold already fetched
		// inputs. Only an unfinished fixed boundary limits their delivery.
		if c.boundary != nil && !c.boundary.reached && !c.continuing.Load() &&
			(partition.position >= partition.target || partition.polled[0].Offset >= partition.target) {
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
		if c.boundary != nil && !c.boundary.reached && partition.position < partition.target {
			watermark = 0
		}
		if selected < 0 || watermark < progress || (watermark == progress && (partition.sequence < sequence || (partition.sequence == sequence && partitionID < selected))) {
			selected, progress, sequence = partitionID, watermark, partition.sequence
		}
	}
	return selected
}

func (c *kafkaReader) Close() {
	c.client.Close()
	c.memory.release(c.prefetchedBytes)
}
