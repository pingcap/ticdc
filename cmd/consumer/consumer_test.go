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
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/downstreamadapter/sink/mock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/open"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaReaderSmallMessageLimit(t *testing.T) {
	const topic = "small-message-limit"
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	t.Cleanup(cluster.Close)
	for _, query := range []string{
		"protocol=canal-json&enable-tidb-extension=true",
		"protocol=open-protocol",
		"protocol=simple",
		"protocol=simple&encoding-format=avro",
		"protocol=debezium&enable-tidb-extension=true",
	} {
		t.Run(query, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			upstreamURI := &url.URL{Scheme: "kafka", Host: cluster.ListenAddrs()[0], Path: topic, RawQuery: query + "&max-message-bytes=262144"}
			input, err := newKafkaReader(ctx, upstreamURI, "small-message-limit", "UTC", config.GetDefaultReplicaConfig(), &memoryUsage{})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, input.Close()) })
			require.Len(t, input.buffer.partitions, 1)
			require.NotNil(t, input.buffer.partitions[0].decoder)
			if !strings.HasPrefix(query, "protocol=canal-json") {
				return
			}
			// The topic limit can grow while this reader keeps the original URI.
			producer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.ProducerBatchMaxBytes(2<<20))
			require.NoError(t, err)
			t.Cleanup(producer.Close)
			payload := strings.Repeat("x", 1<<20)
			value := []byte(`{"database":"test","table":"t","pkNames":["id"],"isDdl":false,"type":"INSERT","sqlType":{"id":4,"v":12},"mysqlType":{"id":"int","v":"longtext"},"data":[{"id":"1","v":"` + payload + `"}],"_tidb":{"commitTs":10}}`)
			require.NoError(t, producer.ProduceSync(ctx,
				&kgo.Record{Topic: topic, Value: value},
				&kgo.Record{Topic: topic, Value: []byte(`{"isDdl":false,"type":"TIDB_WATERMARK","_tidb":{"watermarkTs":10}}`)},
			).FirstErr())
			result, err := input.Read(ctx)
			require.NoError(t, err)
			require.NotNil(t, result.dml)
			require.EqualValues(t, 10, result.dml.CommitTs)
			require.Equal(t, payload, result.dml.Rows.GetRow(0).GetString(1))
		})
	}
}

func TestStorageReaderSmallMessageLimit(t *testing.T) {
	for _, protocol := range []string{"csv", "canal-json"} {
		t.Run(protocol, func(t *testing.T) {
			replicaConfig := config.GetDefaultReplicaConfig()
			replicaConfig.Sink.Protocol = new(protocol)
			upstreamURI := &url.URL{Scheme: "file", Path: t.TempDir(), RawQuery: "max-message-bytes=262144"}
			input, err := newStorageReader(t.Context(), upstreamURI, "UTC", replicaConfig, &memoryUsage{})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, input.Close()) })
		})
	}
}

func TestKafkaReaderSplitRenameDDL(t *testing.T) {
	codecConfig := codecCommon.NewConfig(config.ProtocolOpen)
	encoder, err := open.NewBatchEncoder(codecConfig, nil)
	require.NoError(t, err)
	// A rename chain can carry the same final table name in different messages.
	ddls := []*event.DDLEvent{
		{FinishedTs: 10, SchemaName: "common", TableName: "test4", Type: byte(timodel.ActionRenameTable), Query: "RENAME TABLE `common_1`.`test1` TO `common`.`test2`;"},
		{FinishedTs: 10, SchemaName: "common_1", TableName: "test4", Type: byte(timodel.ActionRenameTable), Query: "RENAME TABLE `common`.`test2` TO `common_1`.`test3`;"},
		{FinishedTs: 10, SchemaName: "common_1", TableName: "test1", Type: byte(timodel.ActionRenameTable), Query: "RENAME TABLE `common`.`test4` TO `common_1`.`test1`;"},
		{FinishedTs: 10, SchemaName: "common", TableName: "test4", Type: byte(timodel.ActionRenameTable), Query: "RENAME TABLE `common_1`.`test3` TO `common`.`test4`;"},
		{FinishedTs: 11, SchemaName: "common_1", TableName: "test5", Type: byte(timodel.ActionCreateTable), Query: "CREATE TABLE `common_1`.`test5` (id int primary key);"},
	}
	watermark, err := encoder.EncodeCheckpointEvent(11)
	require.NoError(t, err)
	// Partition zero can resume halfway through the DDL job, or after it,
	// while other partitions still replay every copy.
	for _, startIndex := range []int{0, 1, len(ddls)} {
		client, err := kgo.NewClient()
		require.NoError(t, err)
		t.Cleanup(client.Close)
		memory := &memoryUsage{}
		c := &kafkaReader{client: client, buffer: &readBuffer{memory: memory, protocol: config.ProtocolOpen, partitions: make(map[int32]*partition), orderedDML: true}, offsets: make(map[*ack]int64), ddlCopies: make(map[uint64][]*ack)}
		for partitionID := range int32(2) {
			decoder, err := open.NewDecoder(t.Context(), int(partitionID), codecConfig, nil)
			require.NoError(t, err)
			c.buffer.partitions[partitionID] = &partition{decoder: decoder, schemas: make(map[schemaKey]bool), schemaPointers: make(map[*common.TableInfo]bool)}
		}
		// Noncanonical copies may arrive first and are never executed.
		for _, partitionID := range []int32{1, 0} {
			input := ddls
			if partitionID == 0 {
				input = input[startIndex:]
			}
			for offset, ddl := range input {
				message, err := encoder.EncodeDDLEvent(ddl)
				require.NoError(t, err)
				require.NoError(t, c.processRecord(t.Context(), &kgo.Record{Partition: partitionID, Offset: int64(offset), Key: message.Key, Value: message.Value}))
			}
			require.NoError(t, c.processRecord(t.Context(), &kgo.Record{Partition: partitionID, Offset: int64(len(input)), Key: watermark.Key, Value: watermark.Value}))
		}
		downstream := mock.NewMockSink(gomock.NewController(t))
		w := &writer{downstream: downstream, memory: memory}
		consumer := &consumer{writer: w}
		for index, ddl := range ddls[startIndex:] {
			result, err := c.Read(t.Context())
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, ddl.Query, result.ddl.Query)
			require.EqualValues(t, 1, c.buffer.partitions[0].records[index].refs.Load())
			downstream.EXPECT().FlushDMLBeforeBlock(result.ddl).Return(nil)
			downstream.EXPECT().WriteBlockEvent(result.ddl).Return(nil)
			require.NoError(t, consumer.writeDDL(t.Context(), result))
			require.Zero(t, c.buffer.partitions[0].records[index].refs.Load())
			for _, record := range c.buffer.partitions[1].records[:len(ddls)] {
				require.EqualValues(t, 1, record.refs.Load())
			}
		}
		result, err := c.Read(t.Context())
		require.NoError(t, err)
		require.True(t, result.hasWatermark)
		require.EqualValues(t, 11, result.watermark)
		require.NotNil(t, result.onFlush)
		result.onFlush()
		for _, p := range c.buffer.partitions {
			for _, record := range p.records {
				require.Zero(t, record.refs.Load())
			}
		}
		remainingBytes := int64(0)
		for record := range c.offsets {
			remainingBytes += record.memory.Load()
		}
		require.Equal(t, remainingBytes, memory.bytes.Load())
	}
}

func TestReaderDDLNormalization(t *testing.T) {
	memory := &memoryUsage{}
	buffer := &readBuffer{memory: memory}
	first, err := buffer.memory.newAck(t.Context(), 128)
	require.NoError(t, err)
	ddl := &event.DDLEvent{Type: byte(timodel.ActionAddColumn), SchemaName: "test", TableName: "t", Query: "alter table t add column v int", FinishedTs: 10}
	require.NoError(t, buffer.queueDDL(t.Context(), ddl, first))
	result := buffer.nextReady(9)
	require.Nil(t, result)
	first.refs.Add(-1)
	beforeDDL := &event.DMLEvent{CommitTs: 10}
	afterDDL := &event.DMLEvent{CommitTs: 11}
	buffer.pendingDML = []*readResult{{dml: beforeDDL}, {dml: afterDDL}}
	result = buffer.nextReady(10)
	require.Same(t, beforeDDL, result.dml)
	result = buffer.nextReady(10)
	require.Same(t, ddl, result.ddl)
	require.EqualValues(t, 1, first.refs.Load())

	downstream := mock.NewMockSink(gomock.NewController(t))
	gomock.InOrder(
		downstream.EXPECT().FlushDMLBeforeBlock(ddl).Return(nil),
		downstream.EXPECT().WriteBlockEvent(ddl).Return(nil),
	)
	w := &writer{downstream: downstream, memory: memory}
	c := &consumer{writer: w}
	require.NoError(t, c.writeDDL(t.Context(), result))
	require.Zero(t, first.refs.Load())
	result = buffer.nextReady(11)
	require.Same(t, afterDDL, result.dml)
	require.EqualValues(t, 128, memory.bytes.Load())
}

func TestKafkaReaderWatermark(t *testing.T) {
	c := &kafkaReader{buffer: &readBuffer{partitions: map[int32]*partition{
		0: {watermark: 20, hasWatermark: true},
		1: {watermark: 10, hasWatermark: true},
	}}}
	watermark, ready := c.globalWatermark()
	require.True(t, ready)
	require.EqualValues(t, 10, watermark)
	c.buffer.partitions[1].hasWatermark = false
	_, ready = c.globalWatermark()
	require.False(t, ready)
	c.buffer.partitions[1].hasWatermark = true
	c.buffer.partitions[1].cachedUnreleased = 1
	_, ready = c.globalWatermark()
	require.False(t, ready)
}

func TestPulsarWatermarkWaitsForPartitionPositions(t *testing.T) {
	memory := &memoryUsage{}
	require.NoError(t, memory.reserve(t.Context(), 128+3*128))
	record := &ack{}
	record.refs.Store(1)
	control := &readResult{watermark: 100, hasWatermark: true, onFlush: func() { record.refs.Add(-1) }}
	ddl := &event.DDLEvent{FinishedTs: 100}
	c := &pulsarReader{
		buffer: &readBuffer{
			memory: memory, orderedDML: true, partitions: map[int32]*partition{0: {}, 1: {}, 2: {}},
			pendingDDL: []*readDDL{{event: ddl, record: &ack{}}},
		},
		positions: map[int32]pulsar.MessageID{0: pulsar.NewMessageID(1, 10, -1, 0)},
		checkpoints: []*pulsarCheckpoint{{result: control, positions: map[int32]pulsar.MessageID{
			0: pulsar.NewMessageID(1, 10, -1, 0), 1: pulsar.NewMessageID(1, 5, -1, 1), 2: pulsar.EarliestMessageID(),
		}}},
	}
	// A checkpoint seen on one partition cannot release DDL while another is unread.
	c.advanceWatermarks()
	require.Empty(t, c.pendingWatermarks)
	require.Nil(t, c.buffer.nextReady(c.watermark))
	c.positions[1] = pulsar.NewMessageID(1, 4, -1, 1)
	c.advanceWatermarks()
	require.Empty(t, c.pendingWatermarks)
	// Reaching the broker entry without decoding its entire batch is also insufficient.
	c.positions[1] = pulsar.NewMessageID(1, 5, 0, 1)
	c.advanceWatermarks()
	require.Empty(t, c.pendingWatermarks)
	require.Nil(t, c.buffer.nextReady(c.watermark))
	c.positions[1] = pulsar.NewMessageID(1, 5, -1, 1)
	c.advanceWatermarks()
	require.EqualValues(t, 100, c.watermark)
	require.Same(t, ddl, c.buffer.nextReady(c.watermark).ddl)
	require.Empty(t, c.checkpoints)
	require.Equal(t, []*readResult{control}, c.pendingWatermarks)
	require.Zero(t, memory.used())
	// Read progress alone cannot confirm the checkpoint input to Pulsar.
	require.EqualValues(t, 1, record.refs.Load())
	control.onFlush()
	require.Zero(t, record.refs.Load())
}

func TestKafkaReaderDDLWaitsForBufferedDML(t *testing.T) {
	dml := &event.DMLEvent{CommitTs: 20}
	ddl := &event.DDLEvent{FinishedTs: 30}
	copyRecord := &ack{}
	copyRecord.refs.Store(1)
	memory := &memoryUsage{}
	memory.bytes.Store(128)
	c := &kafkaReader{
		buffer: &readBuffer{
			memory: memory, orderedDML: true,
			partitions: map[int32]*partition{
				0: {watermark: 40, hasWatermark: true},
				1: {watermark: 40, hasWatermark: true},
			},
			pendingDML: []*readResult{{dml: dml}},
			pendingDDL: []*readDDL{{event: ddl, record: &ack{}}},
		},
		// An already delivered CREATE TABLE still has an unconfirmed copy.
		ddlCopies: map[uint64][]*ack{10: {copyRecord}},
	}
	result, err := c.Read(t.Context())
	require.NoError(t, err)
	require.Same(t, dml, result.dml)
	require.EqualValues(t, 1, copyRecord.refs.Load())
	result, err = c.Read(t.Context())
	require.NoError(t, err)
	require.Same(t, ddl, result.ddl)
	result, err = c.Read(t.Context())
	require.NoError(t, err)
	require.True(t, result.hasWatermark)
	require.EqualValues(t, 40, result.watermark)
	require.EqualValues(t, 1, copyRecord.refs.Load())
	result.onFlush()
	require.Zero(t, copyRecord.refs.Load())
	require.Zero(t, memory.bytes.Load())
}

func TestInputCompletionAcrossBatches(t *testing.T) {
	memory := &memoryUsage{}
	buffer := &readBuffer{memory: memory}
	file, err := buffer.memory.newAck(t.Context(), 256)
	require.NoError(t, err)
	later, err := buffer.memory.newAck(t.Context(), 256)
	require.NoError(t, err)
	later.refs.Add(-1)
	file.refs.Add(2)
	firstDML, secondDML := &event.DMLEvent{}, &event.DMLEvent{}
	for _, dml := range []*event.DMLEvent{firstDML, secondDML} {
		dml.AddPostFlushFunc(func() {
			file.refs.Add(-1)
		})
	}
	first := &writeBatch{events: []*event.DMLEvent{firstDML}, done: make(chan bool)}
	second := &writeBatch{events: []*event.DMLEvent{secondDML}, done: make(chan bool)}
	input := &storageReader{buffer: buffer, records: []*ack{file, later}}
	w := &writer{memory: memory, inFlight: []*writeBatch{first, second}}
	c := &consumer{reader: input, writer: w}
	// A later input cannot release positions past an incomplete file.
	require.NoError(t, c.confirmCompleted(t.Context()))
	require.Len(t, input.records, 2)
	secondDML.PostFlush()
	close(second.done)
	w.finishBatches()
	require.NoError(t, c.confirmCompleted(t.Context()))
	require.EqualValues(t, 2, file.refs.Load())
	firstDML.PostFlush()
	close(first.done)
	w.finishBatches()
	require.NoError(t, c.confirmCompleted(t.Context()))
	require.EqualValues(t, 1, file.refs.Load())
	require.Len(t, input.records, 2)
	// Both batches are durable, but the decoder may still register more rows.
	file.refs.Add(-1)
	require.NoError(t, c.confirmCompleted(t.Context()))
	require.Empty(t, input.records)
	require.EqualValues(t, 2, memory.confirmed.Load())
	require.Zero(t, memory.bytes.Load())
}

func TestWatermarkConfirmationWaitsForWrites(t *testing.T) {
	memory := &memoryUsage{}
	buffer := &readBuffer{memory: memory}
	record, err := buffer.memory.newAck(t.Context(), 128)
	require.NoError(t, err)
	input := &storageReader{buffer: buffer, records: []*ack{record}}
	batch := &writeBatch{items: []*readResult{{dml: &event.DMLEvent{CommitTs: 10}}}, done: make(chan bool)}
	w := &writer{memory: memory, inFlight: []*writeBatch{batch}}
	c := &consumer{
		reader: input, writer: w,
		pendingWatermarks: []*readResult{{watermark: 10, onFlush: func() {
			record.refs.Add(-1)
		}}},
	}
	require.NoError(t, c.confirmCompleted(t.Context()))
	require.EqualValues(t, 1, record.refs.Load())
	require.Len(t, input.records, 1)
	close(batch.done)
	require.NoError(t, c.waitBatch(t.Context(), batch))
	require.Empty(t, input.records)
	require.Empty(t, c.pendingWatermarks)
	require.Zero(t, memory.bytes.Load())
}

func TestReadyDMLFlushDoesNotNeedAnotherWatermark(t *testing.T) {
	table := common.NewTableInfo4Decoder("test", &timodel.TableInfo{
		ID: 1, Name: ast.NewCIStr("t"),
		Columns: []*timodel.ColumnInfo{{ID: 1, Name: ast.NewCIStr("v"), Offset: 0, State: timodel.StatePublic, FieldType: *types.NewFieldType(mysql.TypeLong)}},
	})
	dml := event.NewDMLEvent(common.NewDispatcherID(), 1, 0, 10, table)
	rows := chunk.NewChunkWithCapacity(table.GetFieldSlice(), 1)
	rows.AppendRow(chunk.MutRowFromValues(int64(1)).ToRow())
	dml.SetRows(rows)
	dml.RowTypes = []common.RowType{common.RowTypeInsert}
	dml.Length = 1
	memory := &memoryUsage{}
	buffer := &readBuffer{memory: memory}
	record, err := buffer.memory.newAck(t.Context(), 128)
	require.NoError(t, err)
	require.NoError(t, buffer.queueDML(t.Context(), dml, []*ack{record}, nil))
	record.refs.Add(-1)
	result := buffer.nextReady(10)
	input := &storageReader{buffer: buffer, records: []*ack{record}}
	downstream := mock.NewMockSink(gomock.NewController(t))
	downstream.EXPECT().AddDMLEvent(dml).Do(func(dml *event.DMLEvent) { dml.PostFlush() })
	w := &writer{downstream: downstream, memory: memory, mutations: make(map[mutationKey]*writeBatch)}
	c := &consumer{reader: input, writer: w}
	require.NoError(t, c.consume(t.Context(), result))
	require.Len(t, w.pendingDML, 1)
	require.NoError(t, c.flushDML(t.Context(), nil))
	require.Empty(t, w.pendingDML)
	require.NoError(t, c.confirmCompleted(t.Context()))
	require.Empty(t, input.records)
	require.Zero(t, memory.bytes.Load())
}

func TestStorageProgressDoesNotRejectUnreadRows(t *testing.T) {
	table := common.NewTableInfo4Decoder("test", &timodel.TableInfo{ID: 1, Name: ast.NewCIStr("t")})
	dml := &event.DMLEvent{PhysicalTableID: 1, CommitTs: 10, TableInfo: table}
	w := &writer{memory: &memoryUsage{}, mutations: make(map[mutationKey]*writeBatch)}
	c := &consumer{writer: w}
	w.advanceReplay(20, 1)
	filtered, err := c.filterRows(t.Context(), dml, &writeBatch{})
	require.NoError(t, err)
	require.Same(t, dml, filtered)
	// A true topic-wide complete watermark retains the MQ replay cutoff.
	w.advanceReplay(20, 0)
	filtered, err = c.filterRows(t.Context(), dml, &writeBatch{})
	require.NoError(t, err)
	require.Nil(t, filtered)
}

func TestReaderBudgetIncludesInFlightMemory(t *testing.T) {
	memory := &memoryUsage{}
	memory.bytes.Store(maxMemoryBytes - 32)
	require.NoError(t, memory.reserve(t.Context(), 32))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, memory.reserve(ctx, 1), context.Canceled)
	var wg sync.WaitGroup
	done := make(chan error, 1)
	wg.Go(func() { done <- memory.reserve(t.Context(), 128) })
	memory.release(128)
	require.NoError(t, <-done)
	wg.Wait()
	memory.release(128)
	require.NoError(t, memory.reserve(t.Context(), 128))
	buffer := &readBuffer{memory: memory}
	_, err := buffer.memory.newAck(ctx, 256)
	require.ErrorIs(t, err, context.Canceled)
}

func TestKafkaReaderPrioritizesBlockedPartitions(t *testing.T) {
	c := &kafkaReader{
		buffer: &readBuffer{partitions: map[int32]*partition{
			0: {hasWatermark: true, watermark: 20},
			1: {hasWatermark: true, watermark: 10},
			2: {},
		}},
		polled: map[int32][]*kgo.Record{
			0: {{Partition: 0}}, 1: {{Partition: 1}}, 2: {{Partition: 2}},
		},
	}
	require.EqualValues(t, 2, c.nextPartition())
	c.buffer.partitions[2].hasWatermark = true
	c.buffer.partitions[2].watermark = 10
	c.buffer.partitions[1].readSequence = 1
	require.EqualValues(t, 2, c.nextPartition())
	c.buffer.partitions[2].readSequence = 2
	require.EqualValues(t, 1, c.nextPartition())
	c.buffer.partitions[0].cachedUnreleased = 1
	require.EqualValues(t, 0, c.nextPartition())
}

func TestOrderedReaderKeepsInputOrderAndDDLBoundary(t *testing.T) {
	first := &event.DMLEvent{CommitTs: 20}
	second := &event.DMLEvent{CommitTs: 10}
	buffer := &readBuffer{
		memory: &memoryUsage{}, orderedDML: true, dmlBoundary: ^uint64(0), dmlDirty: true,
		pendingDML: []*readResult{{dml: first}, {dml: second}},
	}
	require.Same(t, first, buffer.nextReady(0).dml)
	require.Same(t, second, buffer.nextReady(0).dml)
	ddl := &event.DDLEvent{FinishedTs: 15}
	buffer.pendingDML = []*readResult{{dml: first}, {dml: second}}
	buffer.pendingDDL = []*readDDL{{event: ddl}}
	require.Same(t, second, buffer.nextReady(0).dml)
	require.Nil(t, buffer.nextReady(0))
	buffer.pendingDDL = nil
	buffer.dmlBoundary = 15
	require.Nil(t, buffer.nextReady(0))
	buffer.pendingDML = nil
	firstDDL, secondDDL := &event.DDLEvent{FinishedTs: 20}, &event.DDLEvent{FinishedTs: 10}
	require.NoError(t, buffer.queueDDL(t.Context(), firstDDL, &ack{}))
	require.NoError(t, buffer.queueDDL(t.Context(), secondDDL, &ack{}))
	require.Same(t, firstDDL, buffer.nextReady(^uint64(0)).ddl)
	require.Same(t, secondDDL, buffer.nextReady(^uint64(0)).ddl)
}

func TestReaderDDLArrivalOrder(t *testing.T) {
	tableA := common.NewTableInfo4Decoder("test", &timodel.TableInfo{ID: 1, Name: ast.NewCIStr("a")})
	tableB := common.NewTableInfo4Decoder("test", &timodel.TableInfo{ID: 2, Name: ast.NewCIStr("b")})
	beforeA, afterA := &event.DMLEvent{CommitTs: 280, TableInfo: tableA}, &event.DMLEvent{CommitTs: 310, TableInfo: tableA}
	beforeB, afterB := &event.DMLEvent{CommitTs: 180, TableInfo: tableB}, &event.DMLEvent{CommitTs: 220, TableInfo: tableB}
	buffer := &readBuffer{
		memory: &memoryUsage{}, orderedDML: true, dmlBoundary: ^uint64(0),
		pendingDML: []*readResult{{dml: beforeB}, {dml: afterB}, {dml: beforeA}, {dml: afterA}},
	}
	ddlA := &event.DDLEvent{SchemaName: "test", TableName: "a", FinishedTs: 300}
	ddlB := &event.DDLEvent{SchemaName: "test", TableName: "b", FinishedTs: 200}
	require.NoError(t, buffer.queueDDL(t.Context(), ddlA, &ack{}))
	require.NoError(t, buffer.queueDDL(t.Context(), ddlB, &ack{}))
	// Only the head DDL's table is drained, even though B's DML has smaller timestamps.
	require.Same(t, beforeA, buffer.nextReady(300).dml)
	require.Same(t, ddlA, buffer.nextReady(300).ddl)
	require.Same(t, beforeB, buffer.nextReady(300).dml)
	require.Same(t, ddlB, buffer.nextReady(300).ddl)
	require.Same(t, afterB, buffer.nextReady(300).dml)
	require.Same(t, afterA, buffer.nextReady(300).dml)
}

func TestDDLScope(t *testing.T) {
	tableA := common.NewTableInfo4Decoder("test", &timodel.TableInfo{ID: 1, Name: ast.NewCIStr("a")})
	tableB := common.NewTableInfo4Decoder("test", &timodel.TableInfo{ID: 2, Name: ast.NewCIStr("b")})
	tableC := common.NewTableInfo4Decoder("other", &timodel.TableInfo{ID: 3, Name: ast.NewCIStr("c")})
	a := &event.DMLEvent{PhysicalTableID: 10, TableInfo: tableA}
	b := &event.DMLEvent{PhysicalTableID: 20, TableInfo: tableB}
	c := &event.DMLEvent{PhysicalTableID: 30, TableInfo: tableC}
	ddl := &event.DDLEvent{SchemaName: "test", TableName: "a"}
	require.True(t, ddlBlocksTable(ddl, a))
	require.True(t, ddlBlocksTable(ddl, &event.DMLEvent{PhysicalTableID: 11, TableInfo: tableA}))
	require.False(t, ddlBlocksTable(ddl, b))
	ddl = &event.DDLEvent{SchemaName: "test", BlockedTables: &event.InfluencedTables{InfluenceType: event.InfluenceTypeDB}}
	require.True(t, ddlBlocksTable(ddl, a))
	require.True(t, ddlBlocksTable(ddl, b))
	require.False(t, ddlBlocksTable(ddl, c))
	ddl = &event.DDLEvent{SchemaName: "test", TableName: "a", ExtraSchemaName: "other", ExtraTableName: "c", Type: byte(timodel.ActionRenameTable)}
	require.True(t, ddlBlocksTable(ddl, a))
	require.True(t, ddlBlocksTable(ddl, c))
	require.False(t, ddlBlocksTable(ddl, b))
	ddl = &event.DDLEvent{
		SchemaName: "test", TableName: "a", Type: byte(timodel.ActionExchangeTablePartition),
		BlockedTableNames: []event.SchemaTableName{{SchemaName: "test", TableName: "a"}, {SchemaName: "other", TableName: "c"}},
	}
	require.True(t, ddlBlocksTable(ddl, a))
	require.True(t, ddlBlocksTable(ddl, c))
	require.False(t, ddlBlocksTable(ddl, b))
	ddl = &event.DDLEvent{SchemaName: "test", BlockedTables: &event.InfluencedTables{InfluenceType: event.InfluenceTypeAll}}
	require.True(t, ddlBlocksTable(ddl, a))
	require.True(t, ddlBlocksTable(ddl, b))
	require.True(t, ddlBlocksTable(ddl, c))
}

func TestDDLCancellationLeavesInputUnconfirmed(t *testing.T) {
	memory := &memoryUsage{}
	buffer := &readBuffer{memory: memory}
	record, err := buffer.memory.newAck(t.Context(), 128)
	require.NoError(t, err)
	input := &storageReader{buffer: buffer, records: []*ack{record}}
	table := common.NewTableInfo4Decoder("test", &timodel.TableInfo{ID: 1, Name: ast.NewCIStr("t")})
	batch := &writeBatch{items: []*readResult{{dml: &event.DMLEvent{CommitTs: 9, TableInfo: table}}}, done: make(chan bool)}
	ddl := &event.DDLEvent{SchemaName: "test", TableName: "t", Query: "alter table t add column v int", FinishedTs: 10}
	result := &readResult{ddl: ddl, onFlush: func() {
		record.refs.Add(-1)
	}}
	downstream := mock.NewMockSink(gomock.NewController(t))
	w := &writer{downstream: downstream, memory: memory, inFlight: []*writeBatch{batch}, inFlightEvents: len(batch.items)}
	c := &consumer{reader: input, writer: w}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, c.consume(ctx, result), context.Canceled)
	require.EqualValues(t, 1, record.refs.Load())
	close(batch.done)
	require.ErrorIs(t, c.confirmCompleted(ctx), context.Canceled)
	require.Len(t, input.records, 1)
	require.EqualValues(t, 128, memory.bytes.Load())
}

func TestConsumerCancellationDuringStartup(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.Protocol = new("csv")
	upstreamURI := &url.URL{Scheme: "file", Path: t.TempDir()}
	c, err := newConsumer(ctx, upstreamURI, "blackhole://", "", "UTC", replicaConfig)
	require.NoError(t, err)
	var wg sync.WaitGroup
	done := make(chan error, 1)
	wg.Go(func() { done <- c.start(ctx) })
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not stop after cancellation")
	}
	wg.Wait()
	require.ErrorIs(t, context.Cause(ctx), context.Canceled)
	require.False(t, errors.Is(context.Cause(ctx), context.DeadlineExceeded))
}
