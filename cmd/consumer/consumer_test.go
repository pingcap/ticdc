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
	"sync"
	"testing"
	"time"

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
	"github.com/twmb/franz-go/pkg/kgo"
)

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
		memory := &bufferUsage{}
		c := &kafkaReader{client: client, buffer: &readBuffer{memory: memory, protocol: config.ProtocolOpen, partitions: make(map[int32]*partition)}, offsets: make(map[*inputRecord]int64), ddlCopies: make(map[uint64][]*inputRecord)}
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
				require.NoError(t, c.processRecord(&kgo.Record{Partition: partitionID, Offset: int64(offset), Key: message.Key, Value: message.Value}))
			}
			require.NoError(t, c.processRecord(&kgo.Record{Partition: partitionID, Offset: int64(len(input)), Key: watermark.Key, Value: watermark.Value}))
		}
		downstream := mock.NewMockSink(gomock.NewController(t))
		w := &writer{downstream: downstream, memory: memory}
		for index, ddl := range ddls[startIndex:] {
			result, err := c.Read(t.Context())
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, ddl.Query, result.ddl.Query)
			require.EqualValues(t, 1, c.buffer.partitions[0].records[index].pending.Load())
			downstream.EXPECT().FlushDMLBeforeBlock(result.ddl).Return(nil)
			downstream.EXPECT().WriteBlockEvent(result.ddl).Return(nil)
			require.NoError(t, w.writeDDL(t.Context(), t.Context(), result))
			require.Zero(t, c.buffer.partitions[0].records[index].pending.Load())
			for _, record := range c.buffer.partitions[1].records[:len(ddls)] {
				require.EqualValues(t, 1, record.pending.Load())
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
				require.Zero(t, record.pending.Load())
			}
		}
		require.Zero(t, memory.effects.Load())
		remainingBytes := int64(0)
		for record := range c.offsets {
			remainingBytes += record.bytes.Load()
		}
		require.Equal(t, remainingBytes, memory.bytes.Load())
		require.Equal(t, remainingBytes, memory.readBytes.Load())
	}
}

func TestReaderDDLNormalization(t *testing.T) {
	memory := &bufferUsage{}
	buffer := &readBuffer{memory: memory}
	first, err := buffer.newRecord(128)
	require.NoError(t, err)
	ddl := &event.DDLEvent{Type: byte(timodel.ActionAddColumn), SchemaName: "test", TableName: "t", Query: "alter table t add column v int", FinishedTs: 10}
	require.NoError(t, buffer.queueDDL(ddl, first))
	result := buffer.nextReady(9)
	require.Nil(t, result)
	first.pending.Add(-1)
	memory.effects.Add(-1)
	beforeDDL := &event.DMLEvent{CommitTs: 10}
	afterDDL := &event.DMLEvent{CommitTs: 11}
	buffer.pendingDML = []*readDML{{event: beforeDDL}, {event: afterDDL}}
	result = buffer.nextReady(10)
	require.Same(t, beforeDDL, result.dml)
	result = buffer.nextReady(10)
	require.Same(t, ddl, result.ddl)
	require.EqualValues(t, 1, first.pending.Load())

	downstream := mock.NewMockSink(gomock.NewController(t))
	gomock.InOrder(
		downstream.EXPECT().FlushDMLBeforeBlock(ddl).Return(nil),
		downstream.EXPECT().WriteBlockEvent(ddl).Return(nil),
	)
	w := &writer{downstream: downstream, memory: memory}
	require.NoError(t, w.writeDDL(t.Context(), t.Context(), result))
	require.Zero(t, first.pending.Load())
	require.Zero(t, memory.effects.Load())
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

func TestInputCompletionAcrossBatches(t *testing.T) {
	memory := &bufferUsage{}
	buffer := &readBuffer{memory: memory}
	file, err := buffer.newRecord(256)
	require.NoError(t, err)
	later, err := buffer.newRecord(256)
	require.NoError(t, err)
	later.pending.Add(-1)
	memory.effects.Add(-1)
	file.pending.Add(2)
	memory.effects.Add(2)
	firstDML, secondDML := &event.DMLEvent{}, &event.DMLEvent{}
	for _, dml := range []*event.DMLEvent{firstDML, secondDML} {
		dml.AddPostFlushFunc(func() {
			file.pending.Add(-1)
			memory.effects.Add(-1)
		})
	}
	first := &writeBatch{events: []*event.DMLEvent{firstDML}, done: make(chan bool)}
	second := &writeBatch{events: []*event.DMLEvent{secondDML}, done: make(chan bool)}
	input := &storageReader{buffer: buffer, records: []*inputRecord{file, later}}
	w := &writer{memory: memory, inFlight: []*writeBatch{first, second}}
	c := &consumer{reader: input, writer: w}
	// A later input cannot release positions past an incomplete file.
	require.NoError(t, c.confirmCompleted(t.Context()))
	require.Len(t, input.records, 2)
	secondDML.PostFlush()
	close(second.done)
	w.finishBatches()
	require.NoError(t, c.confirmCompleted(t.Context()))
	require.EqualValues(t, 2, file.pending.Load())
	firstDML.PostFlush()
	close(first.done)
	w.finishBatches()
	require.NoError(t, c.confirmCompleted(t.Context()))
	require.EqualValues(t, 1, file.pending.Load())
	require.Len(t, input.records, 2)
	// Both batches are durable, but the decoder may still register more rows.
	file.pending.Add(-1)
	memory.effects.Add(-1)
	require.NoError(t, c.confirmCompleted(t.Context()))
	require.Empty(t, input.records)
	require.EqualValues(t, 2, memory.confirmed.Load())
	require.Zero(t, memory.bytes.Load())
	require.Zero(t, memory.readBytes.Load())
	require.Zero(t, memory.records.Load())
	require.Zero(t, memory.effects.Load())
}

func TestWatermarkConfirmationWaitsForWrites(t *testing.T) {
	memory := &bufferUsage{}
	buffer := &readBuffer{memory: memory}
	record, err := buffer.newRecord(128)
	require.NoError(t, err)
	input := &storageReader{buffer: buffer, records: []*inputRecord{record}}
	batch := &writeBatch{items: []*pendingDML{{event: &event.DMLEvent{CommitTs: 10}}}, done: make(chan bool)}
	w := &writer{memory: memory, inFlight: []*writeBatch{batch}}
	c := &consumer{
		reader: input, writer: w,
		pendingWatermarks: []*readResult{{watermark: 10, onFlush: func() {
			record.pending.Add(-1)
			memory.effects.Add(-1)
		}}},
	}
	require.NoError(t, c.confirmCompleted(t.Context()))
	require.EqualValues(t, 1, record.pending.Load())
	require.Len(t, input.records, 1)
	close(batch.done)
	w.finishBatches()
	require.NoError(t, c.confirmCompleted(t.Context()))
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
	memory := &bufferUsage{}
	buffer := &readBuffer{memory: memory}
	record, err := buffer.newRecord(128)
	require.NoError(t, err)
	require.NoError(t, buffer.queueDML(dml, []*inputRecord{record}, nil))
	record.pending.Add(-1)
	memory.effects.Add(-1)
	result := buffer.nextReady(10)
	input := &storageReader{buffer: buffer, records: []*inputRecord{record}}
	downstream := mock.NewMockSink(gomock.NewController(t))
	downstream.EXPECT().AddDMLEvent(dml).Do(func(dml *event.DMLEvent) { dml.PostFlush() })
	w := &writer{downstream: downstream, memory: memory, mutations: make(map[mutationKey]*writeBatch)}
	c := &consumer{reader: input, writer: w}
	w.confirm = c.confirmCompleted
	require.NoError(t, c.consume(t.Context(), t.Context(), result))
	require.Len(t, w.pendingDML, 1)
	// Advance the batch's age deterministically, without a sleep.
	w.readySince = time.Now().Add(-batchLinger)
	require.NoError(t, w.flushDML(t.Context(), t.Context(), ^uint64(0), true, false))
	require.Empty(t, w.pendingDML)
	require.NoError(t, c.confirmCompleted(t.Context()))
	require.Empty(t, input.records)
	require.Zero(t, memory.bytes.Load())
}

func TestStorageProgressDoesNotRejectUnreadRows(t *testing.T) {
	table := common.NewTableInfo4Decoder("test", &timodel.TableInfo{ID: 1, Name: ast.NewCIStr("t")})
	dml := &event.DMLEvent{PhysicalTableID: 1, CommitTs: 10, TableInfo: table}
	w := &writer{memory: &bufferUsage{}, mutations: make(map[mutationKey]*writeBatch)}
	w.advanceReplay(20, 1)
	filtered, err := w.filterRows(t.Context(), dml, &writeBatch{})
	require.NoError(t, err)
	require.Same(t, dml, filtered)
	// A true topic-wide complete watermark retains the MQ replay cutoff.
	w.advanceReplay(20, 0)
	filtered, err = w.filterRows(t.Context(), dml, &writeBatch{})
	require.NoError(t, err)
	require.Nil(t, filtered)
}

func TestReaderBudgetIncludesInFlightMemory(t *testing.T) {
	memory := &bufferUsage{}
	memory.bytes.Store(maxBufferedBytes - 32)
	require.NoError(t, memory.reserve(32))
	require.Error(t, memory.reserve(1))
	memory.bytes.Add(-128)
	require.NoError(t, memory.reserve(128))
	buffer := &readBuffer{memory: memory}
	_, err := buffer.newRecord(256)
	require.Error(t, err)
	require.Zero(t, memory.records.Load())
}

func TestDDLRetryAfterReadCancellation(t *testing.T) {
	memory := &bufferUsage{}
	buffer := &readBuffer{memory: memory}
	record, err := buffer.newRecord(128)
	require.NoError(t, err)
	input := &storageReader{buffer: buffer, records: []*inputRecord{record}}
	table := common.NewTableInfo4Decoder("test", &timodel.TableInfo{ID: 1, Name: ast.NewCIStr("t")})
	batch := &writeBatch{items: []*pendingDML{{event: &event.DMLEvent{CommitTs: 9, TableInfo: table}}}, done: make(chan bool)}
	ddl := &event.DDLEvent{SchemaName: "test", TableName: "t", Query: "alter table t add column v int", FinishedTs: 10}
	result := &readResult{ddl: ddl, onFlush: func() {
		record.pending.Add(-1)
		memory.effects.Add(-1)
	}}
	downstream := mock.NewMockSink(gomock.NewController(t))
	gomock.InOrder(
		downstream.EXPECT().FlushDMLBeforeBlock(ddl).Return(nil),
		downstream.EXPECT().WriteBlockEvent(ddl).Return(nil),
	)
	w := &writer{downstream: downstream, memory: memory, inFlight: []*writeBatch{batch}}
	c := &consumer{reader: input, writer: w}
	w.confirm = c.confirmCompleted
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, c.consume(ctx, t.Context(), result), context.Canceled)
	require.EqualValues(t, 1, record.pending.Load())
	// The shutdown drain must finish this DDL before consuming later results.
	close(batch.done)
	require.NoError(t, w.writeDDL(t.Context(), t.Context(), result))
	require.NoError(t, c.confirmCompleted(t.Context()))
	require.Empty(t, input.records)
	require.Zero(t, memory.bytes.Load())
}

func TestConsumerCancellationDuringStartup(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.Protocol = new("csv")
	upstreamURI := &url.URL{Scheme: "file", Path: t.TempDir()}
	memory := &bufferUsage{}
	input, err := newStorageReader(ctx, upstreamURI, "UTC", replicaConfig, memory)
	require.NoError(t, err)
	var wg sync.WaitGroup
	done := make(chan error, 1)
	wg.Go(func() { done <- runConsumer(ctx, &wg, input, "blackhole://", replicaConfig, memory) })
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(shutdownTimeout + time.Second):
		t.Fatal("consumer did not stop after cancellation")
	}
	wg.Wait()
	require.ErrorIs(t, context.Cause(ctx), context.Canceled)
	require.False(t, errors.Is(context.Cause(ctx), context.DeadlineExceeded))
}
