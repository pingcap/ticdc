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
	"strconv"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/downstreamadapter/sink/mock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func conductorTestRow(memory *memoryUsage, tableID int64, stream int32, ts uint64, ready bool) *decodeTask {
	name := "t" + strconv.FormatInt(tableID, 10)
	table := common.NewTableInfo4Decoder("test", &timodel.TableInfo{ID: tableID, Name: ast.NewCIStr(name)})
	row := &decodeTask{
		tableID: tableID, stream: stream, commitTs: ts,
		name:   event.SchemaTableName{SchemaName: "test", TableName: name},
		record: newAck(memory, 10, nil), done: make(chan struct{}), bytes: 64,
		dml: &event.DMLEvent{PhysicalTableID: tableID, CommitTs: ts, Length: 1, TableInfo: table},
	}
	memory.reserve(row.bytes)
	if ready {
		close(row.done)
	}
	return row
}

func TestConductorGroupsWaitForConversion(t *testing.T) {
	memory := newMemoryUsage()
	c := &conductor{orderedDML: true, watermark: 200}
	slow := conductorTestRow(memory, 1, 0, 150, false)
	later := conductorTestRow(memory, 1, 0, 160, true)
	otherInput := conductorTestRow(memory, 1, 1, 170, true)
	independent := conductorTestRow(memory, 2, 0, 180, true)
	for _, row := range []*decodeTask{slow, later, otherInput, independent} {
		c.queueDML(row)
	}
	allow := func(*decodeTask) bool { return true }
	require.Same(t, independent, c.next(allow))
	require.Nil(t, c.next(allow))
	w := &writer{}
	c.updateWatermark(w)
	require.True(t, w.hasUnsubmitted)
	require.Equal(t, uint64(150), w.unsubmitted)
	close(slow.done)
	for _, row := range []*decodeTask{slow, later, otherInput} {
		require.Same(t, row, c.next(allow))
	}
	require.Zero(t, c.pendingDMLCount)
	require.Empty(t, c.pendingDML)
}

func TestConductorOtherSourceCommitOrder(t *testing.T) {
	c := &conductor{}
	for _, ts := range []uint64{180, 150, 160, 150} {
		c.queueDML(&decodeTask{tableID: 1, commitTs: ts})
	}
	var timestamps []uint64
	for c.pendingDMLCount != 0 {
		row := firstDML(c.pendingDML[1])
		timestamps = append(timestamps, c.popDML(row).commitTs)
	}
	require.Equal(t, []uint64{150, 150, 160, 180}, timestamps)
}

func TestConductorFlushBoundsBacklogAndCheckpoint(t *testing.T) {
	memory := newMemoryUsage()
	c := &conductor{orderedDML: true, watermark: 200}
	count := maxInFlightEvents + batchRows + 1
	rows := make([]*decodeTask, count)
	for i := range rows {
		rows[i] = conductorTestRow(memory, 1, 0, 150, true)
		c.queueDML(rows[i])
	}
	marker := newAck(memory, 10, nil)
	c.addWatermark(200, []*ack{marker})
	downstream := mock.NewMockSink(gomock.NewController(t))
	var submitted []*event.DMLEvent
	downstream.EXPECT().AddDMLEvent(gomock.Any()).Do(func(row *event.DMLEvent) {
		submitted = append(submitted, row)
	}).AnyTimes()
	w := &writer{downstream: downstream, memory: memory}
	require.NoError(t, c.flush(t.Context(), w))
	require.Len(t, submitted, maxInFlightEvents)
	require.Equal(t, count-maxInFlightEvents, c.pendingDMLCount)
	require.Empty(t, w.pendingDML)
	require.Equal(t, uint64(149), w.checkpoint)
	require.False(t, rows[0].record.ready())
	require.False(t, marker.ready())

	// A later completion must not release the earlier timestamp or its ack.
	submitted[len(submitted)-1].PostFlush()
	require.NoError(t, c.flush(t.Context(), w))
	require.Equal(t, uint64(149), w.checkpoint)
	require.False(t, rows[0].record.ready())
	require.True(t, rows[maxInFlightEvents-1].record.ready())
	for _, row := range submitted[:maxInFlightEvents-1] {
		row.PostFlush()
	}
	require.NoError(t, c.flush(t.Context(), w))
	require.Len(t, submitted, count)
	require.Zero(t, c.pendingDMLCount)
	require.Empty(t, w.pendingDML)
	require.Equal(t, uint64(149), w.checkpoint)
	for _, row := range submitted[maxInFlightEvents:] {
		row.PostFlush()
	}
	require.NoError(t, c.flush(t.Context(), w))
	require.Equal(t, uint64(200), w.checkpoint)
	require.True(t, marker.ready())
	require.Empty(t, w.inFlight)
	require.Zero(t, memory.used())
}

func TestConductorFlushKeepsDDLBarrier(t *testing.T) {
	memory := newMemoryUsage()
	c := &conductor{orderedDML: true, watermark: 200}
	before := conductorTestRow(memory, 1, 0, 150, true)
	after := conductorTestRow(memory, 1, 0, 160, true)
	independent := conductorTestRow(memory, 2, 0, 170, true)
	for _, row := range []*decodeTask{before, after, independent} {
		c.queueDML(row)
	}
	ddl := &event.DDLEvent{FinishedTs: 155, SchemaName: "test", TableName: "t1", Type: byte(timodel.ActionAddColumn)}
	record := newAck(memory, 10, nil)
	c.queueDDL(ddl, record)
	record.release()
	downstream := mock.NewMockSink(gomock.NewController(t))
	var submitted []*event.DMLEvent
	downstream.EXPECT().AddDMLEvent(gomock.Any()).Do(func(row *event.DMLEvent) {
		submitted = append(submitted, row)
	}).AnyTimes()
	downstream.EXPECT().FlushDMLBeforeBlock(ddl).Return(nil)
	downstream.EXPECT().WriteBlockEvent(ddl).Return(nil)
	w := &writer{downstream: downstream, memory: memory, ddlJobs: make(chan *ddlWrite, 1), ddlDone: make(chan struct{}, 1)}
	require.NoError(t, c.flush(t.Context(), w))
	require.Len(t, submitted, 2)
	require.Contains(t, submitted, before.dml)
	require.Contains(t, submitted, independent.dml)
	require.NotContains(t, submitted, after.dml)
	require.Empty(t, w.ddlJobs)
	require.Equal(t, uint64(149), w.checkpoint)
	for _, row := range submitted {
		row.PostFlush()
	}
	require.NoError(t, c.flush(t.Context(), w))
	require.Len(t, w.ddlJobs, 1)
	require.Equal(t, uint64(154), w.checkpoint)
	require.Len(t, submitted, 2)
	require.False(t, record.ready())
	job := <-w.ddlJobs
	require.NoError(t, w.writeDDL(t.Context(), job))
	w.ddlDone <- struct{}{}
	require.NoError(t, c.flush(t.Context(), w))
	require.Len(t, submitted, 3)
	require.Same(t, after.dml, submitted[2])
	require.Equal(t, uint64(159), w.checkpoint)
	submitted[2].PostFlush()
	require.NoError(t, c.flush(t.Context(), w))
	require.Equal(t, uint64(200), w.checkpoint)
	require.True(t, record.ready())
	require.Zero(t, memory.used())
}

func TestConductorFlushUsesSourceBoundaryBeforeGlobalWatermark(t *testing.T) {
	memory := newMemoryUsage()
	c := &conductor{orderedDML: true, watermark: 100}
	row := conductorTestRow(memory, 1, 1, 150, true)
	row.boundary = &readBoundary{reached: true}
	c.queueDML(row)
	downstream := mock.NewMockSink(gomock.NewController(t))
	downstream.EXPECT().AddDMLEvent(row.dml).Do(func(dml *event.DMLEvent) { dml.PostFlush() })
	w := &writer{downstream: downstream, memory: memory}
	require.NoError(t, c.flush(t.Context(), w))
	require.Zero(t, c.pendingDMLCount)
	require.Equal(t, uint64(100), w.checkpoint)
	require.True(t, row.record.ready())
	require.Zero(t, memory.used())
}

func TestConductorFlushByteBudget(t *testing.T) {
	for _, size := range []int64{maxInFlightBytes / 2, maxInFlightBytes + 1} {
		t.Run(strconv.FormatInt(size, 10), func(t *testing.T) {
			memory := newMemoryUsage()
			c := &conductor{orderedDML: true, watermark: 200}
			for range 3 {
				row := conductorTestRow(memory, 1, 0, 150, true)
				memory.reserve(size - row.bytes)
				row.bytes = size
				c.queueDML(row)
			}
			downstream := mock.NewMockSink(gomock.NewController(t))
			var submitted []*event.DMLEvent
			downstream.EXPECT().AddDMLEvent(gomock.Any()).Do(func(row *event.DMLEvent) {
				submitted = append(submitted, row)
			}).AnyTimes()
			w := &writer{downstream: downstream, memory: memory}
			require.NoError(t, c.flush(t.Context(), w))
			expected := 2
			if size > maxInFlightBytes {
				expected = 1 // An oversized event can run alone, as before.
			}
			require.Len(t, submitted, expected)
			require.Equal(t, 3-expected, c.pendingDMLCount)
			require.Equal(t, int64(expected)*size, w.inFlightBytes)
			for completed := 0; completed < 3; {
				end := len(submitted)
				for _, row := range submitted[completed:end] {
					row.PostFlush()
				}
				completed = end
				require.NoError(t, c.flush(t.Context(), w))
			}
			require.Equal(t, uint64(200), w.checkpoint)
			require.Zero(t, memory.used())
		})
	}
}

func TestConductorFlushRetiresCoveredDDLReplica(t *testing.T) {
	memory := newMemoryUsage()
	c := &conductor{watermark: 200}
	record := newAck(memory, 10, nil)
	input := c.queueDDL(&event.DDLEvent{FinishedTs: 150}, record)
	input.event.awaitingDDL = true
	record.release()
	w := &writer{memory: memory}
	require.NoError(t, c.flush(t.Context(), w))
	require.True(t, record.ready())
	require.Empty(t, c.pendingDDL)
	require.Equal(t, uint64(200), w.checkpoint)
	require.Zero(t, memory.used())
}
