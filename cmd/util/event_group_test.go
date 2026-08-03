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

package util

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestEventsGroupAppendForceMergesExistingCommitTs(t *testing.T) {
	// Scenario:
	// 1) An upstream transaction (commitTs=100) is split into multiple messages.
	// 2) Due to sink retry/restart, a later transaction (commitTs=200) is observed first.
	// 3) A "late" fragment of the commitTs=100 transaction arrives afterwards.
	//
	// The EventsGroup must merge the late fragment into the existing commitTs=100 event,
	// instead of turning it into a second commitTs=100 item (which would split one upstream
	// transaction into multiple downstream transactions).
	group := NewEventsGroup(0, 1)

	newDMLEvent := func(commitTs uint64) *commonEvent.DMLEvent {
		return &commonEvent.DMLEvent{
			CommitTs: commitTs,
			RowTypes: []common.RowType{common.RowTypeUpdate},
			Rows:     chunk.NewChunkWithCapacity(nil, 0),
			Length:   0,
			TableInfo: common.NewTableInfo4Decoder("test", &timodel.TableInfo{
				ID:   100,
				Name: parser_model.NewCIStr("t"),
				Columns: []*timodel.ColumnInfo{
					{Name: parser_model.NewCIStr("a")},
				},
			}),
		}
	}

	group.Append(newDMLEvent(100), false)
	group.Append(newDMLEvent(200), false)
	group.Append(newDMLEvent(100), true)

	require.Equal(t, uint64(200), group.HighWatermark)

	var dst []*commonEvent.DMLEvent
	dst = group.ResolveInto(150, dst)
	require.Len(t, dst, 1)
	require.Equal(t, uint64(100), dst[0].CommitTs)
	require.Len(t, dst[0].RowTypes, 2)
}

func TestEventsGroupResolveIntoAppendsAndClearsResolvedMessages(t *testing.T) {
	// Scenario: A consumer resolves events by watermark/commit-ts and appends them into a downstream
	// batch slice. We must clear resolved messages in the group's backing array to avoid retaining
	// already-flushed events and causing unbounded memory growth.
	//
	// Steps:
	//  1. Append 3 events with increasing CommitTs.
	//  2. Call ResolveInto with resolve=2 and a nil dst.
	//  3. Verify (a) returned events are correct, (b) group keeps only the remaining event,
	//     (c) resolved messages in the original backing slice are cleared (nil'd).
	group := NewEventsGroup(0, 1)
<<<<<<< HEAD
	e1 := &commonEvent.DMLEvent{CommitTs: 1}
	e2 := &commonEvent.DMLEvent{CommitTs: 2}
	e3 := &commonEvent.DMLEvent{CommitTs: 3}
	group.Append(e1, false)
	group.Append(e2, false)
	group.Append(e3, false)

	// Keep a reference to the original slice header so we can validate that ResolveInto clears
	// the resolved prefix in-place (this is what prevents GC retention of flushed events).
	original := group.events
=======
	m1 := newTestDMLMessage(1)
	m2 := newTestDMLMessage(2)
	m3 := newTestDMLMessage(3)
	group.AppendMessage(m1)
	group.AppendMessage(m2)
	group.AppendMessage(m3)

	// Keep a reference to the original slice header so we can validate that ResolveInto clears
	// resolved messages in-place (this is what prevents GC retention of flushed events).
	original := group.messages
>>>>>>> af33cc193 (consumer: sort fallback DML before flush (#5824))

	var dst []*codeccommon.DMLMessage
	dst = group.ResolveInto(2, dst)

	require.Len(t, dst, 2)
	require.Same(t, m1, dst[0])
	require.Same(t, m2, dst[1])

	require.Len(t, group.messages, 1)
	require.Same(t, m3, group.messages[0])

	// The unresolved event is compacted to the front, and the tail is cleared so the group
	// doesn't keep flushed events alive via its backing array.
	require.Same(t, m3, original[0])
	require.Nil(t, original[1])
<<<<<<< HEAD
	require.Same(t, e3, original[2])
=======
	require.Nil(t, original[2])
>>>>>>> af33cc193 (consumer: sort fallback DML before flush (#5824))
}

func TestEventsGroupResolveIntoNoopWhenNothingResolved(t *testing.T) {
	// Scenario: resolveTs is behind all buffered events.
	// Expectation: ResolveInto should be a no-op (dst unchanged, group unchanged).
	group := NewEventsGroup(0, 1)
<<<<<<< HEAD
	e1 := &commonEvent.DMLEvent{CommitTs: 10}
	e2 := &commonEvent.DMLEvent{CommitTs: 20}
	group.Append(e1, false)
	group.Append(e2, false)
=======
	m1 := newTestDMLMessage(10)
	m2 := newTestDMLMessage(20)
	group.AppendMessage(m1)
	group.AppendMessage(m2)
>>>>>>> af33cc193 (consumer: sort fallback DML before flush (#5824))

	original := group.messages
	dst := make([]*codeccommon.DMLMessage, 0, 1)
	dst = group.ResolveInto(5, dst)

	require.Len(t, dst, 0)
	require.Len(t, group.messages, 2)
	require.Same(t, m1, group.messages[0])
	require.Same(t, m2, group.messages[1])

	// No prefix should be cleared because nothing was resolved.
	require.Same(t, m1, original[0])
	require.Same(t, m2, original[1])
}

func TestEventsGroupResolveIntoClearsAllWhenFullyResolved(t *testing.T) {
	// Scenario: resolveTs advances beyond all buffered events.
	// Expectation: group is emptied and all backing-array pointers for resolved events are cleared.
	group := NewEventsGroup(0, 1)
<<<<<<< HEAD
	e1 := &commonEvent.DMLEvent{CommitTs: 1}
	e2 := &commonEvent.DMLEvent{CommitTs: 2}
	group.Append(e1, false)
	group.Append(e2, false)
=======
	m1 := newTestDMLMessage(1)
	m2 := newTestDMLMessage(2)
	group.AppendMessage(m1)
	group.AppendMessage(m2)
>>>>>>> af33cc193 (consumer: sort fallback DML before flush (#5824))

	original := group.messages
	var dst []*codeccommon.DMLMessage
	dst = group.ResolveInto(100, dst)

	require.Len(t, dst, 2)
	require.Same(t, m1, dst[0])
	require.Same(t, m2, dst[1])

	require.Len(t, group.messages, 0)
	require.Nil(t, original[0])
	require.Nil(t, original[1])
}
<<<<<<< HEAD
=======

func TestEventsGroupResolveIntoSortsOutOfOrderResolvedMessages(t *testing.T) {
	group := NewEventsGroup(0, 1)
	m1 := newTestDMLMessage(20)
	m2 := newTestDMLMessage(10)
	m3 := newTestDMLMessage(30)
	group.AppendMessage(m1)
	group.AppendMessage(m2)
	group.AppendMessage(m3)

	original := group.messages
	var dst []*codeccommon.DMLMessage
	dst = group.ResolveInto(25, dst)

	require.Len(t, dst, 2)
	require.Same(t, m2, dst[0])
	require.Same(t, m1, dst[1])

	require.Len(t, group.messages, 1)
	require.Same(t, m3, group.messages[0])
	require.Same(t, m3, original[0])
	require.Nil(t, original[1])
	require.Nil(t, original[2])
}

func TestEventsGroupResolveIntoKeepsSameCommitTsStable(t *testing.T) {
	group := NewEventsGroup(0, 1)
	m1 := newTestDMLMessage(20)
	m2 := newTestDMLMessage(10)
	m3 := newTestDMLMessage(20)
	group.AppendMessage(m1)
	group.AppendMessage(m2)
	group.AppendMessage(m3)

	var dst []*codeccommon.DMLMessage
	dst = group.ResolveInto(20, dst)

	require.Len(t, dst, 3)
	require.Same(t, m2, dst[0])
	require.Same(t, m1, dst[1])
	require.Same(t, m3, dst[2])
	require.Empty(t, group.messages)
}

func TestEventsGroupGetAllMessagesSortsOutOfOrderMessages(t *testing.T) {
	group := NewEventsGroup(0, 1)
	m1 := newTestDMLMessage(20)
	m2 := newTestDMLMessage(10)
	m3 := newTestDMLMessage(30)
	group.AppendMessage(m1)
	group.AppendMessage(m2)
	group.AppendMessage(m3)

	messages := group.GetAllMessages()

	require.Len(t, messages, 3)
	require.Same(t, m2, messages[0])
	require.Same(t, m1, messages[1])
	require.Same(t, m3, messages[2])
	require.Empty(t, group.messages)
}

func TestAppendOrMergeDMLEventMergesSameCommitTs(t *testing.T) {
	var flushed []int
	e1 := newTestDMLEvent(10, common.RowTypeInsert)
	e1.AddPostFlushFunc(func() { flushed = append(flushed, 1) })
	e2 := newTestDMLEvent(10, common.RowTypeDelete)
	e2.AddPostFlushFunc(func() { flushed = append(flushed, 2) })

	events := AppendOrMergeDMLEvent(nil, e1)
	events = AppendOrMergeDMLEvent(events, e2)

	require.Len(t, events, 1)
	require.Same(t, e1, events[0])
	require.Equal(t, int32(2), events[0].Length)
	require.Equal(t, []common.RowType{common.RowTypeInsert, common.RowTypeDelete}, events[0].RowTypes)

	events[0].PostFlush()
	require.Equal(t, []int{1, 2}, flushed)
}

func TestAppendOrMergeDMLEventAppendsDifferentCommitTs(t *testing.T) {
	e1 := newTestDMLEvent(10, common.RowTypeInsert)
	e2 := newTestDMLEvent(20, common.RowTypeDelete)

	events := AppendOrMergeDMLEvent(nil, e1)
	events = AppendOrMergeDMLEvent(events, e2)

	require.Len(t, events, 2)
	require.Same(t, e1, events[0])
	require.Same(t, e2, events[1])
}
>>>>>>> af33cc193 (consumer: sort fallback DML before flush (#5824))
