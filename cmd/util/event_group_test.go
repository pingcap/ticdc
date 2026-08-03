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

func newTestDMLMessage(commitTs uint64) *codeccommon.DMLMessage {
	return codeccommon.NewDMLMessage(1, "test", "t", commitTs, common.RowTypeInsert, nil)
}

func newTestDMLEvent(commitTs uint64, rowTypes ...common.RowType) *commonEvent.DMLEvent {
	return &commonEvent.DMLEvent{
		PhysicalTableID: 1,
		CommitTs:        commitTs,
		Length:          int32(len(rowTypes)),
		RowTypes:        rowTypes,
		Rows:            chunk.NewChunkWithCapacity(nil, 0),
	}
}

func TestEventsGroupResolveIntoAppendsAndClearsResolvedPrefix(t *testing.T) {
	// Scenario: A consumer resolves a prefix of events by watermark/commit-ts and appends them
	// into a downstream batch slice. We must clear the resolved prefix in the group's backing
	// array to avoid retaining already-flushed events and causing unbounded memory growth.
	//
	// Steps:
	//  1. Append 3 events with increasing CommitTs.
	//  2. Call ResolveInto with resolve=2 and a nil dst.
	//  3. Verify (a) returned events are correct, (b) group keeps only the remaining event,
	//     (c) the resolved prefix in the original backing slice is cleared (nil'd).
	group := NewEventsGroup(0, 1)
	m1 := newTestDMLMessage(1)
	m2 := newTestDMLMessage(2)
	m3 := newTestDMLMessage(3)
	group.AppendMessage(m1, false)
	group.AppendMessage(m2, false)
	group.AppendMessage(m3, false)

	// Keep a reference to the original slice header so we can validate that ResolveInto clears
	// the resolved prefix in-place (this is what prevents GC retention of flushed events).
	original := group.messages

	var dst []*codeccommon.DMLMessage
	dst = group.ResolveInto(2, dst)

	require.Len(t, dst, 2)
	require.Same(t, m1, dst[0])
	require.Same(t, m2, dst[1])

	require.Len(t, group.messages, 1)
	require.Same(t, m3, group.messages[0])

	// The resolved prefix must be nil so the group doesn't keep flushed events alive via its
	// backing array (classic Go slice memory retention pitfall).
	require.Nil(t, original[0])
	require.Nil(t, original[1])
	require.Same(t, m3, original[2])
}

func TestEventsGroupResolveIntoNoopWhenNothingResolved(t *testing.T) {
	// Scenario: resolveTs is behind all buffered events.
	// Expectation: ResolveInto should be a no-op (dst unchanged, group unchanged).
	group := NewEventsGroup(0, 1)
	m1 := newTestDMLMessage(10)
	m2 := newTestDMLMessage(20)
	group.AppendMessage(m1, false)
	group.AppendMessage(m2, false)

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
	m1 := newTestDMLMessage(1)
	m2 := newTestDMLMessage(2)
	group.AppendMessage(m1, false)
	group.AppendMessage(m2, false)

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
