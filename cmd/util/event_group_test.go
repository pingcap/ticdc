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
			CommitTs:  commitTs,
			RowTypes:  []common.RowType{common.RowTypeUpdate},
			Rows:      chunk.NewChunkWithCapacity(nil, 0),
			Length:    0,
			TableInfo: &common.TableInfo{},
		}
	}

	group.Append(newDMLEvent(100), false)
	group.Append(newDMLEvent(200), false)
	group.Append(newDMLEvent(100), true)

	require.Equal(t, uint64(200), group.HighWatermark)

	resolved := group.Resolve(150)
	require.Len(t, resolved, 1)
	require.Equal(t, uint64(100), resolved[0].CommitTs)
	require.Len(t, resolved[0].RowTypes, 2)
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
	e1 := &commonEvent.DMLEvent{CommitTs: 1}
	e2 := &commonEvent.DMLEvent{CommitTs: 2}
	e3 := &commonEvent.DMLEvent{CommitTs: 3}
	group.Append(e1, false)
	group.Append(e2, false)
	group.Append(e3, false)

	// Keep a reference to the original slice header so we can validate that ResolveInto clears
	// the resolved prefix in-place (this is what prevents GC retention of flushed events).
	original := group.events

	var dst []*commonEvent.DMLEvent
	dst = group.ResolveInto(2, dst)

	require.Len(t, dst, 2)
	require.Same(t, e1, dst[0])
	require.Same(t, e2, dst[1])

	require.Len(t, group.events, 1)
	require.Same(t, e3, group.events[0])

	// The resolved prefix must be nil so the group doesn't keep flushed events alive via its
	// backing array (classic Go slice memory retention pitfall).
	require.Nil(t, original[0])
	require.Nil(t, original[1])
	require.Same(t, e3, original[2])
}

func TestEventsGroupResolveIntoNoopWhenNothingResolved(t *testing.T) {
	// Scenario: resolveTs is behind all buffered events.
	// Expectation: ResolveInto should be a no-op (dst unchanged, group unchanged).
	group := NewEventsGroup(0, 1)
	e1 := &commonEvent.DMLEvent{CommitTs: 10}
	e2 := &commonEvent.DMLEvent{CommitTs: 20}
	group.Append(e1, false)
	group.Append(e2, false)

	original := group.events
	dst := make([]*commonEvent.DMLEvent, 0, 1)
	dst = group.ResolveInto(5, dst)

	require.Len(t, dst, 0)
	require.Len(t, group.events, 2)
	require.Same(t, e1, group.events[0])
	require.Same(t, e2, group.events[1])

	// No prefix should be cleared because nothing was resolved.
	require.Same(t, e1, original[0])
	require.Same(t, e2, original[1])
}

func TestEventsGroupResolveIntoClearsAllWhenFullyResolved(t *testing.T) {
	// Scenario: resolveTs advances beyond all buffered events.
	// Expectation: group is emptied and all backing-array pointers for resolved events are cleared.
	group := NewEventsGroup(0, 1)
	e1 := &commonEvent.DMLEvent{CommitTs: 1}
	e2 := &commonEvent.DMLEvent{CommitTs: 2}
	group.Append(e1, false)
	group.Append(e2, false)

	original := group.events
	var dst []*commonEvent.DMLEvent
	dst = group.ResolveInto(100, dst)

	require.Len(t, dst, 2)
	require.Same(t, e1, dst[0])
	require.Same(t, e2, dst[1])

	require.Len(t, group.events, 0)
	require.Nil(t, original[0])
	require.Nil(t, original[1])
}
