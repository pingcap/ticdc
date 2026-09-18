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
	"math"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/cmd/util"
	sinkmock "github.com/pingcap/ticdc/downstreamadapter/sink/mock"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

// newPipelineTestMessage builds a spillable event of one row for the given
// commit-ts, mirroring the events the read loop produces.
func newPipelineTestMessage(tableID int64, commitTs uint64) *codecCommon.DMLMessage {
	message := codecCommon.NewDMLMessageFromEvent(&commonEvent.DMLEvent{
		PhysicalTableID: tableID,
		CommitTs:        commitTs,
		RowTypes:        []common.RowType{common.RowTypeInsert},
		Rows:            chunk.NewChunkWithCapacity(nil, 0),
		TableInfo: &common.TableInfo{
			TableName: common.TableName{Schema: "test", Table: "t", TableID: tableID},
		},
	})
	data := codecCommon.NewDMLMessageData(nil, nil, func([]byte) ([]*codecCommon.DMLMessage, error) {
		return []*codecCommon.DMLMessage{message}, nil
	})
	data.AttachDMLMessage(message)
	return message
}

// TestPipelineAppliesLateEventAfterTheInFlightBatch pins the ordering contract of
// the parallel resolve path: every appended event reaches the sink exactly once
// per group, and an event appended while a batch is in flight is applied after
// that batch instead of being dropped with the applied range.
func TestPipelineAppliesLateEventAfterTheInFlightBatch(t *testing.T) {
	const tableID = 1
	ctrl := gomock.NewController(t)
	s := sinkmock.NewMockSink(ctrl)

	var (
		mu      sync.Mutex
		applied []uint64
	)
	inFlight := make(chan struct{}, 1)
	release := make(chan struct{})
	s.EXPECT().AddDMLEvent(gomock.Any()).DoAndReturn(func(event *commonEvent.DMLEvent) {
		select {
		case inFlight <- struct{}{}:
		default:
		}
		<-release
		mu.Lock()
		applied = append(applied, event.GetCommitTs())
		mu.Unlock()
		event.PostFlush()
	}).AnyTimes()

	w := newTestWriter(t, &writer{
		progresses: []*partitionProgress{{
			partition:   0,
			eventsGroup: make(map[int64]*util.EventsGroup),
		}},
		mysqlSink:  s,
		spillStore: util.NewSpillStore(),
	})

	group := w.progresses[0].group(tableID, func() *util.EventsGroup {
		return util.NewEventsGroup(0, tableID, w.getSpillStore())
	})

	const batchSize = 40
	for commitTs := uint64(1); commitTs <= batchSize; commitTs++ {
		require.NoError(t, group.AppendMessage(newPipelineTestMessage(tableID, commitTs)))
	}
	w.globalWatermarkValue.Store(math.MaxUint64)
	w.pipeline.request()

	// The batch is claimed and handed to the sink, and it is still in flight.
	select {
	case <-inFlight:
	case <-time.After(10 * time.Second):
		require.Fail(t, "the resolve pipeline did not submit the batch")
	}

	// This event is older than the in-flight batch end, so it must not be
	// removed together with the applied range.
	lateCommitTs := uint64(25)
	require.NoError(t, group.AppendMessage(newPipelineTestMessage(tableID, lateCommitTs)))
	close(release)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(applied) == batchSize+1
	}, 10*time.Second, 10*time.Millisecond, "every appended event must be applied")

	// The barrier drains the resolve path: once it returns, the sink saw every
	// submitted event and no batch is in flight.
	resume := w.pipeline.pause(t.Context())
	mu.Lock()
	require.Len(t, applied, batchSize+1)
	mu.Unlock()
	resume()

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, batchSize+1, len(applied))
	for i := 1; i <= batchSize; i++ {
		require.Equal(t, uint64(i), applied[i-1], "the in-flight batch keeps commit-ts order")
	}
	require.Equal(t, lateCommitTs, applied[batchSize], "the late event is applied after the batch")
	require.Equal(t, int64(batchSize+1), w.getSpillStore().Stats().AppliedEventCount)
}
