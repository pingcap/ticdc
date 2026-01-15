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

package dispatcher

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

type errorBlockEventSink struct {
	sinkType common.SinkType
	writeErr error

	writeCalledOnce sync.Once
	writeCalled     chan struct{}
}

var _ sink.Sink = (*errorBlockEventSink)(nil)

func newErrorBlockEventSink(sinkType common.SinkType, writeErr error) *errorBlockEventSink {
	return &errorBlockEventSink{
		sinkType:    sinkType,
		writeErr:    writeErr,
		writeCalled: make(chan struct{}),
	}
}

func (s *errorBlockEventSink) SinkType() common.SinkType {
	return s.sinkType
}

func (s *errorBlockEventSink) IsNormal() bool {
	return true
}

func (s *errorBlockEventSink) AddDMLEvent(*commonEvent.DMLEvent) {}

func (s *errorBlockEventSink) WriteBlockEvent(commonEvent.BlockEvent) error {
	s.writeCalledOnce.Do(func() {
		close(s.writeCalled)
	})
	return s.writeErr
}

func (s *errorBlockEventSink) AddCheckpointTs(uint64) {}

func (s *errorBlockEventSink) SetTableSchemaStore(*commonEvent.TableSchemaStore) {}

func (s *errorBlockEventSink) Close(bool) {}

func (s *errorBlockEventSink) Run(context.Context) error {
	return nil
}

func TestPendingACKCountRollbackOnBlockEventWriteError(t *testing.T) {
	// Scenario: For the table-trigger dispatcher, schedule-related non-blocking DDLs (add/drop tables)
	// increment pendingACKCount before writing downstream. If the downstream write fails before a resend
	// task is created, the maintainer will never ACK it, so pendingACKCount must be rolled back to avoid
	// stalling future DB/All block events behind a leaked counter.
	//
	// Steps:
	// 1) Create a table-trigger dispatcher with a sink that always fails WriteBlockEvent.
	// 2) Send a non-blocking DDL event that has NeedDroppedTables set (schedule-related).
	// 3) Wait until the sink write is attempted and the dispatcher reports the error.
	// 4) Assert pendingACKCount is rolled back and no resend task / block status is created.
	writeErr := errors.New("injected sink write error")
	mockSink := newErrorBlockEventSink(common.MysqlSinkType, writeErr)
	dispatcher := newDispatcherForTest(mockSink, common.KeyspaceDDLSpan(getTestingKeyspaceID()))
	t.Cleanup(dispatcher.sharedInfo.Close)

	require.True(t, dispatcher.IsTableTriggerEventDispatcher())

	ddlEvent := &commonEvent.DDLEvent{
		StartTs:    1,
		FinishedTs: 2,
		// InfluenceTypeNormal + single table => should not enter the blocking DDL path.
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		// Non-nil NeedDroppedTables makes it schedule-related and triggers pendingACKCount tracking.
		NeedDroppedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{1},
		},
	}
	require.False(t, dispatcher.shouldBlock(ddlEvent))

	nodeID := node.NewID()
	dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, ddlEvent)}, func() {})

	select {
	case <-mockSink.writeCalled:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for block event downstream write attempt")
	}

	select {
	case err := <-dispatcher.sharedInfo.errCh:
		require.ErrorIs(t, err, writeErr)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for dispatcher error")
	}

	require.Equal(t, int64(0), dispatcher.pendingACKCount.Load())
	require.Equal(t, 0, dispatcher.resendTaskMap.Len())

	select {
	case msg := <-dispatcher.sharedInfo.blockStatusesChan:
		t.Fatalf("unexpected block status sent after downstream write error: %+v", msg)
	default:
	}
}
