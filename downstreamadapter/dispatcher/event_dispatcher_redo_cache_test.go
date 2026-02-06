package dispatcher

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

// TestEventDispatcherRedoGating verifies redo gating enforces "commitTs <= redoGlobalTs" before emitting events.
// This matters because emitting events ahead of redo catch-up can break downstream correctness and recovery semantics.
// The key scenario is a DML batch with commitTs greater than redoGlobalTs that must be cached and replayed later.
func TestEventDispatcherRedoGating(t *testing.T) {
	var redoGlobalTs atomic.Uint64
	redoGlobalTs.Store(50)

	mockSink := sink.NewMockSink(common.MysqlSinkType)
	txnAtomicity := config.DefaultAtomicityLevel()
	sharedInfo := NewSharedInfo(
		common.NewChangefeedID4Test("test", "dispatcher_redo_gating"),
		"system",
		false,
		false,
		nil,
		nil,
		nil, // no syncpoint config is required for this unit test
		&txnAtomicity,
		false,
		make(chan TableSpanStatusWithSeq, 16),
		make(chan *heartbeatpb.TableSpanBlockStatus, 16),
		make(chan error, 1),
	)
	t.Cleanup(sharedInfo.Close)

	tableSpan, err := getCompleteTableSpan(getTestingKeyspaceID())
	require.NoError(t, err)
	dispatcher := NewEventDispatcher(
		common.NewDispatcherID(),
		tableSpan,
		0,
		1,
		NewSchemaIDToDispatchers(),
		false,
		false,
		0,
		mockSink,
		sharedInfo,
		true, // redoEnable
		&redoGlobalTs,
	)

	// Avoid side effects on the dispatcher status dynamic stream in this unit test.
	dispatcher.componentStatus.Set(heartbeatpb.ComponentState_Working)

	dml := &commonEvent.DMLEvent{
		StartTs:  1,
		CommitTs: 100,
		Length:   1,
	}
	from := node.NewID()

	var wakeCount atomic.Int32
	wakeCallback := func() {
		wakeCount.Add(1)
	}

	// The event must be cached when redoGlobalTs < commitTs.
	block := dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&from, dml)}, wakeCallback)
	require.True(t, block)
	require.Len(t, mockSink.GetDMLs(), 0)
	require.Equal(t, int32(0), wakeCount.Load())

	// Calling HandleCacheEvents too early must not leak events to the sink.
	dispatcher.HandleCacheEvents()
	require.Len(t, mockSink.GetDMLs(), 0)
	require.Equal(t, int32(0), wakeCount.Load())

	// Once redoGlobalTs catches up, the cached events are replayed into the sink.
	redoGlobalTs.Store(100)
	dispatcher.HandleCacheEvents()
	require.Len(t, mockSink.GetDMLs(), 1)
	require.Equal(t, int32(0), wakeCount.Load())

	// Wake the upstream only after the sink flush completes.
	require.Eventually(t, func() bool {
		mockSink.FlushDMLs()
		return wakeCount.Load() == int32(1)
	}, time.Second, 10*time.Millisecond)
	require.Len(t, mockSink.GetDMLs(), 0)
}
