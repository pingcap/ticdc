// Copyright 2025 PingCAP, Inc.
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

package dispatchermanager

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

// mockKafkaSink implements the sink.Sink interface with a blocking AddCheckpointTs method
type mockKafkaSink struct {
	cancel         context.CancelFunc
	ctx            context.Context
	checkpointChan chan uint64
	isNormal       *atomic.Bool
	mu             sync.Mutex
}

func newMockKafkaSink(ctx context.Context, cancel context.CancelFunc) *mockKafkaSink {
	return &mockKafkaSink{
		cancel:         cancel,
		ctx:            ctx,
		checkpointChan: make(chan uint64),
		isNormal:       atomic.NewBool(true),
	}
}

func (m *mockKafkaSink) SinkType() common.SinkType {
	return common.KafkaSinkType
}

func (m *mockKafkaSink) IsNormal() bool {
	return true
}

func (m *mockKafkaSink) AddDMLEvent(event *commonEvent.DMLEvent) {
	// No-op for this test
}

func (m *mockKafkaSink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	// No-op for this test
	return nil
}

// AddCheckpointTs simulates the blocking behavior when sink is closed
func (m *mockKafkaSink) AddCheckpointTs(ts uint64) {
	select {
	case m.checkpointChan <- ts:
	case <-m.ctx.Done():
		return
	}
}

func (m *mockKafkaSink) SetTableSchemaStore(tableSchemaStore *commonEvent.TableSchemaStore) {
	// No-op for this test
}

func (m *mockKafkaSink) Close(removeChangefeed bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.isNormal.Store(false)
	// Don't close the channel to simulate the real bug where channel becomes orphaned
}

func (m *mockKafkaSink) Run(ctx context.Context) error {
	// Simulate consuming from checkpoint channel
	for {
		select {
		case <-ctx.Done():
			log.Info("mockKafkaSink context done")
			return ctx.Err()
		case ts := <-m.checkpointChan:
			// Simulate processing checkpoint
			_ = ts
		}
	}
}

// simulateCloseSink simulates closing the sink (e.g., when path is removed)
func (m *mockKafkaSink) CloseSinkAndCancelContext() {
	m.Close(false)
	m.cancel()
}

func TestCheckpointTsMessageHandlerDeadlock(t *testing.T) {
	t.Parallel()

	// Create context for the test
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create mock sink
	mockSink := newMockKafkaSink(ctx, cancel)

	// Start the sink's Run method in background to consume checkpoint messages
	sinkCtx, _ := context.WithCancel(ctx)
	go func() {
		_ = mockSink.Run(sinkCtx)
	}()

	// Create a mock DispatcherManager
	dispatcherManager := &DispatcherManager{
		sink:                        mockSink,
		tableTriggerEventDispatcher: &dispatcher.EventDispatcher{}, // Non-nil to pass the check
	}

	// Create CheckpointTsMessage
	changefeedID := &heartbeatpb.ChangefeedID{
		Keyspace: "test-namespace",
		Name:     "test-changefeed",
	}

	checkpointTsMessage := NewCheckpointTsMessage(&heartbeatpb.CheckpointTsMessage{
		ChangefeedID: changefeedID,
		CheckpointTs: 12345,
	})

	// Create handler
	handler := &CheckpointTsMessageHandler{}

	// Step 1: Normal operation should work
	t.Run("normal_operation", func(t *testing.T) {
		// This should complete quickly
		done := make(chan bool, 1)
		go func() {
			blocking := handler.Handle(dispatcherManager, checkpointTsMessage)
			require.False(t, blocking, "Handler should not return blocking=true")
			done <- true
		}()

		select {
		case <-done:
			// Success
		case <-time.After(1 * time.Second):
			t.Fatal("Normal operation took too long, unexpected")
		}
	})

	// Step 2: Simulate the deadlock scenario
	t.Run("deadlock_scenario", func(t *testing.T) {
		// Create a new mock sink for this test to avoid interference
		deadlockMockSink := newMockKafkaSink(ctx, cancel)

		// Create a new dispatcher manager with the deadlock sink
		deadlockDispatcherManager := &DispatcherManager{
			sink:                        deadlockMockSink,
			tableTriggerEventDispatcher: &dispatcher.EventDispatcher{},
		}

		// Close the sink but does not cancel the context, so the AddCheckpointTs will block forever
		deadlockMockSink.Close(false)

		// Now try to send a checkpoint message
		// This should block because there's no consumer for the channel
		done := make(chan bool, 1)
		go func() {
			// This should block indefinitely
			handler.Handle(deadlockDispatcherManager, checkpointTsMessage)
			done <- true
		}()

		select {
		case <-done:
			t.Fatal("Handler completed unexpectedly - deadlock was not reproduced")
		case <-time.After(1 * time.Second):
			// Expected: the handler should be blocked
			t.Log("Successfully reproduced the deadlock: handler is blocked in AddCheckpointTs")
		}
	})

	// Step 3: close the sink and cancel the context, so the AddCheckpointTs will return
	t.Run("deadlock_resolve_scenario", func(t *testing.T) {
		// Create a new mock sink for this test to avoid interference
		deadlockMockSink := newMockKafkaSink(ctx, cancel)

		// Create a new dispatcher manager with the deadlock sink
		deadlockDispatcherManager := &DispatcherManager{
			sink:                        deadlockMockSink,
			tableTriggerEventDispatcher: &dispatcher.EventDispatcher{},
		}

		// Close the sink but does not cancel the context, so the AddCheckpointTs will block forever
		deadlockMockSink.CloseSinkAndCancelContext()

		// Now try to send a checkpoint message
		// This should not block because the context is canceled
		done := make(chan bool, 1)
		go func() {
			// This should not block
			handler.Handle(deadlockDispatcherManager, checkpointTsMessage)
			done <- true
		}()

		select {
		case <-done:
			// Expected: the handler should complete without blocking.
			t.Log("Handler completed normally")
		case <-time.After(1 * time.Second):
			t.Fatal("deadlock: handler is blocked in AddCheckpointTs")
		}
	})
}
