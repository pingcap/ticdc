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

package dispatcher

import (
	"sync"
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestScanPauseFollowsRedoWaitLifecycle(t *testing.T) {
	testSink := newDispatcherTestSink(t, common.MysqlSinkType)
	d := newDispatcherForTest(testSink.Sink(), getUncompleteTableSpan())
	defer d.Remove()
	d.redoGlobalTs.Store(50)
	from := node.NewID()
	batch := func(ts uint64) []DispatcherEvent {
		return []DispatcherEvent{NewDispatcherEvent(&from, commonEvent.NewResolvedEvent(ts, d.id, 1))}
	}
	// Redo-disabled dispatchers do not wait even when the watermark is behind.
	require.False(t, d.HandleEvents(batch(60), func() {}))
	require.False(t, d.ShouldPauseScan())
	d.redoEnable = true
	require.False(t, d.ShouldPauseScan(), "a low watermark alone must not pause scanning")
	wakes := 0
	require.True(t, d.HandleEvents(batch(100), func() {
		wakes++
		require.False(t, d.ShouldPauseScan(), "clear the old wait before Wake")
		// Wake can immediately begin a new wait. Completion of the old batch
		// must not clear this new pause.
		require.True(t, d.HandleEvents(batch(200), func() {
			wakes++
			require.False(t, d.ShouldPauseScan())
		}))
	}))
	require.True(t, d.ShouldPauseScan())
	d.redoGlobalTs.Store(90)
	d.HandleCacheEvents()
	require.True(t, d.ShouldPauseScan(), "an insufficient watermark must retain the wait")
	require.Zero(t, wakes)
	require.Equal(t, uint64(60), d.GetResolvedTs())

	d.redoGlobalTs.Store(100)
	require.False(t, d.ShouldPauseScan(), "quota can recover as soon as the watermark catches up")
	d.HandleCacheEvents()
	require.Equal(t, 1, wakes)
	require.Equal(t, uint64(100), d.GetResolvedTs())
	require.True(t, d.ShouldPauseScan(), "the next cached batch still needs redo progress")
	d.redoGlobalTs.Store(201)
	d.HandleCacheEvents()
	require.Equal(t, 2, wakes)
	require.Equal(t, uint64(200), d.GetResolvedTs())
	require.False(t, d.ShouldPauseScan())
	d.HandleCacheEvents()
	require.Equal(t, 2, wakes, "recovery must not replay a batch twice")

	// A completed redo wait must not mask a subsequent syncpoint barrier.
	d.blockEventStatus.setBlockEvent(commonEvent.NewSyncPointEvent(d.id, 200, 1, 1), heartbeatpb.BlockStage_WAITING)
	require.True(t, d.ShouldPauseScan())
	d.blockEventStatus.clear()
	require.False(t, d.ShouldPauseScan())
}

func TestRedoScanPauseConcurrentRemoval(t *testing.T) {
	testSink := newDispatcherTestSink(t, common.MysqlSinkType)
	d := newDispatcherForTest(testSink.Sink(), getUncompleteTableSpan())
	d.redoEnable = true
	d.redoGlobalTs.Store(50)
	from := node.NewID()
	batch := []DispatcherEvent{NewDispatcherEvent(&from, commonEvent.NewResolvedEvent(100, d.id, 1))}
	require.True(t, d.HandleEvents(batch, func() { t.Error("insufficient redo progress must not wake the path") }))
	require.True(t, d.ShouldPauseScan())

	// The watermark retry, congestion snapshot and removal run on different
	// goroutines. A retry racing with removal must not republish an old wait.
	var workers sync.WaitGroup
	start := make(chan struct{})
	for _, action := range []func(){d.HandleCacheEvents, d.Remove, func() { d.ShouldPauseScan() }} {
		workers.Add(1)
		go func(action func()) {
			defer workers.Done()
			<-start
			action()
		}(action)
	}
	close(start)
	workers.Wait()
	require.False(t, d.ShouldPauseScan())
	d.HandleCacheEvents()
	require.False(t, d.ShouldPauseScan(), "a removed dispatcher must not reenter a redo wait")

	replacement := newDispatcherForTest(testSink.Sink(), getUncompleteTableSpan())
	defer replacement.Remove()
	replacement.redoEnable = true
	replacement.redoGlobalTs.Store(50)
	require.False(t, replacement.ShouldPauseScan(), "a new dispatcher must derive its own pause state")
}
