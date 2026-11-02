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

package eventcollector

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func sendLargeBatchForReclamation(ch chan<- *messaging.TargetMessage, from node.ID, id common.DispatcherID, collected chan<- struct{}) {
	batch := &commonEvent.BatchDMLEvent{
		DMLEvents: []*commonEvent.DMLEvent{{DispatcherID: id, Seq: 3, Epoch: 1, CommitTs: 101}},
		RawRows:   make([]byte, 1<<20),
	}
	runtime.SetFinalizer(batch, func(*commonEvent.BatchDMLEvent) { close(collected) })
	ch <- newMessage(from, batch)
}

func TestReclaimedBatchDoesNotRetainTargetMessage(t *testing.T) {
	c := &EventCollector{
		ds: NewEventDynamicStream(false), redoDs: NewEventDynamicStream(true),
		metricDispatcherReceivedKVEventCount: prometheus.NewCounter(prometheus.CounterOpts{Name: "test_reclaimed_batch_received_events"}),
	}
	defer c.ds.Close()
	defer c.redoDs.Close()
	id := common.NewDispatcherID()
	from := node.ID("broker")
	blocked := make(chan func(), 1)
	target := &mockEventDispatcher{
		id: id, changefeedID: common.NewChangefeedID4Test("default", "reclamation-retention"),
		tableSpan: &heartbeatpb.TableSpan{TableID: 1}, checkpointTs: 99,
		handleEvents: func(events []dispatcher.DispatcherEvent, wake func()) bool {
			blocked <- wake
			return true
		},
	}
	messages := make(chan *messaging.TargetMessage, 1)
	stat := newDispatcherStatInternal(target, c, from, func(msg *messaging.TargetMessage) { messages <- msg }, nil)
	stat.currentEpoch.Store(newDispatcherEpochState(1, 1, 99))
	markSessionReceiving(stat.session, from)
	c.dispatcherMap.Store(id, stat)
	require.NoError(t, c.ds.AddPath(id, stat, dynstream.NewAreaSettingsWithMaxPendingSize(1<<20, dynstream.MemoryControlForEventCollector, "test")))
	ctx, cancel := context.WithCancel(context.Background())
	feedbackDone := make(chan error, 1)
	go func() { feedbackDone <- c.processDSFeedback(ctx) }()
	defer func() {
		cancel()
		require.ErrorIs(t, <-feedbackDone, context.Canceled)
	}()
	runCtx, stopRun := context.WithCancel(context.Background())
	defer stopRun()
	inCh := make(chan *messaging.TargetMessage, 2)
	runDone := make(chan error, 1)
	go func() { runDone <- c.runDispatchMessage(runCtx, inCh, common.DefaultMode) }()
	inCh <- newMessage(from, commonEvent.NewSyncPointEvent(id, 100, 2, 1))
	var wake func()
	select {
	case wake = <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("syncpoint did not block")
	}
	collected := make(chan struct{})
	sendLargeBatchForReclamation(inCh, from, id, collected)
	used := func() int64 { return c.ds.GetMetrics().MemoryControl.AreaMemoryMetrics[0].MemoryUsage() }
	require.Eventually(t, func() bool { return used() >= 1<<20 }, 5*time.Second, time.Millisecond)
	stopRun()
	require.ErrorIs(t, <-runDone, context.Canceled)
	require.Eventually(t, func() bool {
		c.ds.CheckMemoryPressure()
		return used() < 1000
	}, 10*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		runtime.GC()
		select {
		case <-collected:
			return true
		default:
			return false
		}
	}, 5*time.Second, 20*time.Millisecond, "reclaimed payload remains reachable while the drop marker is queued")
	require.Empty(t, messages)
	wake()
	select {
	case msg := <-messages:
		req := msg.Message[0].(*messaging.DispatcherRequest)
		require.Equal(t, eventpb.ActionType_ACTION_TYPE_RESET, req.ActionType)
	case <-time.After(5 * time.Second):
		t.Fatal("retained drop notification did not reset")
	}
}

func TestReclaimedDataResetsAfterWakeWithoutNewInput(t *testing.T) {
	for _, mode := range []struct {
		name  string
		value int64
	}{{"main", common.DefaultMode}, {"redo", common.RedoMode}} {
		t.Run(mode.name, func(t *testing.T) {
			c := &EventCollector{ds: NewEventDynamicStream(false), redoDs: NewEventDynamicStream(true)}
			defer c.ds.Close()
			defer c.redoDs.Close()
			ds := c.getDynamicStream(mode.value)
			blocked := make(chan func(), 1)
			target := &mockEventDispatcher{
				mode:         mode.value,
				id:           common.NewDispatcherID(),
				changefeedID: common.NewChangefeedID4Test("default", "reclamation"),
				tableSpan:    &heartbeatpb.TableSpan{TableID: 1},
				checkpointTs: 99,
				handleEvents: func(events []dispatcher.DispatcherEvent, wake func()) bool {
					blocked <- wake
					return true
				},
			}
			messages := make(chan *messaging.TargetMessage, 4)
			stat := newDispatcherStatInternal(target, c, "broker", func(msg *messaging.TargetMessage) { messages <- msg }, nil)
			stat.currentEpoch.Store(newDispatcherEpochState(1, 1, 99))
			markSessionReceiving(stat.session, "broker")
			c.dispatcherMap.Store(target.id, stat)
			require.NoError(t, ds.AddPath(target.id, stat, dynstream.NewAreaSettingsWithMaxPendingSize(1<<20, dynstream.MemoryControlForEventCollector, "test")))
			ctx, cancel := context.WithCancel(context.Background())
			feedbackDone := make(chan error, 1)
			go func() { feedbackDone <- c.processDSFeedback(ctx) }()
			defer func() {
				cancel()
				require.ErrorIs(t, <-feedbackDone, context.Canceled)
			}()
			from := node.ID("broker")
			push := func(e commonEvent.Event) { ds.Push(target.id, dispatcher.NewDispatcherEvent(&from, e)) }
			if common.IsRedoMode(mode.value) {
				// Redo can block on sink enqueue; it does not wait for syncpoints.
				push(&commonEvent.DMLEvent{DispatcherID: target.id, Seq: 2, Epoch: 1, CommitTs: 100, ApproximateSize: 100})
			} else {
				push(commonEvent.NewSyncPointEvent(target.id, 100, 2, 1))
			}
			var wake func()
			select {
			case wake = <-blocked:
			case <-time.After(5 * time.Second):
				t.Fatal("active batch not handled")
			}
			push(&commonEvent.DMLEvent{DispatcherID: target.id, Seq: 3, Epoch: 1, CommitTs: 101, ApproximateSize: 900000})
			resolved := commonEvent.NewResolvedEvent(102, target.id, 1)
			push(resolved)
			used := func() int64 { return ds.GetMetrics().MemoryControl.AreaMemoryMetrics[0].MemoryUsage() }
			require.Eventually(t, func() bool { return used() >= 900000 }, 5*time.Second, time.Millisecond)
			// No new input drives recovery: periodic pressure checks and feedback alone
			// must reclaim the blocked pending data in either stream.
			require.Eventually(t, func() bool {
				ds.CheckMemoryPressure()
				return used() < 1000
			}, 10*time.Second, 10*time.Millisecond)
			require.Empty(t, messages, "reclamation cannot reset or release an active batch")
			// Upstream sends nothing further: the retained DropEvent alone must reset.
			wake()
			select {
			case msg := <-messages:
				req := msg.Message[0].(*messaging.DispatcherRequest)
				require.Equal(t, eventpb.ActionType_ACTION_TYPE_RESET, req.ActionType)
				require.Equal(t, uint64(2), req.Epoch)
				require.Equal(t, uint64(99), req.StartTs)
			case <-time.After(5 * time.Second):
				t.Fatal("retained drop notification did not reset")
			}
			require.Eventually(t, func() bool { return used() == 0 }, 5*time.Second, time.Millisecond)
			// A delayed marker from the old epoch must not reset the new session.
			ds.Close()
			stat.handleDropEvent(dispatcher.NewDispatcherEvent(&from, commonEvent.NewDropEvent(target.id, 4, 1, 103)))
			require.Empty(t, messages)
			require.Equal(t, uint64(2), stat.loadCurrentEpochState().epoch)
		})
	}
}

// A fast table waiting at a syncpoint retains an active batch while its
// pending DML consumes the shared quota. Reclamation must advertise the freed
// capacity to the lagging table (or its redo producer), then the barrier can
// finish and the reclaimed fast path can reset without another input event.
func TestSyncpointReclamationRestoresLaggingScanQuota(t *testing.T) {
	for _, withRedo := range []bool{false, true} {
		name := "main"
		if withRedo {
			name = "redo"
		}
		t.Run(name, func(t *testing.T) {
			c := &EventCollector{ds: NewEventDynamicStream(false), redoDs: NewEventDynamicStream(true)}
			defer c.ds.Close()
			defer c.redoDs.Close()
			cf := common.NewChangefeedID4Test("default", "syncpoint-reclaim-"+name)
			cfStat := newChangefeedStat(cf)
			c.changefeedMap.Store(cf.ID(), cfStat)
			from := node.ID("broker")
			blocked := make(chan func(), 1)
			fast := &mockEventDispatcher{
				id: common.NewDispatcherID(), changefeedID: cf,
				tableSpan: &heartbeatpb.TableSpan{TableID: 1}, checkpointTs: 99,
				handleEvents: func(events []dispatcher.DispatcherEvent, wake func()) bool {
					blocked <- wake
					return true
				},
			}
			slowProgress := make(chan struct{}, 1)
			slowMode := int64(common.DefaultMode)
			if withRedo {
				slowMode = common.RedoMode
			}
			slow := &mockEventDispatcher{
				id: common.NewDispatcherID(), changefeedID: cf, mode: slowMode,
				tableSpan: &heartbeatpb.TableSpan{TableID: 2}, checkpointTs: 99,
				handleEvents: func(events []dispatcher.DispatcherEvent, wake func()) bool {
					slowProgress <- struct{}{}
					return false
				},
			}
			messages := make(chan *messaging.TargetMessage, 2)
			for _, target := range []*mockEventDispatcher{fast, slow} {
				stat := newDispatcherStatInternal(target, c, from, func(msg *messaging.TargetMessage) { messages <- msg }, nil)
				stat.currentEpoch.Store(newDispatcherEpochState(1, 1, 99))
				markSessionReceiving(stat.session, from)
				c.dispatcherMap.Store(target.id, stat)
				require.NoError(t, c.getDynamicStream(target.mode).AddPath(target.id, stat,
					dynstream.NewAreaSettingsWithMaxPendingSize(1<<20, dynstream.MemoryControlForEventCollector, "test")))
			}
			ctx, cancel := context.WithCancel(context.Background())
			feedbackDone := make(chan error, 1)
			go func() { feedbackDone <- c.processDSFeedback(ctx) }()
			defer func() {
				cancel()
				require.ErrorIs(t, <-feedbackDone, context.Canceled)
			}()
			push := func(target *mockEventDispatcher, e commonEvent.Event) {
				c.getDynamicStream(target.mode).Push(target.id, dispatcher.NewDispatcherEvent(&from, e))
			}
			quota := func() commonEvent.AvailableMemory {
				msgs := c.newCongestionControlMessages()
				require.Contains(t, msgs, from)
				entries := msgs[from].GetAvailables()
				require.Len(t, entries, 1)
				return entries[0]
			}

			push(fast, commonEvent.NewSyncPointEvent(fast.id, 100, 2, 1))
			var wakeFast func()
			select {
			case wakeFast = <-blocked:
			case <-time.After(5 * time.Second):
				t.Fatal("fast syncpoint did not block")
			}
			fast.pauseScan = true
			push(fast, &commonEvent.DMLEvent{DispatcherID: fast.id, Seq: 3, Epoch: 1, CommitTs: 101, ApproximateSize: 1 << 20})
			require.Eventually(t, func() bool {
				return c.ds.GetMetrics().MemoryControl.AreaMemoryMetrics[0].MemoryUsage() >= 1<<20
			}, 5*time.Second, time.Millisecond)
			before := quota()
			require.Zero(t, before.Available, "the fast path has exhausted shared scan capacity")
			require.Zero(t, before.DispatcherAvailable[fast.id], "syncpoint waiter must stop scanning")
			require.Positive(t, before.DispatcherAvailable[slow.id], "lagging producer must retain its path quota")

			// The periodic pressure check works even though no further events arrive.
			require.Eventually(t, func() bool {
				c.ds.CheckMemoryPressure()
				return c.ds.GetMetrics().MemoryControl.AreaMemoryMetrics[0].MemoryUsage() < 1000
			}, 10*time.Second, 10*time.Millisecond)
			require.Eventually(t, func() bool { return cfStat.memoryReleaseCount.Load() == 1 }, 5*time.Second, time.Millisecond)
			after := quota()
			require.Zero(t, after.DispatcherAvailable[fast.id])
			require.Greater(t, after.Available, before.Available)
			require.Equal(t, uint32(1), after.MemoryReleaseCount)
			require.Empty(t, messages, "reclamation must not reset an active batch")

			// Model the producer's next scan only after it receives freed quota.
			if withRedo {
				push(slow, &commonEvent.DMLEvent{DispatcherID: slow.id, Seq: 2, Epoch: 1, CommitTs: 100, ApproximateSize: 100})
			} else {
				push(slow, commonEvent.NewSyncPointEvent(slow.id, 100, 2, 1))
			}
			select {
			case <-slowProgress:
			case <-time.After(5 * time.Second):
				t.Fatal("lagging producer did not advance")
			}
			fast.pauseScan = false
			wakeFast()
			select {
			case msg := <-messages:
				req := msg.Message[0].(*messaging.DispatcherRequest)
				require.Equal(t, fast.id, req.GetID())
				require.Equal(t, eventpb.ActionType_ACTION_TYPE_RESET, req.ActionType)
				require.Equal(t, uint64(2), req.Epoch)
			case <-time.After(5 * time.Second):
				t.Fatal("reclaimed fast path did not reset after barrier completion")
			}
			require.Positive(t, quota().DispatcherAvailable[fast.id], "resumed path must be eligible to scan")
		})
	}
}
