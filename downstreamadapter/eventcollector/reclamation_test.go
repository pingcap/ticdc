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
	"github.com/stretchr/testify/require"
)

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
