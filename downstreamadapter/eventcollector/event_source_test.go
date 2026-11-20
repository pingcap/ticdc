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
	"testing"

	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestDispatcherRejectsEventsFromOtherServices(t *testing.T) {
	for _, event := range []commonEvent.Event{
		&commonEvent.HandshakeEvent{Seq: 1, Epoch: 7, ResolvedTs: 200, TableInfo: &common.TableInfo{}},
		&commonEvent.DMLEvent{Seq: 2, Epoch: 7, CommitTs: 200},
		&commonEvent.BatchDMLEvent{Rows: chunk.NewEmptyChunk(nil), DMLEvents: []*commonEvent.DMLEvent{{Seq: 2, Epoch: 7, CommitTs: 200}}},
		&commonEvent.ResolvedEvent{Seq: 1, Epoch: 7, ResolvedTs: 200},
		&commonEvent.DDLEvent{Seq: 2, Epoch: 7, FinishedTs: 200},
		&commonEvent.SyncPointEvent{Seq: 2, Epoch: 7, CommitTs: 200},
		commonEvent.NewDropEvent(common.NewDispatcherID(), 2, 7, 200),
	} {
		t.Run(commonEvent.TypeToString(event.GetType()), func(t *testing.T) {
			for _, state := range []string{"wrong sender", "missing sender", "no current service", "removed"} {
				t.Run(state, func(t *testing.T) {
					target := newMockDispatcher(common.NewDispatcherID(), 100)
					target.handleEvents = func([]dispatcher.DispatcherEvent, func()) bool { return true }
					var sent []*messaging.TargetMessage
					stat := newDispatcherStatInternal(target, nil, "local", func(msg *messaging.TargetMessage) { sent = append(sent, msg) }, nil)
					markSessionReceiving(stat.session, "selected")
					stat.currentEpoch.Store(newDispatcherEpochState(7, 1, 100))
					tableInfo := &common.TableInfo{}
					stat.tableInfo.Store(tableInfo)
					from := createNodeID("other")
					switch state {
					case "missing sender":
						from = nil
					case "no current service":
						markSessionReceiving(stat.session, "")
						from = createNodeID("selected")
					case "removed":
						stat.session.connState.removed = true
						from = createNodeID("selected")
					}
					handle := func(from *node.ID) {
						incoming := dispatcher.NewDispatcherEvent(from, event)
						switch event.GetType() {
						case commonEvent.TypeHandshakeEvent:
							stat.handleHandshakeEvent(incoming)
						case commonEvent.TypeDropEvent:
							stat.handleDropEvent(incoming)
						default:
							stat.handleDataEvents(incoming)
						}
					}
					handle(from)
					require.Empty(t, target.events)
					require.Empty(t, sent, "a rejected event must not trigger RESET")
					require.Equal(t, uint64(7), stat.loadCurrentEpochState().epoch)
					require.Equal(t, uint64(1), stat.loadCurrentEpochState().lastEventSeq.Load())
					require.Equal(t, uint64(100), stat.loadCurrentEpochState().maxEventTs.Load())
					require.Equal(t, uint64(100), stat.lastEventCommitTs.Load())
					require.False(t, stat.gotDDLOnTs.Load())
					require.False(t, stat.gotSyncpointOnTS.Load())
					require.Same(t, tableInfo, stat.tableInfo.Load())
					if state == "wrong sender" || state == "missing sender" {
						handle(createNodeID("selected"))
						if event.GetType() == commonEvent.TypeDropEvent {
							require.Len(t, sent, 1)
							require.Equal(t, uint64(8), stat.loadCurrentEpochState().epoch)
						} else {
							require.Equal(t, uint64(200), stat.loadCurrentEpochState().maxEventTs.Load())
							if event.GetType() != commonEvent.TypeHandshakeEvent {
								require.NotEmpty(t, target.events)
							}
						}
					}
				})
			}
		})
	}
}

func TestEventSourceValidationPreservesLocalTakeover(t *testing.T) {
	target := newMockDispatcher(common.NewDispatcherID(), 100)
	target.handleEvents = func([]dispatcher.DispatcherEvent, func()) bool { return true }
	stat := newDispatcherStatInternal(target, nil, "local", func(*messaging.TargetMessage) {}, nil)
	stat.run()
	stat.startRemoteProbing([]string{"remote"})
	stat.session.handleReadyEvent("remote", 150)
	require.Equal(t, node.ID("remote"), stat.session.getEventServiceID())
	remoteHandshake := commonEvent.NewHandshakeEvent(target.id, 150, 1, nil)
	stat.handleHandshakeEvent(dispatcher.NewDispatcherEvent(createNodeID("local"), &remoteHandshake))
	require.Zero(t, stat.loadCurrentEpochState().lastEventSeq.Load())
	stat.handleHandshakeEvent(dispatcher.NewDispatcherEvent(createNodeID("remote"), &remoteHandshake))
	stat.handleDataEvents(dispatcher.NewDispatcherEvent(createNodeID("remote"), &commonEvent.DMLEvent{Seq: 2, Epoch: 1, CommitTs: 160}))
	require.Len(t, target.events, 1)

	stat.session.handleReadyEvent("local", 160)
	require.Equal(t, node.ID("local"), stat.session.getEventServiceID())
	require.Equal(t, uint64(2), stat.loadCurrentEpochState().epoch)
	localHandshake := commonEvent.NewHandshakeEvent(target.id, 160, 2, nil)
	// The replaced service cannot send even with the new epoch and sequence.
	stat.handleHandshakeEvent(dispatcher.NewDispatcherEvent(createNodeID("remote"), &localHandshake))
	require.Zero(t, stat.loadCurrentEpochState().lastEventSeq.Load())
	stat.handleHandshakeEvent(dispatcher.NewDispatcherEvent(createNodeID("local"), &localHandshake))
	dml := &commonEvent.DMLEvent{Seq: 2, Epoch: 2, CommitTs: 170}
	stat.handleDataEvents(dispatcher.NewDispatcherEvent(createNodeID("remote"), dml))
	require.Len(t, target.events, 1)
	require.Equal(t, uint64(1), stat.loadCurrentEpochState().lastEventSeq.Load())
	stat.handleDataEvents(dispatcher.NewDispatcherEvent(createNodeID("local"), dml))
	require.Len(t, target.events, 2)
	require.Equal(t, uint64(170), stat.loadCurrentEpochState().maxEventTs.Load())
}
