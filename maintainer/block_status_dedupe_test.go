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

package maintainer

import (
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/stretchr/testify/require"
)

func TestFilterBlockStatusEventDeduplicatesRequestStatuses(t *testing.T) {
	m := &Maintainer{
		changefeedID: common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName),
	}
	dispatcherID := common.NewDispatcherID()
	waitingStatus := newTestBlockStatus(dispatcherID, 100, heartbeatpb.BlockStage_WAITING)
	waitingDuplicate := newTestBlockStatus(dispatcherID, 100, heartbeatpb.BlockStage_WAITING)
	doneStatus := newTestBlockStatus(dispatcherID, 100, heartbeatpb.BlockStage_DONE)
	doneDuplicate := newTestBlockStatus(dispatcherID, 100, heartbeatpb.BlockStage_DONE)
	noneStatus := newTestBlockStatus(dispatcherID, 100, heartbeatpb.BlockStage_NONE)
	noneDuplicate := newTestBlockStatus(dispatcherID, 100, heartbeatpb.BlockStage_NONE)

	event := newTestBlockStatusEvent("capture-1", common.DefaultMode,
		waitingStatus, waitingDuplicate, doneStatus, doneDuplicate, noneStatus, noneDuplicate)

	filteredEvent, ok := m.filterBlockStatusEvent(event)
	require.True(t, ok)
	require.NotNil(t, filteredEvent)
	require.NotSame(t, event, filteredEvent)
	require.Len(t, filteredEvent.blockStatusReleaseKeys, 3)

	filteredReq := filteredEvent.message.Message[0].(*heartbeatpb.BlockStatusRequest)
	require.Len(t, filteredReq.BlockStatuses, 3)
	require.Same(t, waitingStatus, filteredReq.BlockStatuses[0])
	require.Same(t, doneStatus, filteredReq.BlockStatuses[1])
	require.Same(t, noneStatus, filteredReq.BlockStatuses[2])
}

func TestPushEventSuppressesDuplicatesUntilRelease(t *testing.T) {
	m := &Maintainer{
		changefeedID: common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName),
		eventCh:      chann.NewAutoDrainChann[*Event](chann.Cap(8)),
	}
	defer m.eventCh.CloseAndDrain()

	dispatcherID := common.NewDispatcherID()
	first := newTestBlockStatusEvent("capture-1", common.DefaultMode,
		newTestBlockStatus(dispatcherID, 200, heartbeatpb.BlockStage_WAITING))
	duplicate := newTestBlockStatusEvent("capture-1", common.DefaultMode,
		newTestBlockStatus(dispatcherID, 200, heartbeatpb.BlockStage_WAITING))

	m.pushEvent(first)
	m.pushEvent(duplicate)

	queued := <-m.eventCh.Out()
	require.NotNil(t, queued)
	require.Len(t, queued.blockStatusReleaseKeys, 1)

	select {
	case unexpected := <-m.eventCh.Out():
		t.Fatalf("unexpected duplicate event queued: %#v", unexpected)
	default:
	}

	m.blockStatusPending.release(queued.blockStatusReleaseKeys)
	m.pushEvent(duplicate)

	requeued := <-m.eventCh.Out()
	require.NotNil(t, requeued)
	require.Len(t, requeued.blockStatusReleaseKeys, 1)
}

func TestFilterBlockStatusEventLeavesHeartbeatUntouched(t *testing.T) {
	m := &Maintainer{
		changefeedID: common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName),
	}
	heartbeat := &heartbeatpb.HeartBeatRequest{
		ChangefeedID: common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName).ToPB(),
	}
	event := &Event{
		changefeedID: common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName),
		eventType:    EventMessage,
		message: &messaging.TargetMessage{
			From:    "capture-1",
			Type:    messaging.TypeHeartBeatRequest,
			Message: []messaging.IOTypeT{heartbeat},
		},
	}

	filteredEvent, ok := m.filterBlockStatusEvent(event)
	require.True(t, ok)
	require.Same(t, event, filteredEvent)
}

func newTestBlockStatusEvent(
	from node.ID,
	mode int64,
	statuses ...*heartbeatpb.TableSpanBlockStatus,
) *Event {
	return &Event{
		changefeedID: common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName),
		eventType:    EventMessage,
		message: &messaging.TargetMessage{
			From: from,
			Type: messaging.TypeBlockStatusRequest,
			Message: []messaging.IOTypeT{
				&heartbeatpb.BlockStatusRequest{
					ChangefeedID:  common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName).ToPB(),
					Mode:          mode,
					BlockStatuses: statuses,
				},
			},
		},
	}
}

func newTestBlockStatus(
	dispatcherID common.DispatcherID,
	blockTs uint64,
	stage heartbeatpb.BlockStage,
) *heartbeatpb.TableSpanBlockStatus {
	return &heartbeatpb.TableSpanBlockStatus{
		ID: dispatcherID.ToPB(),
		State: &heartbeatpb.State{
			IsBlocked:   stage != heartbeatpb.BlockStage_NONE,
			BlockTs:     blockTs,
			IsSyncPoint: false,
			Stage:       stage,
		},
		Mode: common.DefaultMode,
	}
}
