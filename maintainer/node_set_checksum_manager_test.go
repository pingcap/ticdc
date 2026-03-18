// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package maintainer

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestDispatcherSetChecksumResendAndAck(t *testing.T) {
	mgr := newNodeSetChecksumManager(
		common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName),
		1,
		common.DefaultMode,
	)

	capture := node.ID("capture-1")
	id1 := common.NewDispatcherID()
	id2 := common.NewDispatcherID()

	mgr.ApplyDelta(capture, []common.DispatcherID{id1}, nil)
	mgr.ApplyDelta(capture, []common.DispatcherID{id2}, nil)

	msgs := mgr.FlushDirty()
	require.Len(t, msgs, 1)
	update := msgs[0].Message[0].(*heartbeatpb.DispatcherSetChecksumUpdateRequest)
	require.Equal(t, uint64(2), update.Seq)

	mgr.mu.Lock()
	mgr.state.nodes[capture].lastSendAt = time.Time{}
	mgr.mu.Unlock()

	msgs = mgr.ResendPending()
	require.Len(t, msgs, 1)
	update = msgs[0].Message[0].(*heartbeatpb.DispatcherSetChecksumUpdateRequest)
	require.Equal(t, uint64(2), update.Seq)

	mgr.HandleAck(capture, &heartbeatpb.DispatcherSetChecksumAckResponse{
		ChangefeedID: mgr.changefeedID.ToPB(),
		Epoch:        1,
		Mode:         common.DefaultMode,
		Seq:          1,
	})
	mgr.mu.Lock()
	mgr.state.nodes[capture].lastSendAt = time.Time{}
	mgr.mu.Unlock()
	msgs = mgr.ResendPending()
	require.Len(t, msgs, 1)

	mgr.HandleAck(capture, &heartbeatpb.DispatcherSetChecksumAckResponse{
		ChangefeedID: mgr.changefeedID.ToPB(),
		Epoch:        1,
		Mode:         common.DefaultMode,
		Seq:          2,
	})
	msgs = mgr.ResendPending()
	require.Empty(t, msgs)
}

func TestNodeSetChecksumManagerRemoveNodesCleansState(t *testing.T) {
	mgr := newNodeSetChecksumManager(
		common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName),
		1,
		common.DefaultMode,
	)

	capture := node.ID("capture-1")
	otherCapture := node.ID("capture-2")
	id := common.NewDispatcherID()

	mgr.ApplyDelta(capture, []common.DispatcherID{id}, nil)
	mgr.ApplyDelta(otherCapture, []common.DispatcherID{common.NewDispatcherID()}, nil)

	mgr.RemoveNodes([]node.ID{capture})

	mgr.mu.Lock()
	_, captureExists := mgr.state.nodes[capture]
	_, mappingExists := mgr.state.dispatcherToNode[id]
	_, otherExists := mgr.state.nodes[otherCapture]
	mgr.mu.Unlock()

	require.False(t, captureExists)
	require.False(t, mappingExists)
	require.True(t, otherExists)
}
