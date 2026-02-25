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
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/testutil"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func newRecoverDispatcherTestMaintainer(t *testing.T) (*Maintainer, *Controller, common.DispatcherID, node.ID) {
	t.Helper()

	testutil.SetUpTestServices()
	nodeID := node.ID("node1")
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()[nodeID] = &node.Info{ID: nodeID}

	changefeedID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	ddlDispatcherID := common.NewDispatcherID()
	ddlSpan := replica.NewWorkingSpanReplication(
		changefeedID,
		ddlDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID),
		&heartbeatpb.TableSpanStatus{
			ID:              ddlDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		},
		nodeID,
		false,
	)
	refresher := replica.NewRegionCountRefresher(changefeedID, time.Minute)
	controller := NewController(changefeedID, 1, nil, nil, ddlSpan, nil, 1, time.Minute, refresher, common.DefaultKeyspace, false)

	dispatcherID := common.NewDispatcherID()
	spanReplica := replica.NewSpanReplication(
		changefeedID,
		dispatcherID,
		1,
		testutil.GetTableSpanByID(1),
		1,
		common.DefaultMode,
		false,
	)
	spanReplica.SetNodeID(nodeID)
	controller.spanController.AddReplicatingSpan(spanReplica)

	m := &Maintainer{
		changefeedID:  changefeedID,
		controller:    controller,
		nodeManager:   nodeManager,
		statusChanged: atomic.NewBool(false),
		mc:            appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter),
		info:          &config.ChangeFeedInfo{Epoch: 1},
	}
	m.runningErrors.m = make(map[node.ID]*heartbeatpb.RunningError)
	m.recoverDispatcherHandler = newRecoverDispatcherHandler(m)
	m.initialized.Store(true)
	return m, controller, dispatcherID, nodeID
}

func newRecoverDispatcherRequest(changefeedID common.ChangeFeedID, dispatcherID common.DispatcherID) *heartbeatpb.RecoverDispatcherRequest {
	return &heartbeatpb.RecoverDispatcherRequest{
		ChangefeedID: changefeedID.ToPB(),
		Identities: []*heartbeatpb.RecoverDispatcherIdentity{
			{
				DispatcherID:    dispatcherID.ToPB(),
				DispatcherEpoch: 1,
				MaintainerEpoch: 1,
			},
		},
	}
}

func TestRecoverDispatcherRequestRestartDispatchers(t *testing.T) {
	m, controller, dispatcherID, nodeID := newRecoverDispatcherTestMaintainer(t)
	req := newRecoverDispatcherRequest(m.changefeedID, dispatcherID)
	m.onRecoverDispatcherRequest(nodeID, req)

	op := controller.operatorController.GetOperator(dispatcherID)
	require.NotNil(t, op)

	msg := op.Schedule()
	require.NotNil(t, msg)
	require.Equal(t, nodeID.String(), msg.To.String())
	scheduleMsg, ok := msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest)
	require.True(t, ok)
	require.Equal(t, heartbeatpb.ScheduleAction_Remove, scheduleMsg.ScheduleAction)
}

func TestRecoverDispatcherRequestSkipWhenAnyOperatorExists(t *testing.T) {
	m, controller, dispatcherID, nodeID := newRecoverDispatcherTestMaintainer(t)
	req := newRecoverDispatcherRequest(m.changefeedID, dispatcherID)

	destNodeID := node.ID("node2")
	m.nodeManager.GetAliveNodes()[destNodeID] = &node.Info{ID: destNodeID}
	replication := controller.spanController.GetTaskByID(dispatcherID)
	require.NotNil(t, replication)
	op := controller.operatorController.NewMoveOperator(replication, nodeID, destNodeID)
	require.True(t, controller.operatorController.AddOperator(op))

	existing := controller.operatorController.GetOperator(dispatcherID)
	require.NotNil(t, existing)
	require.Equal(t, "move", existing.Type())

	m.onRecoverDispatcherRequest(nodeID, req)

	current := controller.operatorController.GetOperator(dispatcherID)
	require.NotNil(t, current)
	require.Equal(t, existing, current)
	require.Empty(t, m.runningErrors.m)
	_, tracked := m.recoverDispatcherHandler.tracked[dispatcherID]
	require.False(t, tracked)
}

func TestRecoverDispatcherRequestRestartAgainAfterPreviousRestartFinished(t *testing.T) {
	m, controller, dispatcherID, nodeID := newRecoverDispatcherTestMaintainer(t)
	req := newRecoverDispatcherRequest(m.changefeedID, dispatcherID)
	m.onRecoverDispatcherRequest(nodeID, req)
	require.NotNil(t, controller.operatorController.GetOperator(dispatcherID))

	finishMoveOperator(t, controller, dispatcherID, nodeID)
	require.Nil(t, controller.operatorController.GetOperator(dispatcherID))

	// A second request after the previous restart finished should schedule a new restart.
	m.runningErrors.m = make(map[node.ID]*heartbeatpb.RunningError)
	m.onRecoverDispatcherRequest(nodeID, req)
	require.Empty(t, m.runningErrors.m)
	require.NotNil(t, controller.operatorController.GetOperator(dispatcherID))
}

func TestRecoverDispatcherRequestDowngradeToFatalWhenAttemptsExceeded(t *testing.T) {
	m, controller, dispatcherID, nodeID := newRecoverDispatcherTestMaintainer(t)

	for i := 0; i < recoverableMaxAttempts; i++ {
		state := m.recoverDispatcherHandler.getRestartState(dispatcherID)
		state.attempts++
		m.recoverDispatcherHandler.tracked[dispatcherID] = state
	}

	req := newRecoverDispatcherRequest(m.changefeedID, dispatcherID)
	m.onRecoverDispatcherRequest(nodeID, req)

	fatal := m.runningErrors.m[nodeID]
	require.NotNil(t, fatal)
	require.Equal(t, string(errors.ErrMaintainerRecoverableRestartExceededAttempts.RFCCode()), fatal.Code)
	require.Nil(t, controller.operatorController.GetOperator(dispatcherID))
}

func finishMoveOperator(t *testing.T, controller *Controller, dispatcherID common.DispatcherID, nodeID node.ID) {
	stoppedStatus := &heartbeatpb.TableSpanStatus{
		ID:              dispatcherID.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Stopped,
		CheckpointTs:    1,
	}
	controller.operatorController.UpdateOperatorStatus(dispatcherID, nodeID, stoppedStatus)

	workingStatus := &heartbeatpb.TableSpanStatus{
		ID:              dispatcherID.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Working,
		CheckpointTs:    1,
	}
	controller.operatorController.UpdateOperatorStatus(dispatcherID, nodeID, workingStatus)

	// Drain the finished operator from controller so subsequent restarts can be scheduled.
	for i := 0; i < 4 && controller.operatorController.GetOperator(dispatcherID) != nil; i++ {
		controller.operatorController.Execute()
	}
	require.Nil(t, controller.operatorController.GetOperator(dispatcherID))
}
