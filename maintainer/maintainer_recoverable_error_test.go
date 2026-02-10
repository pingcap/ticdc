package maintainer

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/testutil"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestRecoverDispatcherRequest_RestartDispatchers(t *testing.T) {
	testutil.SetUpTestServices()
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}

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
		node.ID("node1"),
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
	spanReplica.SetNodeID(node.ID("node1"))
	controller.spanController.AddReplicatingSpan(spanReplica)

	m := &Maintainer{
		changefeedID: changefeedID,
		controller:   controller,
	}
	m.initialized.Store(true)

	req := &heartbeatpb.RecoverDispatcherRequest{
		ChangefeedID:  changefeedID.ToPB(),
		DispatcherIDs: []*heartbeatpb.DispatcherID{dispatcherID.ToPB()},
	}
	m.onRecoverDispatcherRequest(node.ID("node1"), req)

	op := controller.operatorController.GetOperator(dispatcherID)
	require.NotNil(t, op)

	msg := op.Schedule()
	require.NotNil(t, msg)
	require.Equal(t, node.ID("node1").String(), msg.To.String())
	scheduleMsg, ok := msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest)
	require.True(t, ok)
	require.Equal(t, heartbeatpb.ScheduleAction_Remove, scheduleMsg.ScheduleAction)
}

func TestRecoverDispatcherRequest_SkipWhenWithinBackoff(t *testing.T) {
	testutil.SetUpTestServices()
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}

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
		node.ID("node1"),
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
	spanReplica.SetNodeID(node.ID("node1"))
	controller.spanController.AddReplicatingSpan(spanReplica)

	m := &Maintainer{
		changefeedID:  changefeedID,
		controller:    controller,
		nodeManager:   nodeManager,
		statusChanged: atomic.NewBool(false),
	}
	m.runningErrors.m = make(map[node.ID]*heartbeatpb.RunningError)
	m.initialized.Store(true)

	req := &heartbeatpb.RecoverDispatcherRequest{
		ChangefeedID:  changefeedID.ToPB(),
		DispatcherIDs: []*heartbeatpb.DispatcherID{dispatcherID.ToPB()},
	}
	m.onRecoverDispatcherRequest(node.ID("node1"), req)
	require.NotNil(t, controller.operatorController.GetOperator(dispatcherID))

	finishMoveOperator(t, controller, dispatcherID, node.ID("node1"))
	require.Nil(t, controller.operatorController.GetOperator(dispatcherID))

	// A second request that happens too soon should be skipped.
	m.runningErrors.m = make(map[node.ID]*heartbeatpb.RunningError)
	m.onRecoverDispatcherRequest(node.ID("node1"), req)
	require.Empty(t, m.runningErrors.m)
	require.Nil(t, controller.operatorController.GetOperator(dispatcherID))
}

func TestRecoverDispatcherRequest_DowngradeToFatalWhenAttemptsExceeded(t *testing.T) {
	testutil.SetUpTestServices()
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}

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
		node.ID("node1"),
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
	spanReplica.SetNodeID(node.ID("node1"))
	controller.spanController.AddReplicatingSpan(spanReplica)

	m := &Maintainer{
		changefeedID:  changefeedID,
		controller:    controller,
		nodeManager:   nodeManager,
		statusChanged: atomic.NewBool(false),
	}
	m.runningErrors.m = make(map[node.ID]*heartbeatpb.RunningError)
	m.initialized.Store(true)

	now := time.Now()
	for i := 0; i < recoverableDispatcherRestartMaxAttempts; i++ {
		m.recordRecoverableDispatcherRestart(dispatcherID, now)
	}

	req := &heartbeatpb.RecoverDispatcherRequest{
		ChangefeedID:  changefeedID.ToPB(),
		DispatcherIDs: []*heartbeatpb.DispatcherID{dispatcherID.ToPB()},
	}
	m.onRecoverDispatcherRequest(node.ID("node1"), req)

	fatal := m.runningErrors.m[node.ID("node1")]
	require.NotNil(t, fatal)
	require.Equal(t, string(cerrors.ErrKafkaAsyncSendMessage.RFCCode()), fatal.Code)
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
