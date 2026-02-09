package maintainer

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/testutil"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/node"
	kafkapkg "github.com/pingcap/ticdc/pkg/sink/kafka"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
)

func TestHandleRecoverableKafkaError_RestartDispatchers(t *testing.T) {
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

	report := kafkapkg.ErrorReport{
		KafkaErrCode:  20,
		KafkaErrName:  "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
		Message:       "recoverable kafka error",
		Topic:         "test-topic",
		Partition:     1,
		DispatcherIDs: []common.DispatcherID{dispatcherID},
	}
	payload, err := json.Marshal(&report)
	require.NoError(t, err)

	runningErr := &heartbeatpb.RunningError{
		Code:    kafkapkg.KafkaTransientErrorCode,
		Message: string(payload),
	}
	require.True(t, m.handleRecoverableKafkaError(node.ID("node1"), runningErr))

	op := controller.operatorController.GetOperator(dispatcherID)
	require.NotNil(t, op)

	msg := op.Schedule()
	require.NotNil(t, msg)
	require.Equal(t, node.ID("node1").String(), msg.To.String())
	scheduleMsg, ok := msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest)
	require.True(t, ok)
	require.Equal(t, heartbeatpb.ScheduleAction_Remove, scheduleMsg.ScheduleAction)
}
