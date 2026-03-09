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
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/span"
	"github.com/pingcap/ticdc/maintainer/testutil"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestNewBarrierDefaultsFlushDisabled(t *testing.T) {
	testutil.SetUpTestServices()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	spanController := span.NewController(cfID, ddlSpan, nil, nil, nil, common.DefaultKeyspaceID, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000, common.DefaultMode)

	barrier := NewBarrier(spanController, operatorController, false, false, nil, common.DefaultMode)
	require.False(t, barrier.flushEnabled)
}

func TestInitialBarrierPhase(t *testing.T) {
	require.Equal(t, barrierPhaseFlush, initialBarrierPhase(true))
	require.Equal(t, barrierPhaseWrite, initialBarrierPhase(false))
}

func TestNewBlockEventUsesFlushEnabled(t *testing.T) {
	testutil.SetUpTestServices()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)
	spanController := span.NewController(cfID, ddlSpan, nil, nil, nil, common.DefaultKeyspaceID, common.DefaultMode)
	operatorController := operator.NewOperatorController(cfID, spanController, 1000, common.DefaultMode)

	event := NewBlockEvent(cfID, tableTriggerEventDispatcherID, spanController, operatorController, &heartbeatpb.State{
		IsBlocked: true,
		BlockTs:   10,
		BlockTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_Normal,
			TableIDs:      []int64{0},
		},
	}, false, false)
	require.False(t, event.flushEnabled)
	require.Equal(t, barrierPhaseWrite, event.phase)
}

func TestInferLegacyDoneActionByPhase(t *testing.T) {
	dispatcherID := common.NewDispatcherID()
	otherDispatcherID := common.NewDispatcherID()

	event := &BarrierEvent{
		phase:            barrierPhaseFlush,
		writerDispatcher: dispatcherID,
	}
	require.Equal(t, heartbeatpb.DoneAction_FlushDone, inferLegacyDoneAction(event, dispatcherID))

	event.phase = barrierPhaseWrite
	require.Equal(t, heartbeatpb.DoneAction_WriteDone, inferLegacyDoneAction(event, dispatcherID))
	require.Equal(t, heartbeatpb.DoneAction_Unknown, inferLegacyDoneAction(event, otherDispatcherID))

	event.phase = barrierPhaseFlushThenPass
	require.Equal(t, heartbeatpb.DoneAction_FlushDone, inferLegacyDoneAction(event, otherDispatcherID))

	event.phase = barrierPhasePass
	require.Equal(t, heartbeatpb.DoneAction_PassDone, inferLegacyDoneAction(event, otherDispatcherID))
}
