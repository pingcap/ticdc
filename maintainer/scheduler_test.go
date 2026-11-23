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
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/scheduler"
	"github.com/pingcap/ticdc/maintainer/span"
	"github.com/pingcap/ticdc/maintainer/testutil"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/node"
	pkgscheduler "github.com/pingcap/ticdc/pkg/scheduler"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
)

func TestScheduleControllerSharesTrafficMoveLimit(t *testing.T) {
	for _, first := range []int{0, 1} {
		names := []string{pkgscheduler.BalanceScheduler, pkgscheduler.RedoBalanceScheduler}
		t.Run(names[first]+" first", func(t *testing.T) {
			m, self := newMaintainerForRedoCheckpointCalculationTest(t)
			c := m.controller
			nm := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
			nm.GetAliveNodes()[self] = &node.Info{ID: self}
			nm.GetAliveNodes()["target"] = &node.Info{ID: "target"}
			tracker := replica.NewNodeLoadTracker()
			tracker.Replace([]*heartbeatpb.NodeLoad{{NodeId: self.String(), BytesPerSecond: 400}, {NodeId: "target"}}, heartbeatpb.NodeLoadStatus_LOAD_AVAILABLE, time.Now())
			controllers := []*span.Controller{c.spanController, c.redoSpanController}
			operators := []*operator.Controller{c.operatorController, c.redoOperatorController}
			for i, sc := range controllers {
				sc.SetNodeLoadTracker(tracker)
				mode := []int64{common.DefaultMode, common.RedoMode}[i]
				for j, id := range []node.ID{self, self, "target"} {
					rate := float32(100)
					if id == "target" {
						rate = 1
					}
					r := replica.NewWorkingSpanReplication(m.changefeedID, common.NewDispatcherID(), 1,
						testutil.GetTableSpanByID(int64(j+1)), &heartbeatpb.TableSpanStatus{CheckpointTs: 1, Mode: mode}, id, false)
					r.UpdateStatus(&heartbeatpb.TableSpanStatus{CheckpointTs: 1, Mode: mode, ComponentStatus: heartbeatpb.ComponentState_Working, EventSizePerSecond: rate})
					sc.AddReplicatingSpan(r)
				}
			}
			sc := NewScheduleController(m.changefeedID, 10, operators[0], operators[1], controllers[0], controllers[1], time.Second, nil, nil, scheduler.NewDrainState(), 10)
			sc.GetScheduler(names[first]).Execute()
			require.Equal(t, 1, operators[first].OperatorSize())
			sc.GetScheduler(names[1-first]).Execute()
			require.Zero(t, operators[1-first].OperatorSize(), "normal and redo must share the in-flight traffic move limit")

			op := operators[first].GetAllOperators()[0]
			op.Check(op.Schedule().To, &heartbeatpb.TableSpanStatus{CheckpointTs: 1, ComponentStatus: heartbeatpb.ComponentState_Stopped})
			op.Check(op.Schedule().To, &heartbeatpb.TableSpanStatus{CheckpointTs: 1, ComponentStatus: heartbeatpb.ComponentState_Working})
			require.True(t, op.IsFinished())
			op.PostFinish()
			operators[first].RemoveOp(op.ID())
			// Either mode can first observe completion. Both must honor the same
			// observation period before acting on the unchanged node snapshot.
			sc.GetScheduler(names[1-first]).Execute()
			sc.GetScheduler(names[first]).Execute()
			require.Zero(t, operators[0].OperatorSize())
			require.Zero(t, operators[1].OperatorSize())
		})
	}
}
