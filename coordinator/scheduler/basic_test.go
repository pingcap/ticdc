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

package scheduler

import (
	"testing"

	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/coordinator/drain"
	"github.com/pingcap/ticdc/coordinator/operator"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
)

func TestBasicSchedulerRequiresTargetAckBeforeUsingDestination(t *testing.T) {
	setupCoordinatorSchedulerTestServices()
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)

	target := node.ID("target")
	pending := node.ID("pending")
	acked := node.ID("acked")
	nodeManager.GetAliveNodes()[target] = &node.Info{ID: target}
	nodeManager.GetAliveNodes()[pending] = &node.Info{ID: pending}
	nodeManager.GetAliveNodes()[acked] = &node.Info{ID: acked}

	drainController := drain.NewController(mc)
	epoch := uint64(11)
	drainController.StartDrainTargetSchedulerGate(target, epoch)
	drainController.ObserveHeartbeat(pending, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch: 1,
	})
	drainController.ObserveHeartbeat(acked, &heartbeatpb.NodeHeartbeat{
		Liveness:                    heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch:                   1,
		DispatcherDrainTargetNodeId: target.String(),
		DispatcherDrainTargetEpoch:  epoch,
	})

	db := changefeed.NewChangefeedDB(1)
	cfID := addAbsentChangefeed(t, db, "cf-absent")

	selfNode := &node.Info{ID: node.ID("coordinator")}
	oc := operator.NewOperatorController(selfNode, db, nil, nil, 10)
	s := NewBasicScheduler("test", 10, oc, db, drainController)
	_ = s.Execute()

	op := oc.GetOperator(cfID)
	require.NotNil(t, op)
	require.ElementsMatch(t, []node.ID{acked}, op.AffectedNodes())
}

func TestBasicSchedulerSkipsWhenSchedulingFrozen(t *testing.T) {
	setupCoordinatorSchedulerTestServices()
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)

	nodeA := node.ID("node-a")
	nodeB := node.ID("node-b")
	nodeManager.GetAliveNodes()[nodeA] = &node.Info{ID: nodeA}
	nodeManager.GetAliveNodes()[nodeB] = &node.Info{ID: nodeB}

	drainController := drain.NewController(mc)
	drainController.SetSchedulingFrozen(true)

	db := changefeed.NewChangefeedDB(1)
	cfID := addAbsentChangefeed(t, db, "cf-frozen")

	selfNode := &node.Info{ID: node.ID("coordinator")}
	oc := operator.NewOperatorController(selfNode, db, nil, nil, 10)
	s := NewBasicScheduler("test", 10, oc, db, drainController)
	_ = s.Execute()

	require.Nil(t, oc.GetOperator(cfID))
	require.Equal(t, 0, oc.OperatorSize())
}

func addAbsentChangefeed(t *testing.T, db *changefeed.ChangefeedDB, name string) common.ChangeFeedID {
	t.Helper()

	cfID := common.NewChangeFeedIDWithName(name, common.DefaultKeyspaceName)
	info := &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		SinkURI:      "blackhole://absent",
		Config:       config.GetDefaultReplicaConfig(),
		State:        config.StateNormal,
	}
	db.AddAbsentChangefeed(changefeed.NewChangefeed(cfID, info, 1, true))
	return cfID
}
