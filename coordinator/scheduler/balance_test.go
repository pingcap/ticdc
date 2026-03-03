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
	"fmt"
	"testing"
	"time"

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

func TestBalanceSchedulerCreatesMoveOperators(t *testing.T) {
	setupCoordinatorSchedulerTestServices()
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)

	nodeA := node.ID("node-a")
	nodeB := node.ID("node-b")
	nodeManager.GetAliveNodes()[nodeA] = &node.Info{ID: nodeA}
	nodeManager.GetAliveNodes()[nodeB] = &node.Info{ID: nodeB}

	drainController := drain.NewControllerWithTTL(mc, 30*time.Second)
	db := changefeed.NewChangefeedDB(1)
	addReplicatingMaintainer(t, db, "cf-a-1", nodeA)
	addReplicatingMaintainer(t, db, "cf-a-2", nodeA)

	selfNode := &node.Info{ID: node.ID("coordinator")}
	oc := operator.NewOperatorController(selfNode, db, nil, 10)
	s := NewBalanceScheduler("test", 10, oc, db, 0, drainController)
	_ = s.Execute()

	require.Equal(t, 1, oc.OperatorSize())
}

func TestBalanceSchedulerSkipsWhenDrainActive(t *testing.T) {
	setupCoordinatorSchedulerTestServices()
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)

	drainingNode := node.ID("draining")
	nodeA := node.ID("node-a")
	nodeB := node.ID("node-b")
	nodeManager.GetAliveNodes()[drainingNode] = &node.Info{ID: drainingNode}
	nodeManager.GetAliveNodes()[nodeA] = &node.Info{ID: nodeA}
	nodeManager.GetAliveNodes()[nodeB] = &node.Info{ID: nodeB}

	drainController := drain.NewControllerWithTTL(mc, 30*time.Second)
	drainController.ObserveHeartbeat(drainingNode, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 1,
	})

	db := changefeed.NewChangefeedDB(1)
	addReplicatingMaintainer(t, db, "cf-drain", drainingNode)
	addReplicatingMaintainer(t, db, "cf-a-1", nodeA)
	addReplicatingMaintainer(t, db, "cf-a-2", nodeA)
	addReplicatingMaintainer(t, db, "cf-a-3", nodeA)
	addReplicatingMaintainer(t, db, "cf-a-4", nodeA)

	selfNode := &node.Info{ID: node.ID("coordinator")}
	oc := operator.NewOperatorController(selfNode, db, nil, 10)
	s := NewBalanceScheduler("test", 10, oc, db, 0, drainController)
	_ = s.Execute()

	require.Equal(t, 0, oc.OperatorSize())
}

func TestBalanceSchedulerSkipsUntilAllDrainCompleteAndCooldownExpires(t *testing.T) {
	setupCoordinatorSchedulerTestServices()
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)

	drainingA := node.ID("draining-a")
	drainingB := node.ID("draining-b")
	nodeA := node.ID("node-a")
	nodeB := node.ID("node-b")
	nodeManager.GetAliveNodes()[drainingA] = &node.Info{ID: drainingA}
	nodeManager.GetAliveNodes()[drainingB] = &node.Info{ID: drainingB}
	nodeManager.GetAliveNodes()[nodeA] = &node.Info{ID: nodeA}
	nodeManager.GetAliveNodes()[nodeB] = &node.Info{ID: nodeB}

	drainController := drain.NewControllerWithTTL(mc, 30*time.Second)
	drainController.ObserveHeartbeat(drainingA, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 1,
	})
	drainController.ObserveHeartbeat(drainingB, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 1,
	})

	db := changefeed.NewChangefeedDB(1)
	addReplicatingMaintainer(t, db, "cf-a-1", nodeA)
	addReplicatingMaintainer(t, db, "cf-a-2", nodeA)
	addReplicatingMaintainer(t, db, "cf-a-3", nodeA)
	addReplicatingMaintainer(t, db, "cf-a-4", nodeA)

	selfNode := &node.Info{ID: node.ID("coordinator")}
	oc := operator.NewOperatorController(selfNode, db, nil, 10)
	s := NewBalanceScheduler("test", 10, oc, db, 0, drainController)
	s.drainBalanceBlockedUntil = time.Time{}

	// Any draining node should block balance.
	_ = s.Execute()
	require.Equal(t, 0, oc.OperatorSize())

	// One node remains draining, balance is still blocked.
	drainController.ObserveHeartbeat(drainingA, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch: 2,
	})
	_ = s.Execute()
	require.Equal(t, 0, oc.OperatorSize())

	// After all nodes leave draining, cooldown still blocks balance.
	drainController.ObserveHeartbeat(drainingB, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch: 2,
	})
	s.drainBalanceBlockedUntil = time.Now().Add(100 * time.Millisecond)
	_ = s.Execute()
	require.Equal(t, 0, oc.OperatorSize())

	s.drainBalanceBlockedUntil = time.Now().Add(-time.Millisecond)
	_ = s.Execute()
	require.Greater(t, oc.OperatorSize(), 0)
}

func setupCoordinatorSchedulerTestServices() {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)
	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)
}

func addReplicatingMaintainer(
	t *testing.T,
	db *changefeed.ChangefeedDB,
	name string,
	nodeID node.ID,
) common.ChangeFeedID {
	t.Helper()

	cfID := common.NewChangeFeedIDWithName(name, common.DefaultKeyspaceName)
	info := &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		SinkURI:      fmt.Sprintf("blackhole://%s", name),
		Config:       config.GetDefaultReplicaConfig(),
		State:        config.StateNormal,
	}
	cf := changefeed.NewChangefeed(cfID, info, 1, false)
	db.AddReplicatingMaintainer(cf, nodeID)
	return cfID
}
