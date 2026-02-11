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

package coordinator

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestNodeLivenessViewTTL(t *testing.T) {
	view := newNodeLivenessView(30 * time.Second)
	now := time.Now()
	nodeID := node.ID("node-1")

	status := view.getStatus(nodeID, now)
	require.Equal(t, nodeLivenessAlive, status.liveness)

	view.applyHeartbeat(nodeID, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 11,
	}, now)

	status = view.getStatus(nodeID, now.Add(10*time.Second))
	require.Equal(t, nodeLivenessDraining, status.liveness)

	status = view.getStatus(nodeID, now.Add(31*time.Second))
	require.Equal(t, nodeLivenessUnknown, status.liveness)
}

func TestDrainRemainingGuard(t *testing.T) {
	nodeID := node.ID("node-1")
	now := time.Now()
	remaining := drainRemaining{
		targetNode:                 nodeID,
		maintainersOnTarget:        0,
		inflightOpsInvolvingTarget: 0,
		drainingObserved:           false,
		stoppingObserved:           false,
		nodeLiveness:               nodeLivenessAlive,
	}
	require.Equal(t, 1, remaining.remaining())

	remaining.drainingObserved = true
	remaining.nodeLiveness = nodeLivenessUnknown
	require.Equal(t, 1, remaining.remaining())

	remaining.nodeLiveness = nodeLivenessStopping
	require.Equal(t, 1, remaining.remaining())

	remaining.stoppingObserved = true
	require.Equal(t, 0, remaining.remaining())

	_ = now
}
