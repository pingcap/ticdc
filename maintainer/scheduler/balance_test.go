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
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

const testDefaultBalanceMoveBatchSize = 1024

func TestBalanceSchedulerSkipsWhenDrainActive(t *testing.T) {
	cfID, nodeManager, oc, sc, drainState, self := newDrainSchedulerTestHarness(t)
	target := node.ID("target")
	dest := node.ID("dest")

	nodeManager.GetAliveNodes()[self] = &node.Info{ID: self}
	nodeManager.GetAliveNodes()[target] = &node.Info{ID: target}
	nodeManager.GetAliveNodes()[dest] = &node.Info{ID: dest}

	for i := 1; i <= 20; i++ {
		addReplicatingSpan(t, cfID, sc, int64(i), target)
	}
	drainState.SetSelfNodeID(self)
	drainState.SetDispatcherDrainTarget(target, 1)

	s := NewBalanceScheduler(
		cfID,
		nil,
		oc,
		sc,
		0,
		common.DefaultMode,
		drainState,
		testDefaultBalanceMoveBatchSize,
	)
	_ = s.Execute()

	require.Equal(t, 0, oc.OperatorSize())
}

func TestBalanceSchedulerUsesConfiguredMoveBatchSize(t *testing.T) {
	cfID, nodeManager, oc, sc, drainState, self := newDrainSchedulerTestHarness(t)
	target := node.ID("target")
	dest := node.ID("dest")

	nodeManager.GetAliveNodes()[self] = &node.Info{ID: self}
	nodeManager.GetAliveNodes()[target] = &node.Info{ID: target}
	nodeManager.GetAliveNodes()[dest] = &node.Info{ID: dest}

	for i := 1; i <= 20; i++ {
		addReplicatingSpan(t, cfID, sc, int64(i), target)
	}
	drainState.SetSelfNodeID(self)

	s := NewBalanceScheduler(
		cfID,
		nil,
		oc,
		sc,
		0,
		common.DefaultMode,
		drainState,
		5,
	)
	_ = s.Execute()

	require.Equal(t, 5, oc.OperatorSize())
}

func TestBalanceSchedulerSkipsDuringDrainCooldown(t *testing.T) {
	cfID, nodeManager, oc, sc, drainState, self := newDrainSchedulerTestHarness(t)
	target := node.ID("target")
	dest := node.ID("dest")

	nodeManager.GetAliveNodes()[self] = &node.Info{ID: self}
	nodeManager.GetAliveNodes()[target] = &node.Info{ID: target}
	nodeManager.GetAliveNodes()[dest] = &node.Info{ID: dest}

	for i := 1; i <= 20; i++ {
		addReplicatingSpan(t, cfID, sc, int64(i), target)
	}
	drainState.SetSelfNodeID(self)
	drainState.SetDispatcherDrainTarget(target, 1)

	s := NewBalanceScheduler(
		cfID,
		nil,
		oc,
		sc,
		0,
		common.DefaultMode,
		drainState,
		testDefaultBalanceMoveBatchSize,
	)
	s.drainBalanceBlockedUntil = time.Time{}

	// First run sees an active drain and starts cooldown.
	_ = s.Execute()
	require.Equal(t, 0, oc.OperatorSize())

	// Drain is cleared but scheduler should still be blocked by cooldown.
	drainState.SetDispatcherDrainTarget("", 1)
	_ = s.Execute()
	require.Equal(t, 0, oc.OperatorSize())

	// Expire cooldown in test and verify scheduling resumes.
	s.drainBalanceBlockedUntil = time.Now().Add(-time.Millisecond)
	_ = s.Execute()
	require.Greater(t, oc.OperatorSize(), 0)
}
