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

func TestBalanceSchedulerSkipsWhenDrainActive(t *testing.T) {
	cfID, nodeManager, oc, sc, self := newDrainSchedulerTestHarness(t)
	target := node.ID("target")
	dest := node.ID("dest")

	nodeManager.GetAliveNodes()[self] = &node.Info{ID: self}
	nodeManager.GetAliveNodes()[target] = &node.Info{ID: target}
	nodeManager.GetAliveNodes()[dest] = &node.Info{ID: dest}

	for i := 1; i <= 20; i++ {
		addReplicatingSpan(t, cfID, sc, int64(i), target)
	}

	s := NewBalanceScheduler(
		cfID,
		100,
		nil,
		oc,
		sc,
		0,
		common.DefaultMode,
		func() (node.ID, uint64) { return target, 1 },
	)
	_ = s.Execute()

	require.Equal(t, 0, oc.OperatorSize())
}

func TestBalanceSchedulerCapsMoveOperatorsPerRound(t *testing.T) {
	cfID, nodeManager, oc, sc, self := newDrainSchedulerTestHarness(t)
	target := node.ID("target")
	dest := node.ID("dest")

	nodeManager.GetAliveNodes()[self] = &node.Info{ID: self}
	nodeManager.GetAliveNodes()[target] = &node.Info{ID: target}
	nodeManager.GetAliveNodes()[dest] = &node.Info{ID: dest}

	for i := 1; i <= 20; i++ {
		addReplicatingSpan(t, cfID, sc, int64(i), target)
	}

	s := NewBalanceScheduler(
		cfID,
		100,
		nil,
		oc,
		sc,
		0,
		common.DefaultMode,
		func() (node.ID, uint64) { return "", 0 },
	)
	_ = s.Execute()

	require.Equal(t, maxBalanceMovePerRound, oc.OperatorSize())
}

func TestBalanceSchedulerSkipsDuringDrainCooldown(t *testing.T) {
	cfID, nodeManager, oc, sc, self := newDrainSchedulerTestHarness(t)
	target := node.ID("target")
	dest := node.ID("dest")

	nodeManager.GetAliveNodes()[self] = &node.Info{ID: self}
	nodeManager.GetAliveNodes()[target] = &node.Info{ID: target}
	nodeManager.GetAliveNodes()[dest] = &node.Info{ID: dest}

	for i := 1; i <= 20; i++ {
		addReplicatingSpan(t, cfID, sc, int64(i), target)
	}

	drainTarget := target
	s := NewBalanceScheduler(
		cfID,
		100,
		nil,
		oc,
		sc,
		0,
		common.DefaultMode,
		func() (node.ID, uint64) { return drainTarget, 1 },
	)
	s.drainBalanceBlockedUntil = time.Time{}

	// First run sees an active drain and starts cooldown.
	_ = s.Execute()
	require.Equal(t, 0, oc.OperatorSize())

	// Drain is cleared but scheduler should still be blocked by cooldown.
	drainTarget = ""
	_ = s.Execute()
	require.Equal(t, 0, oc.OperatorSize())

	// Expire cooldown in test and verify scheduling resumes.
	s.drainBalanceBlockedUntil = time.Now().Add(-time.Millisecond)
	_ = s.Execute()
	require.Greater(t, oc.OperatorSize(), 0)
}
