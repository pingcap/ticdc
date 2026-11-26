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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package maintainer

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestMeteringCollectorMultiDMAndAckGating(t *testing.T) {
	mc := newMeteringCollector()
	mc.SetInitialState(10, 0)

	n1 := node.ID("node-1")
	n2 := node.ID("node-2")
	activeCalls := 0
	getActive := func() []node.ID {
		activeCalls++
		return []node.ID{n1, n2}
	}
	now := time.Now()

	// Both DMs advertise support but have no sealed data for (10, 1) yet.
	res := mc.OnHeartbeat(n1, true, 0, 0, 0, getActive, now)
	require.NotNil(t, res.cmd)
	require.Equal(t, uint64(10), res.cmd.epoch)
	require.Equal(t, uint64(1), res.cmd.seq)
	require.False(t, res.roundCompleted)
	require.Equal(t, 1, activeCalls)

	res = mc.OnHeartbeat(n2, true, 0, 0, 0, getActive, now)
	require.NotNil(t, res.cmd)
	require.Equal(t, uint64(10), res.cmd.epoch)
	require.Equal(t, uint64(1), res.cmd.seq)
	require.False(t, res.roundCompleted)

	// n1 reports round 1; round is still incomplete until n2 reports.
	res = mc.OnHeartbeat(n1, true, 10, 1, 300, getActive, now)
	require.Nil(t, res.cmd)
	require.False(t, res.roundCompleted)
	_, ok := mc.GetStatusReport()
	require.False(t, ok)

	// n2 reports round 1 -> round completes with sum 300 + 700 = 1000.
	res = mc.OnHeartbeat(n2, true, 10, 1, 700, getActive, now)
	require.Nil(t, res.cmd)
	require.True(t, res.roundCompleted)
	report, ok := mc.GetStatusReport()
	require.True(t, ok)
	require.Equal(t, uint64(10), report.epoch)
	require.Equal(t, uint64(1), report.seq)
	require.Equal(t, uint64(1000), report.bytes)

	// Before ACK arrives, collector must NOT advance targetSeq or recompute active nodes.
	callsBefore := activeCalls
	res = mc.OnHeartbeat(n1, true, 10, 1, 300, getActive, now)
	require.Nil(t, res.cmd)
	require.False(t, res.roundCompleted)
	require.Equal(t, callsBefore, activeCalls)

	// Coordinator ACKs (10, 1) -> collector advances to targetSeq = 2 after 1s interval.
	mc.OnAck(10, 1, now)
	_, ok = mc.GetStatusReport()
	require.False(t, ok)

	// Before roundInterval elapses, OnHeartbeat is throttled and skips active node lookup.
	res = mc.OnHeartbeat(n1, true, 10, 1, 300, getActive, now)
	require.Nil(t, res.cmd)
	require.False(t, res.roundCompleted)
	require.Equal(t, callsBefore, activeCalls)

	res = mc.OnHeartbeat(n1, true, 10, 1, 300, getActive, now.Add(defaultMeteringRoundInterval))
	require.NotNil(t, res.cmd)
	require.Equal(t, uint64(10), res.cmd.epoch)
	require.Equal(t, uint64(2), res.cmd.seq)
	require.False(t, res.roundCompleted)
	require.Equal(t, callsBefore+1, activeCalls)
}

func TestMeteringCollectorRecoveryAndMixedUpgrade(t *testing.T) {
	mc := newMeteringCollector()
	// Maintainer restarts inheriting (epoch=10, lastSeq=5) from Coordinator.
	mc.SetInitialState(10, 5)

	newDM := node.ID("new-dm")
	oldDM := node.ID("old-dm")
	otherNewDM := node.ID("other-new-dm")
	active := func() []node.ID { return []node.ID{newDM, oldDM, otherNewDM} }
	now := time.Now()

	// Rolling upgrade skips unsupported oldDM without stalling.
	mc.MarkNodeSupported(newDM)
	mc.MarkNodeSupported(otherNewDM)

	res := mc.OnHeartbeat(oldDM, false, 0, 0, 0, active, now)
	require.Nil(t, res.cmd)
	require.False(t, res.roundCompleted)

	res = mc.OnHeartbeat(newDM, true, 0, 0, 0, active, now)
	require.NotNil(t, res.cmd)
	require.Equal(t, uint64(10), res.cmd.epoch)
	require.Equal(t, uint64(6), res.cmd.seq)
	require.False(t, res.roundCompleted)

	// newDM reports round 6, then otherNewDM stalls past threshold -> reported with rate limit.
	res = mc.OnHeartbeat(newDM, true, 10, 6, 450, active, now)
	require.False(t, res.roundCompleted)
	require.Nil(t, res.stall)

	res = mc.OnHeartbeat(newDM, true, 10, 6, 450, active, now.Add(meteringRoundStallThreshold))
	require.False(t, res.roundCompleted)
	require.NotNil(t, res.stall)
	require.Equal(t, uint64(6), res.stall.targetSeq)
	require.Equal(t, []node.ID{otherNewDM}, res.stall.missingNodes)

	res = mc.OnHeartbeat(newDM, true, 10, 6, 450, active, now.Add(meteringRoundStallThreshold+10*time.Second))
	require.Nil(t, res.stall)

	completed := mc.RemoveNode(otherNewDM, []node.ID{newDM, oldDM})
	require.True(t, completed)
	report, ok := mc.GetStatusReport()
	require.True(t, ok)
	require.Equal(t, uint64(10), report.epoch)
	require.Equal(t, uint64(6), report.seq)
	require.Equal(t, uint64(450), report.bytes)

	// Coordinator failover bumps metering_epoch -> collector resets cleanly to seq=1.
	mc.SetEpoch(11)
	_, ok = mc.GetStatusReport()
	require.False(t, ok)

	// Stale SetEpoch(10) after SetEpoch(11) must be ignored and keep epoch=11 intact.
	mc.OnHeartbeat(newDM, true, 10, 6, 450, func() []node.ID { return []node.ID{newDM} }, now)
	mc.SetEpoch(10)
	res = mc.OnHeartbeat(newDM, true, 10, 6, 450, func() []node.ID { return []node.ID{newDM} }, now)
	require.NotNil(t, res.cmd)
	require.Equal(t, uint64(11), res.cmd.epoch)
	require.Equal(t, uint64(1), res.cmd.seq)
	require.False(t, res.roundCompleted)

	// SetEpoch(0) still disables metering cleanly.
	mc.SetEpoch(0)
	calls := 0
	res = mc.OnHeartbeat(newDM, true, 11, 1, 100, func() []node.ID {
		calls++
		return []node.ID{newDM}
	}, now)
	require.Nil(t, res.cmd)
	require.Zero(t, calls)
}
