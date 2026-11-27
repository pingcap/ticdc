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
	var mc meteringCollector
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
	require.Equal(t, uint64(10), res.cmdEpoch)
	require.Equal(t, uint64(1), res.cmdSeq)
	require.False(t, res.roundCompleted)
	require.Equal(t, 1, activeCalls)

	res = mc.OnHeartbeat(n2, true, 0, 0, 0, getActive, now)
	require.Equal(t, uint64(10), res.cmdEpoch)
	require.Equal(t, uint64(1), res.cmdSeq)
	require.False(t, res.roundCompleted)

	// n1 reports round 1; round is still incomplete until n2 reports.
	res = mc.OnHeartbeat(n1, true, 10, 1, 300, getActive, now)
	require.Zero(t, res.cmdEpoch)
	require.False(t, res.roundCompleted)
	epoch, _, _ := mc.CompletedRound()
	require.Zero(t, epoch)

	// n2 reports round 1 -> round completes with sum 300 + 700 = 1000.
	res = mc.OnHeartbeat(n2, true, 10, 1, 700, getActive, now)
	require.Zero(t, res.cmdEpoch)
	require.True(t, res.roundCompleted)
	epoch, seq, bytes := mc.CompletedRound()
	require.Equal(t, uint64(10), epoch)
	require.Equal(t, uint64(1), seq)
	require.Equal(t, uint64(1000), bytes)

	// Before ACK arrives, collector must NOT advance targetSeq or recompute active nodes.
	callsBefore := activeCalls
	res = mc.OnHeartbeat(n1, true, 10, 1, 300, getActive, now)
	require.Zero(t, res.cmdEpoch)
	require.False(t, res.roundCompleted)
	require.Equal(t, callsBefore, activeCalls)

	// Coordinator ACKs (10, 1) -> collector advances to targetSeq = 2 after 1s interval.
	mc.OnAck(10, 1, now)
	epoch, _, _ = mc.CompletedRound()
	require.Zero(t, epoch)

	// Before roundInterval elapses, OnHeartbeat is throttled and skips active node lookup.
	res = mc.OnHeartbeat(n1, true, 10, 1, 300, getActive, now)
	require.Zero(t, res.cmdEpoch)
	require.False(t, res.roundCompleted)
	require.Equal(t, callsBefore, activeCalls)

	res = mc.OnHeartbeat(n1, true, 10, 1, 300, getActive, now.Add(defaultMeteringRoundInterval))
	require.Equal(t, uint64(10), res.cmdEpoch)
	require.Equal(t, uint64(2), res.cmdSeq)
	require.False(t, res.roundCompleted)
	require.Equal(t, callsBefore+1, activeCalls)
}

func TestMeteringCollectorRecoveryAndMixedUpgrade(t *testing.T) {
	var mc meteringCollector
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
	require.Zero(t, res.cmdEpoch)
	require.False(t, res.roundCompleted)

	res = mc.OnHeartbeat(newDM, true, 0, 0, 0, active, now)
	require.Equal(t, uint64(10), res.cmdEpoch)
	require.Equal(t, uint64(6), res.cmdSeq)
	require.False(t, res.roundCompleted)

	// newDM reports round 6, then otherNewDM stalls past threshold -> reported with rate limit.
	res = mc.OnHeartbeat(newDM, true, 10, 6, 450, active, now)
	require.False(t, res.roundCompleted)
	require.Empty(t, res.missingNodes)

	res = mc.OnHeartbeat(newDM, true, 10, 6, 450, active, now.Add(meteringRoundStallThreshold))
	require.False(t, res.roundCompleted)
	require.Equal(t, uint64(6), res.targetSeq)
	require.Equal(t, []node.ID{otherNewDM}, res.missingNodes)

	res = mc.OnHeartbeat(newDM, true, 10, 6, 450, active, now.Add(meteringRoundStallThreshold+10*time.Second))
	require.Empty(t, res.missingNodes)

	completed := mc.RemoveNode(otherNewDM, []node.ID{newDM, oldDM})
	require.True(t, completed)
	epoch, seq, bytes := mc.CompletedRound()
	require.Equal(t, uint64(10), epoch)
	require.Equal(t, uint64(6), seq)
	require.Equal(t, uint64(450), bytes)

	// Coordinator failover bumps metering_epoch -> collector resets cleanly to seq=1.
	mc.SetEpoch(11)
	epoch, _, _ = mc.CompletedRound()
	require.Zero(t, epoch)

	// Stale SetEpoch(10) after SetEpoch(11) must be ignored and keep epoch=11 intact.
	mc.OnHeartbeat(newDM, true, 10, 6, 450, func() []node.ID { return []node.ID{newDM} }, now)
	mc.SetEpoch(10)
	res = mc.OnHeartbeat(newDM, true, 10, 6, 450, func() []node.ID { return []node.ID{newDM} }, now)
	require.Equal(t, uint64(11), res.cmdEpoch)
	require.Equal(t, uint64(1), res.cmdSeq)
	require.False(t, res.roundCompleted)

	// SetEpoch(0) still disables metering cleanly.
	mc.SetEpoch(0)
	calls := 0
	res = mc.OnHeartbeat(newDM, true, 11, 1, 100, func() []node.ID {
		calls++
		return []node.ID{newDM}
	}, now)
	require.Zero(t, res.cmdEpoch)
	require.Zero(t, calls)
}

func TestMeteringCollectorOnAckClearsPartialRound(t *testing.T) {
	var mc meteringCollector
	mc.SetInitialState(10, 5)

	n1 := node.ID("node-1")
	n2 := node.ID("node-2")
	active := func() []node.ID { return []node.ID{n1, n2} }
	now := time.Now()

	mc.MarkNodeSupported(n1)
	mc.MarkNodeSupported(n2)

	// node-1 reports partial round 6, then a catch-up ACK advances targetSeq to 7.
	res := mc.OnHeartbeat(n1, true, 10, 6, 450, active, now)
	require.False(t, res.roundCompleted)

	mc.OnAck(10, 6, now)

	// node-2 reporting round 7 must NOT complete round 7 using node-1's stale seq=6 bytes.
	next := now.Add(defaultMeteringRoundInterval)
	res = mc.OnHeartbeat(n2, true, 10, 7, 200, active, next)
	require.False(t, res.roundCompleted)
	epoch, _, _ := mc.CompletedRound()
	require.Zero(t, epoch)

	// node-1 receives seal command for (10, 7) and completes round 7 cleanly.
	res = mc.OnHeartbeat(n1, true, 10, 6, 450, active, next)
	require.Equal(t, uint64(10), res.cmdEpoch)
	require.Equal(t, uint64(7), res.cmdSeq)
	require.False(t, res.roundCompleted)

	res = mc.OnHeartbeat(n1, true, 10, 7, 100, active, next)
	require.True(t, res.roundCompleted)
	epoch, seq, bytes := mc.CompletedRound()
	require.Equal(t, uint64(10), epoch)
	require.Equal(t, uint64(7), seq)
	require.Equal(t, uint64(300), bytes)
}
