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
	active := []node.ID{n1, n2}
	now := time.Now()

	// Both DMs advertise support but have no sealed data for (10, 1) yet.
	sendCmd, cmdEpoch, cmdSeq, completed := mc.OnHeartbeat(n1, true, 0, 0, 0, active, now)
	require.True(t, sendCmd)
	require.Equal(t, uint64(10), cmdEpoch)
	require.Equal(t, uint64(1), cmdSeq)
	require.False(t, completed)

	sendCmd, cmdEpoch, cmdSeq, completed = mc.OnHeartbeat(n2, true, 0, 0, 0, active, now)
	require.True(t, sendCmd)
	require.Equal(t, uint64(10), cmdEpoch)
	require.Equal(t, uint64(1), cmdSeq)
	require.False(t, completed)

	// n1 reports round 1; round is still incomplete until n2 reports.
	sendCmd, _, _, completed = mc.OnHeartbeat(n1, true, 10, 1, 300, active, now)
	require.False(t, sendCmd)
	require.False(t, completed)
	ep, seq, bytes := mc.GetStatusReport()
	require.Zero(t, ep)
	require.Zero(t, seq)
	require.Zero(t, bytes)

	// n2 reports round 1 -> round completes with sum 300 + 700 = 1000.
	sendCmd, _, _, completed = mc.OnHeartbeat(n2, true, 10, 1, 700, active, now)
	require.False(t, sendCmd)
	require.True(t, completed)
	ep, seq, bytes = mc.GetStatusReport()
	require.Equal(t, uint64(10), ep)
	require.Equal(t, uint64(1), seq)
	require.Equal(t, uint64(1000), bytes)

	// Before ACK arrives, collector must NOT advance targetSeq to 2.
	sendCmd, _, _, completed = mc.OnHeartbeat(n1, true, 10, 1, 300, active, now)
	require.False(t, sendCmd)
	require.False(t, completed)

	// Coordinator ACKs (10, 1) -> collector advances to targetSeq = 2.
	mc.OnAck(10, 1, now)
	ep, seq, bytes = mc.GetStatusReport()
	require.Zero(t, ep)
	require.Zero(t, seq)
	require.Zero(t, bytes)

	// Before roundInterval elapses, OnHeartbeat is throttled; after 1s it starts targetSeq = 2.
	sendCmd, _, _, completed = mc.OnHeartbeat(n1, true, 10, 1, 300, active, now)
	require.False(t, sendCmd)
	require.False(t, completed)

	sendCmd, cmdEpoch, cmdSeq, completed = mc.OnHeartbeat(n1, true, 10, 1, 300, active, now.Add(defaultMeteringRoundInterval))
	require.True(t, sendCmd)
	require.Equal(t, uint64(10), cmdEpoch)
	require.Equal(t, uint64(2), cmdSeq)
	require.False(t, completed)
}

func TestMeteringCollectorRecoveryAndMixedUpgrade(t *testing.T) {
	mc := newMeteringCollector()
	// Maintainer restarts inheriting (epoch=10, lastSeq=5) from Coordinator.
	mc.SetInitialState(10, 5)

	newDM := node.ID("new-dm")
	oldDM := node.ID("old-dm")
	otherNewDM := node.ID("other-new-dm")
	active := []node.ID{newDM, oldDM, otherNewDM}
	now := time.Now()

	// Rolling upgrade skips unsupported oldDM without stalling.
	mc.MarkNodeSupported(newDM)
	mc.MarkNodeSupported(otherNewDM)

	sendCmd, cmdEpoch, cmdSeq, completed := mc.OnHeartbeat(oldDM, false, 0, 0, 0, active, now)
	require.False(t, sendCmd)
	require.False(t, completed)

	sendCmd, cmdEpoch, cmdSeq, completed = mc.OnHeartbeat(newDM, true, 0, 0, 0, active, now)
	require.True(t, sendCmd)
	require.Equal(t, uint64(10), cmdEpoch)
	require.Equal(t, uint64(6), cmdSeq)
	require.False(t, completed)

	// newDM reports round 6, then otherNewDM crashes and is removed from active nodes.
	_, _, _, completed = mc.OnHeartbeat(newDM, true, 10, 6, 450, active, now)
	require.False(t, completed)

	completed = mc.RemoveNode(otherNewDM, []node.ID{newDM, oldDM})
	require.True(t, completed)
	ep, seq, bytes := mc.GetStatusReport()
	require.Equal(t, uint64(10), ep)
	require.Equal(t, uint64(6), seq)
	require.Equal(t, uint64(450), bytes)

	// Coordinator failover bumps metering_epoch -> collector resets cleanly to seq=1.
	mc.SetEpoch(11)
	ep, seq, bytes = mc.GetStatusReport()
	require.Zero(t, ep)
	require.Zero(t, seq)
	require.Zero(t, bytes)

	sendCmd, cmdEpoch, cmdSeq, completed = mc.OnHeartbeat(newDM, true, 10, 6, 450, []node.ID{newDM}, now)
	require.True(t, sendCmd)
	require.Equal(t, uint64(11), cmdEpoch)
	require.Equal(t, uint64(1), cmdSeq)
	require.False(t, completed)
}
