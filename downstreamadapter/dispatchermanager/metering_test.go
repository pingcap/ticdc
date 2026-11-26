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

package dispatchermanager

import (
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestDispatcherManagerMeteringStateMachine(t *testing.T) {
	t.Parallel()

	dm := createTestManager(t)
	dm.meta.maintainerID = "maintainer-1"
	dm.meta.maintainerEpoch = 3

	dm.metering.Counter().Add(120)
	epoch, seq, bytes := dm.metering.Report()
	require.Zero(t, epoch)
	require.Zero(t, seq)
	require.Zero(t, bytes)

	// New epoch & seq seals current into sealedBytes and swaps current to 0.
	dm.metering.HandleCommand(10, 1)
	epoch, seq, bytes = dm.metering.Report()
	require.Equal(t, uint64(10), epoch)
	require.Equal(t, uint64(1), seq)
	require.Equal(t, uint64(120), bytes)
	require.Zero(t, dm.metering.Counter().Load())

	// Receiving the same (epoch, seq) must be idempotent even if new bytes flushed.
	dm.metering.Counter().Add(45)
	dm.metering.HandleCommand(10, 1)
	epoch, seq, bytes = dm.metering.Report()
	require.Equal(t, uint64(10), epoch)
	require.Equal(t, uint64(1), seq)
	require.Equal(t, uint64(120), bytes)
	require.Equal(t, uint64(45), dm.metering.Counter().Load())

	// Older epoch/seq must be ignored.
	dm.metering.HandleCommand(9, 99)
	epoch, seq, bytes = dm.metering.Report()
	require.Equal(t, uint64(10), epoch)
	require.Equal(t, uint64(1), seq)
	require.Equal(t, uint64(120), bytes)

	// Higher seq in same epoch seals newly flushed 45 bytes.
	dm.metering.HandleCommand(10, 2)
	epoch, seq, bytes = dm.metering.Report()
	require.Equal(t, uint64(10), epoch)
	require.Equal(t, uint64(2), seq)
	require.Equal(t, uint64(45), bytes)
	require.Zero(t, dm.metering.Counter().Load())

	// Higher epoch (coordinator failover) resets seq cleanly and seals new bytes.
	dm.metering.Counter().Add(80)
	dm.metering.HandleCommand(11, 1)
	epoch, seq, bytes = dm.metering.Report()
	require.Equal(t, uint64(11), epoch)
	require.Equal(t, uint64(1), seq)
	require.Equal(t, uint64(80), bytes)

	// Without traffic metering attached on SharedInfo, heartbeats do not advertise support.
	req := dm.aggregateDispatcherHeartbeats(false)
	require.False(t, req.MeteringSupported)

	// Once attached on SharedInfo, aggregateDispatcherHeartbeats advertises capability and sealed values.
	dm.sharedInfo.SetTrafficMetering(nil, dm.metering.Counter())
	req = dm.aggregateDispatcherHeartbeats(false)
	require.True(t, req.MeteringSupported)
	require.Equal(t, uint64(11), req.MeteringEpoch)
	require.Equal(t, uint64(1), req.MeteringSeq)
	require.Equal(t, uint64(80), req.MeteringBytes)

	// HeartBeatResponseHandler forwards fenced commands and drops stale maintainer_epoch.
	handler := &HeartBeatResponseHandler{}
	dm.metering.Counter().Add(15)
	handler.Handle(dm, NewHeartBeatResponse(node.ID("stale-maintainer"), &heartbeatpb.HeartBeatResponse{
		ChangefeedID:    dm.changefeedID.ToPB(),
		MaintainerEpoch: 2,
		MeteringEpoch:   11,
		MeteringSeq:     2,
	}))
	_, seq, _ = dm.metering.Report()
	require.Equal(t, uint64(1), seq)

	handler.Handle(dm, NewHeartBeatResponse(node.ID("maintainer-1"), &heartbeatpb.HeartBeatResponse{
		ChangefeedID:    dm.changefeedID.ToPB(),
		MaintainerEpoch: 3,
		MeteringEpoch:   11,
		MeteringSeq:     2,
	}))
	epoch, seq, bytes = dm.metering.Report()
	require.Equal(t, uint64(11), epoch)
	require.Equal(t, uint64(2), seq)
	require.Equal(t, uint64(15), bytes)
}
