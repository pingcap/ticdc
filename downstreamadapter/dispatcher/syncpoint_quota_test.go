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

package dispatcher

import (
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
)

func TestPendingSyncPointFollowsBarrierLifecycle(t *testing.T) {
	d := &BasicDispatcher{}
	require.Zero(t, d.GetPendingSyncPointTs())
	sp := commonEvent.NewSyncPointEvent(common.NewDispatcherID(), 100, 1, 1)
	for _, stage := range []heartbeatpb.BlockStage{heartbeatpb.BlockStage_WAITING, heartbeatpb.BlockStage_WRITING} {
		d.blockEventStatus.setBlockEvent(sp, stage)
		require.Equal(t, uint64(100), d.GetPendingSyncPointTs())
	}
	d.blockEventStatus.clear()
	require.Zero(t, d.GetPendingSyncPointTs())
	d.blockEventStatus.setBlockEvent(&commonEvent.DDLEvent{FinishedTs: 101}, heartbeatpb.BlockStage_WAITING)
	require.Zero(t, d.GetPendingSyncPointTs(), "DDL must not activate the syncpoint quota override")
	d.blockEventStatus.setBlockEvent(sp, heartbeatpb.BlockStage_WAITING)
	d.mode = common.RedoMode
	require.Zero(t, d.GetPendingSyncPointTs(), "redo must remain able to reach the barrier")
}
