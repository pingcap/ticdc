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

func TestScanPauseFollowsSyncPointLifecycle(t *testing.T) {
	d := &BasicDispatcher{}
	require.False(t, d.ShouldPauseScan())
	sp := commonEvent.NewSyncPointEvent(common.NewDispatcherID(), 100, 1, 1)
	for _, stage := range []heartbeatpb.BlockStage{heartbeatpb.BlockStage_WAITING, heartbeatpb.BlockStage_WRITING} {
		d.blockEventStatus.setBlockEvent(sp, stage)
		require.True(t, d.ShouldPauseScan())
	}
	d.blockEventStatus.clear()
	require.False(t, d.ShouldPauseScan())
	d.blockEventStatus.setBlockEvent(&commonEvent.DDLEvent{FinishedTs: 101}, heartbeatpb.BlockStage_WAITING)
	require.False(t, d.ShouldPauseScan(), "DDL must not activate the syncpoint quota override")
	d.blockEventStatus.setBlockEvent(sp, heartbeatpb.BlockStage_WAITING)
	d.mode = common.RedoMode
	require.False(t, d.ShouldPauseScan(), "redo must remain able to reach the barrier")
}
