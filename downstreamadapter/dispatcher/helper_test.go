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

package dispatcher

import (
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
)

func TestBlockEventStatusCompletedWatermark(t *testing.T) {
	var status BlockEventStatus
	ddl10 := &commonEvent.DDLEvent{FinishedTs: 10}
	syncpoint10 := commonEvent.NewSyncPointEvent(common.NewDispatcherID(), 10, 1, 0)
	ddl11 := &commonEvent.DDLEvent{FinishedTs: 11}

	status.recordCompleted(BlockEventIdentifier{CommitTs: 10, IsSyncPoint: false})
	require.True(t, status.isCompletedOrObsolete(ddl10))
	require.False(t, status.isCompletedOrObsolete(syncpoint10))
	require.False(t, status.isCompletedOrObsolete(ddl11))

	status.recordCompleted(BlockEventIdentifier{CommitTs: 10, IsSyncPoint: true})
	require.True(t, status.isCompletedOrObsolete(ddl10))
	require.True(t, status.isCompletedOrObsolete(syncpoint10))
	require.False(t, status.isCompletedOrObsolete(ddl11))
}

func TestBlockEventStatusActionMatchesSyncPointFlag(t *testing.T) {
	var status BlockEventStatus
	status.setBlockEvent(&commonEvent.DDLEvent{FinishedTs: 10}, heartbeatpb.BlockStage_WAITING)

	require.True(t, status.actionMatchs(&heartbeatpb.DispatcherAction{CommitTs: 10}))
	require.False(t, status.actionMatchs(&heartbeatpb.DispatcherAction{CommitTs: 10, IsSyncPoint: true}))
}
