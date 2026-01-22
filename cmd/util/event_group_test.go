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

package util

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestEventsGroupAppendForceMergesExistingCommitTs(t *testing.T) {
	// Scenario:
	// 1) An upstream transaction (commitTs=100) is split into multiple messages.
	// 2) Due to sink retry/restart, a later transaction (commitTs=200) is observed first.
	// 3) A "late" fragment of the commitTs=100 transaction arrives afterwards.
	//
	// The EventsGroup must merge the late fragment into the existing commitTs=100 event,
	// instead of turning it into a second commitTs=100 item (which would split one upstream
	// transaction into multiple downstream transactions).
	group := NewEventsGroup(0, 1)

	newDMLEvent := func(commitTs uint64) *commonEvent.DMLEvent {
		return &commonEvent.DMLEvent{
			CommitTs:  commitTs,
			RowTypes:  []common.RowType{common.RowTypeUpdate},
			Rows:      chunk.NewChunkWithCapacity(nil, 0),
			Length:    0,
			TableInfo: &common.TableInfo{},
		}
	}

	group.Append(newDMLEvent(100), false)
	group.Append(newDMLEvent(200), false)
	group.Append(newDMLEvent(100), true)

	require.Equal(t, uint64(200), group.HighWatermark)

	resolved := group.Resolve(150)
	require.Len(t, resolved, 1)
	require.Equal(t, uint64(100), resolved[0].CommitTs)
	require.Len(t, resolved[0].RowTypes, 2)
}
