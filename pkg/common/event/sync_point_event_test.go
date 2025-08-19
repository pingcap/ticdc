// Copyright 2025 PingCAP, Inc.
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

package event

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestSyncpointEvent(t *testing.T) {
	e := SyncPointEvent{
		State:        EventSenderStatePaused,
		DispatcherID: common.NewDispatcherID(),
		CommitTsList: []uint64{100, 102},
		Seq:          1000,
		Epoch:        10,
		Version:      SyncPointEventVersion,
	}
	data, err := e.Marshal()
	require.NoError(t, err)
	require.Len(t, data, int(e.GetSize()))

	var e2 SyncPointEvent
	err = e2.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, e, e2)
}

func TestSyncpointEventWithEmtpyCommitTsList(t *testing.T) {
	e := SyncPointEvent{
		State:        EventSenderStateNormal,
		DispatcherID: common.NewDispatcherID(),
		CommitTsList: []uint64{},
		Seq:          1000,
		Epoch:        10,
		Version:      SyncPointEventVersion,
	}
	data, err := e.Marshal()
	require.NoError(t, err)
	require.Len(t, data, int(e.GetSize()))

	var e2 SyncPointEvent
	err = e2.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, e, e2)
}
