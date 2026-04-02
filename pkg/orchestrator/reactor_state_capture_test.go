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

package orchestrator

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/orchestrator/util"
	"github.com/stretchr/testify/require"
)

func TestGlobalReactorStateKeepsCaptureAfterReRegister(t *testing.T) {
	t.Parallel()

	state := NewGlobalState(etcd.DefaultCDCClusterID, 0)
	state.captureRemoveTTL = 10

	captureID := config.CaptureID("capture-1")
	var removed []config.CaptureID
	state.SetOnCaptureRemoved(func(id config.CaptureID) {
		removed = append(removed, id)
	})

	mustUpdateCapture(t, state, captureID, "127.0.0.1:8300")
	mustDeleteCapture(t, state, captureID)
	state.toRemoveCaptures[captureID] = time.Now().Add(-11 * time.Second)
	mustUpdateCapture(t, state, captureID, "127.0.0.1:8301")

	state.UpdatePendingChange()

	require.Contains(t, state.Captures, captureID)
	require.Equal(t, "127.0.0.1:8301", state.Captures[captureID].AdvertiseAddr)
	require.Empty(t, removed)
	require.NotContains(t, state.toRemoveCaptures, captureID)
}

func TestGlobalReactorStateRemovesCaptureAfterTombstoneExpires(t *testing.T) {
	t.Parallel()

	state := NewGlobalState(etcd.DefaultCDCClusterID, 0)
	state.captureRemoveTTL = 10

	captureID := config.CaptureID("capture-1")
	var removed []config.CaptureID
	state.SetOnCaptureRemoved(func(id config.CaptureID) {
		removed = append(removed, id)
	})

	mustUpdateCapture(t, state, captureID, "127.0.0.1:8300")
	mustDeleteCapture(t, state, captureID)

	state.UpdatePendingChange()
	require.Contains(t, state.Captures, captureID)
	require.Empty(t, removed)

	state.toRemoveCaptures[captureID] = time.Now().Add(-11 * time.Second)
	state.UpdatePendingChange()

	require.NotContains(t, state.Captures, captureID)
	require.Equal(t, []config.CaptureID{captureID}, removed)
	require.NotContains(t, state.toRemoveCaptures, captureID)
}

func mustUpdateCapture(
	t *testing.T,
	state *GlobalReactorState,
	captureID config.CaptureID,
	advertiseAddr string,
) {
	t.Helper()

	info := &config.CaptureInfo{
		ID:            captureID,
		AdvertiseAddr: advertiseAddr,
	}
	data, err := info.Marshal()
	require.NoError(t, err)

	err = state.Update(util.NewEtcdKey(etcd.GetEtcdKeyCaptureInfo(state.ClusterID, string(captureID))), data, false)
	require.NoError(t, err)
}

func mustDeleteCapture(t *testing.T, state *GlobalReactorState, captureID config.CaptureID) {
	t.Helper()

	err := state.Update(util.NewEtcdKey(etcd.GetEtcdKeyCaptureInfo(state.ClusterID, string(captureID))), nil, false)
	require.NoError(t, err)
}
