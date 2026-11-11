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

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/orchestrator/util"
	"github.com/stretchr/testify/require"
)

func TestGlobalReactorStateCaptureRemoveTTL(t *testing.T) {
	t.Parallel()

	require.Equal(t, 10, NewGlobalState(etcd.DefaultCDCClusterID, 0).captureRemoveTTL)
	require.Equal(t, 10, NewGlobalState(etcd.DefaultCDCClusterID, 10).captureRemoveTTL)
	require.Equal(t, 15, NewGlobalState(etcd.DefaultCDCClusterID, 30).captureRemoveTTL)
}

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
	state.writeFencedReplacements[captureID] = struct{}{}
	mustUpdateCapture(t, state, captureID, "127.0.0.1:8301")

	state.UpdatePendingChange()

	require.Contains(t, state.Captures, captureID)
	require.Equal(t, "127.0.0.1:8301", state.Captures[captureID].AdvertiseAddr)
	require.Empty(t, removed)
	require.NotContains(t, state.toRemoveCaptures, captureID)
	require.NotContains(t, state.writeFencedReplacements, captureID)
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

func TestGlobalReactorStateRemovesWriteStoppedCaptureImmediately(t *testing.T) {
	t.Parallel()

	state := NewGlobalState(etcd.DefaultCDCClusterID, 0)
	captureID := config.CaptureID("capture-1")
	var removed []config.CaptureID
	state.SetOnCaptureRemoved(func(id config.CaptureID) {
		removed = append(removed, id)
	})

	info := &config.CaptureInfo{
		ID:                        captureID,
		AdvertiseAddr:             "127.0.0.1:8300",
		WriteLeaseProtocolVersion: heartbeatpb.CurrentWriteLeaseProtocolVersion,
	}
	mustUpdateCaptureInfo(t, state, info)
	info.WriteStopped = true
	mustUpdateCaptureInfo(t, state, info)
	mustDeleteCapture(t, state, captureID)

	require.NotContains(t, state.Captures, captureID)
	require.Equal(t, []config.CaptureID{captureID}, removed)
	require.NotContains(t, state.toRemoveCaptures, captureID)
}

func TestGlobalReactorStateIgnoresMismatchedWriteStoppedMarker(t *testing.T) {
	t.Parallel()

	state := NewGlobalState(etcd.DefaultCDCClusterID, 0)
	captureID := config.CaptureID("capture-1")
	mustUpdateCaptureAtKey(t, state, captureID, &config.CaptureInfo{
		ID:            "different-capture",
		AdvertiseAddr: "127.0.0.1:8300",
		WriteStopped:  true,
	})
	mustDeleteCapture(t, state, captureID)

	state.UpdatePendingChange()
	require.Contains(t, state.Captures, captureID)
	require.Contains(t, state.toRemoveCaptures, captureID)
}

func TestGlobalReactorStateShortensDelayForWriteFencedReplacement(t *testing.T) {
	t.Parallel()

	state := NewGlobalState(etcd.DefaultCDCClusterID, 0)
	oldCaptureID := config.CaptureID("capture-old")
	newCaptureID := config.CaptureID("capture-new")
	oldCaptureInfo := &config.CaptureInfo{
		ID:                        oldCaptureID,
		AdvertiseAddr:             "127.0.0.1:8300",
		StartTimestamp:            1,
		WriteLeaseProtocolVersion: heartbeatpb.CurrentWriteLeaseProtocolVersion,
	}
	newCaptureInfo := &config.CaptureInfo{
		ID:                        newCaptureID,
		AdvertiseAddr:             oldCaptureInfo.AdvertiseAddr,
		StartTimestamp:            2,
		WriteLeaseProtocolVersion: heartbeatpb.CurrentWriteLeaseProtocolVersion,
	}

	mustUpdateCaptureInfo(t, state, oldCaptureInfo)
	mustDeleteCapture(t, state, oldCaptureID)
	state.toRemoveCaptures[oldCaptureID] = time.Now().Add(-time.Second)
	mustUpdateCaptureInfo(t, state, newCaptureInfo)

	state.UpdatePendingChange()
	require.Contains(t, state.Captures, oldCaptureID)
	require.Contains(t, state.writeFencedReplacements, oldCaptureID)

	state.toRemoveCaptures[oldCaptureID] = time.Now().Add(-6 * time.Second)
	state.UpdatePendingChange()
	require.NotContains(t, state.Captures, oldCaptureID)
	require.Contains(t, state.Captures, newCaptureID)
	require.NotContains(t, state.writeFencedReplacements, oldCaptureID)
}

func TestGlobalReactorStateDetectsReplacementRegisteredBeforeDelete(t *testing.T) {
	t.Parallel()

	state := NewGlobalState(etcd.DefaultCDCClusterID, 0)
	oldCaptureID := config.CaptureID("capture-old")
	newCaptureID := config.CaptureID("capture-new")
	for _, info := range []*config.CaptureInfo{
		{
			ID:                        oldCaptureID,
			AdvertiseAddr:             "127.0.0.1:8300",
			StartTimestamp:            1,
			WriteLeaseProtocolVersion: heartbeatpb.CurrentWriteLeaseProtocolVersion,
		},
		{
			ID:                        newCaptureID,
			AdvertiseAddr:             "127.0.0.1:8300",
			StartTimestamp:            2,
			WriteLeaseProtocolVersion: heartbeatpb.CurrentWriteLeaseProtocolVersion,
		},
	} {
		mustUpdateCaptureInfo(t, state, info)
	}

	mustDeleteCapture(t, state, oldCaptureID)
	require.Contains(t, state.writeFencedReplacements, oldCaptureID)
}

func TestGlobalReactorStateKeepsConservativeDelayWithoutMatchingCapability(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name              string
		oldProtocol       uint32
		newProtocol       uint32
		newAdvertiseAddr  string
		newStartTimestamp int64
	}{
		{
			name:              "legacy old capture",
			oldProtocol:       heartbeatpb.LegacyWriteLeaseProtocolVersion,
			newProtocol:       heartbeatpb.CurrentWriteLeaseProtocolVersion,
			newAdvertiseAddr:  "127.0.0.1:8300",
			newStartTimestamp: 2,
		},
		{
			name:              "legacy new capture",
			oldProtocol:       heartbeatpb.CurrentWriteLeaseProtocolVersion,
			newProtocol:       heartbeatpb.LegacyWriteLeaseProtocolVersion,
			newAdvertiseAddr:  "127.0.0.1:8300",
			newStartTimestamp: 2,
		},
		{
			name:              "different address",
			oldProtocol:       heartbeatpb.CurrentWriteLeaseProtocolVersion,
			newProtocol:       heartbeatpb.CurrentWriteLeaseProtocolVersion,
			newAdvertiseAddr:  "127.0.0.1:8301",
			newStartTimestamp: 2,
		},
		{
			name:              "older start timestamp",
			oldProtocol:       heartbeatpb.CurrentWriteLeaseProtocolVersion,
			newProtocol:       heartbeatpb.CurrentWriteLeaseProtocolVersion,
			newAdvertiseAddr:  "127.0.0.1:8300",
			newStartTimestamp: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			state := NewGlobalState(etcd.DefaultCDCClusterID, 0)
			oldCaptureID := config.CaptureID("capture-old")
			newCaptureID := config.CaptureID("capture-new")
			mustUpdateCaptureInfo(t, state, &config.CaptureInfo{
				ID:                        oldCaptureID,
				AdvertiseAddr:             "127.0.0.1:8300",
				StartTimestamp:            1,
				WriteLeaseProtocolVersion: tc.oldProtocol,
			})
			mustDeleteCapture(t, state, oldCaptureID)
			mustUpdateCaptureInfo(t, state, &config.CaptureInfo{
				ID:                        newCaptureID,
				AdvertiseAddr:             tc.newAdvertiseAddr,
				StartTimestamp:            tc.newStartTimestamp,
				WriteLeaseProtocolVersion: tc.newProtocol,
			})

			state.toRemoveCaptures[oldCaptureID] = time.Now().Add(-6 * time.Second)
			state.UpdatePendingChange()
			require.Contains(t, state.Captures, oldCaptureID)
			require.NotContains(t, state.writeFencedReplacements, oldCaptureID)

			state.toRemoveCaptures[oldCaptureID] = time.Now().Add(-11 * time.Second)
			state.UpdatePendingChange()
			require.NotContains(t, state.Captures, oldCaptureID)
		})
	}
}

func mustUpdateCapture(
	t *testing.T,
	state *GlobalReactorState,
	captureID config.CaptureID,
	advertiseAddr string,
) {
	t.Helper()

	mustUpdateCaptureInfo(t, state, &config.CaptureInfo{
		ID:            captureID,
		AdvertiseAddr: advertiseAddr,
	})
}

func mustUpdateCaptureInfo(t *testing.T, state *GlobalReactorState, info *config.CaptureInfo) {
	t.Helper()
	mustUpdateCaptureAtKey(t, state, info.ID, info)
}

func mustUpdateCaptureAtKey(
	t *testing.T,
	state *GlobalReactorState,
	captureID config.CaptureID,
	info *config.CaptureInfo,
) {
	t.Helper()

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
