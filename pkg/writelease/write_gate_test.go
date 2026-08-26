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

package writelease

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGateRequiresBothProofs(t *testing.T) {
	now := time.Unix(100, 0)
	gate := newGate(func() time.Time { return now })

	require.False(t, gate.IsWritable())
	require.True(t, gate.RenewP2P(now, P2PLeaseDuration))
	require.False(t, gate.IsWritable())
	require.True(t, gate.RenewEtcd(now, EtcdProofDuration))
	require.True(t, gate.IsWritable())

	now = now.Add(P2PLeaseDuration)
	require.False(t, gate.IsWritable())

	require.True(t, gate.RenewP2P(now, P2PLeaseDuration))
	require.False(t, gate.IsWritable())
	require.True(t, gate.RenewEtcd(now, EtcdProofDuration))
	require.True(t, gate.IsWritable())
}

func TestGateRejectsLateRenewalAndFenceIsIrreversible(t *testing.T) {
	now := time.Unix(100, 0)
	gate := newGate(func() time.Time { return now })

	require.False(t, gate.RenewP2P(now.Add(-P2PLeaseDuration), P2PLeaseDuration))
	require.False(t, gate.RenewEtcd(now.Add(-EtcdProofDuration), EtcdProofDuration))

	require.True(t, gate.RenewP2P(now, P2PLeaseDuration))
	require.True(t, gate.RenewEtcd(now, EtcdProofDuration))
	gate.Fence()
	require.False(t, gate.IsWritable())
	require.False(t, gate.RenewP2P(now, 2*P2PLeaseDuration))
	require.False(t, gate.RenewEtcd(now, 2*EtcdProofDuration))
}

func TestGateWaitUntilWritable(t *testing.T) {
	gate := NewGate()
	done := make(chan error, 1)
	go func() {
		done <- gate.WaitUntilWritable(context.Background())
	}()

	gate.RenewP2P(time.Now(), P2PLeaseDuration)
	select {
	case <-done:
		t.Fatal("wait returned without an etcd proof")
	case <-time.After(10 * time.Millisecond):
	}

	gate.RenewEtcd(time.Now(), EtcdProofDuration)
	require.NoError(t, <-done)

	gate.InvalidateP2P()
	require.False(t, gate.IsWritable())
}

func TestGateWaitReturnsOnContextCancellation(t *testing.T) {
	gate := NewGate()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, gate.WaitUntilWritable(ctx), context.Canceled)
}
