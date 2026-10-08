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
	gate.SetP2PRequired(true)

	require.False(t, gate.IsWritable())
	require.Equal(t, BlockReasonBothExpired, gate.Status().Reason)
	require.True(t, gate.RenewP2P(now, P2PLeaseDuration))
	require.False(t, gate.IsWritable())
	require.Equal(t, BlockReasonEtcdExpired, gate.Status().Reason)
	require.True(t, gate.RenewEtcd(now, EtcdProofDuration))
	require.True(t, gate.IsWritable())
	require.Equal(t, BlockReasonWritable, gate.Status().Reason)

	now = now.Add(P2PLeaseDuration)
	require.False(t, gate.IsWritable())
	require.Equal(t, BlockReasonBothExpired, gate.Status().Reason)

	require.True(t, gate.RenewP2P(now, P2PLeaseDuration))
	require.False(t, gate.IsWritable())
	require.True(t, gate.RenewEtcd(now, EtcdProofDuration))
	require.True(t, gate.IsWritable())
}

func TestGateRejectsLateRenewalAndFenceIsIrreversible(t *testing.T) {
	now := time.Unix(100, 0)
	gate := newGate(func() time.Time { return now })
	gate.SetP2PRequired(true)

	require.False(t, gate.RenewP2P(now.Add(-P2PLeaseDuration), P2PLeaseDuration))
	require.False(t, gate.RenewEtcd(now.Add(-EtcdProofDuration), EtcdProofDuration))

	require.True(t, gate.RenewP2P(now, P2PLeaseDuration))
	require.True(t, gate.RenewEtcd(now, EtcdProofDuration))
	gate.Fence()
	require.False(t, gate.IsWritable())
	require.Equal(t, BlockReasonFenced, gate.Status().Reason)
	require.False(t, gate.RenewP2P(now, 2*P2PLeaseDuration))
	require.False(t, gate.RenewEtcd(now, 2*EtcdProofDuration))
}

func TestGateWaitUntilWritable(t *testing.T) {
	gate := NewGate()
	gate.SetP2PRequired(true)
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

func TestGateNotifiesOnlyWhenWritable(t *testing.T) {
	now := time.Unix(100, 0)
	gate := newGate(func() time.Time { return now })
	gate.SetP2PRequired(true)

	changed := gate.changed
	require.True(t, gate.RenewP2P(now, P2PLeaseDuration))
	select {
	case <-changed:
		t.Fatal("P2P renewal notified waiters while etcd proof was still expired")
	default:
	}

	require.True(t, gate.RenewEtcd(now, EtcdProofDuration))
	select {
	case <-changed:
	default:
		t.Fatal("etcd renewal did not notify waiters when the gate became writable")
	}

	changed = gate.changed
	require.True(t, gate.RenewEtcd(now.Add(time.Second), EtcdProofDuration))
	select {
	case <-changed:
		t.Fatal("etcd renewal notified waiters while the gate remained writable")
	default:
	}
}

func TestGateNegotiatesP2PPerCoordinator(t *testing.T) {
	now := time.Unix(100, 0)
	gate := newGate(func() time.Time { return now })

	// Legacy mode never requires a P2P grant, but it still requires fresh etcd
	// proof and remains protected by an irreversible local fence.
	require.False(t, gate.IsWritable())
	require.True(t, gate.RenewEtcd(now, EtcdProofDuration))
	require.True(t, gate.IsWritable())
	require.False(t, gate.Status().P2PRequired)

	gate.SetP2PRequired(true)
	require.False(t, gate.IsWritable())
	require.Equal(t, BlockReasonP2PExpired, gate.Status().Reason)
	require.True(t, gate.RenewP2P(now, P2PLeaseDuration))
	require.True(t, gate.IsWritable())

	gate.InvalidateP2P()
	gate.SetP2PRequired(false)
	require.True(t, gate.IsWritable())
}

func TestGateWaitReturnsOnContextCancellation(t *testing.T) {
	gate := NewGate()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, gate.WaitUntilWritable(ctx), context.Canceled)
}

func TestOptionalGatePreservesLegacyWrites(t *testing.T) {
	require.True(t, CanWrite(nil))
	require.NoError(t, WaitForWrite(t.Context(), nil))
}
