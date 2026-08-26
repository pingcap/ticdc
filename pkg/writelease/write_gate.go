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
	"sync"
	"sync/atomic"
	"time"
)

const (
	NodeHeartbeatInterval = 500 * time.Millisecond
	P2PLeaseDuration      = 5 * time.Second
	EtcdProofDuration     = 5 * time.Second
)

// BlockReason explains why the capture write gate is closed.
type BlockReason string

const (
	BlockReasonWritable    BlockReason = "writable"
	BlockReasonP2PExpired  BlockReason = "p2p_expired"
	BlockReasonEtcdExpired BlockReason = "etcd_proof_expired"
	BlockReasonBothExpired BlockReason = "both_expired"
	BlockReasonFenced      BlockReason = "fenced"
)

// Status is a point-in-time view used for metrics and transition logs. Write
// admission still reads the immutable lease snapshot directly.
type Status struct {
	Reason             BlockReason
	Writable           bool
	P2PRemaining       time.Duration
	EtcdProofRemaining time.Duration
}

type leaseState struct {
	p2pValidUntil       time.Time
	etcdProofValidUntil time.Time
	fenced              bool
}

// Gate controls capture-wide admission of downstream side effects.
// Renewals publish immutable snapshots so the write path only needs an atomic
// load and local monotonic-clock comparisons.
type Gate struct {
	now func() time.Time

	state atomic.Pointer[leaseState]

	mu      sync.Mutex
	changed chan struct{}
}

// NewGate creates a fail-closed write gate. Both P2P and etcd proofs must be
// renewed before writes can be admitted.
func NewGate() *Gate {
	return newGate(time.Now)
}

func newGate(now func() time.Time) *Gate {
	g := &Gate{
		now:     now,
		changed: make(chan struct{}),
	}
	g.state.Store(&leaseState{})
	return g
}

// IsWritable returns whether both write proofs are fresh and the capture has
// not entered the irreversible fenced state.
func (g *Gate) IsWritable() bool {
	return g.isWritableAt(g.now())
}

// Status returns the current gate state and non-negative lease lifetimes.
func (g *Gate) Status() Status {
	now := g.now()
	state := g.state.Load()
	p2pRemaining := max(state.p2pValidUntil.Sub(now), 0)
	etcdRemaining := max(state.etcdProofValidUntil.Sub(now), 0)

	reason := BlockReasonWritable
	switch {
	case state.fenced:
		reason = BlockReasonFenced
	case p2pRemaining == 0 && etcdRemaining == 0:
		reason = BlockReasonBothExpired
	case p2pRemaining == 0:
		reason = BlockReasonP2PExpired
	case etcdRemaining == 0:
		reason = BlockReasonEtcdExpired
	}
	return Status{
		Reason:             reason,
		Writable:           reason == BlockReasonWritable,
		P2PRemaining:       p2pRemaining,
		EtcdProofRemaining: etcdRemaining,
	}
}

func (g *Gate) isWritableAt(now time.Time) bool {
	state := g.state.Load()
	return !state.fenced &&
		now.Before(state.p2pValidUntil) &&
		now.Before(state.etcdProofValidUntil)
}

// WaitUntilWritable blocks until writes are admitted again or ctx is done.
func (g *Gate) WaitUntilWritable(ctx context.Context) error {
	for {
		if g.IsWritable() {
			return nil
		}

		g.mu.Lock()
		changed := g.changed
		g.mu.Unlock()

		// Avoid missing a renewal between the first state check and loading the
		// notification channel.
		if g.IsWritable() {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// RenewP2P renews the coordinator-issued proof from the request send time.
// It rejects grants that were already expired when they arrived.
func (g *Gate) RenewP2P(requestSentAt time.Time, duration time.Duration) bool {
	return g.renew(requestSentAt.Add(duration), true)
}

// RenewEtcd renews the positive etcd proof from the TTL request send time.
// It rejects responses that were already expired when they arrived.
func (g *Gate) RenewEtcd(requestSentAt time.Time, duration time.Duration) bool {
	return g.renew(requestSentAt.Add(duration), false)
}

func (g *Gate) renew(validUntil time.Time, p2p bool) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !validUntil.After(g.now()) {
		return false
	}

	current := g.state.Load()
	if current.fenced {
		return false
	}
	if p2p && !validUntil.After(current.p2pValidUntil) {
		return false
	}
	if !p2p && !validUntil.After(current.etcdProofValidUntil) {
		return false
	}

	next := *current
	if p2p {
		next.p2pValidUntil = validUntil
	} else {
		next.etcdProofValidUntil = validUntil
	}
	g.publishLocked(&next)
	return true
}

// InvalidateP2P closes write admission until a fresh coordinator generation
// grants another P2P lease.
func (g *Gate) InvalidateP2P() {
	g.mu.Lock()
	defer g.mu.Unlock()

	current := g.state.Load()
	if current.p2pValidUntil.IsZero() {
		return
	}
	next := *current
	next.p2pValidUntil = time.Time{}
	g.publishLocked(&next)
}

// Fence irreversibly closes the gate for this capture process lifetime.
func (g *Gate) Fence() {
	g.mu.Lock()
	defer g.mu.Unlock()

	current := g.state.Load()
	if current.fenced {
		return
	}
	next := *current
	next.fenced = true
	g.publishLocked(&next)
}

// EtcdProofValidUntil returns the current positive proof deadline.
func (g *Gate) EtcdProofValidUntil() time.Time {
	return g.state.Load().etcdProofValidUntil
}

func (g *Gate) publishLocked(next *leaseState) {
	g.state.Store(next)
	close(g.changed)
	g.changed = make(chan struct{})
}
