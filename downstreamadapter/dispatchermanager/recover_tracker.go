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

package dispatchermanager

import (
	"sync"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/recoverable"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// recoverEventChSize is set to 1024 to absorb short recover-event bursts and
	// keep non-blocking reporting from dropping events under transient pressure.
	recoverEventChSize = 1024
	// recoverLifecycleResendInterval controls how frequently DM re-sends pending
	// recover requests. It reduces maintainer-switch silent-loss windows.
	recoverLifecycleResendInterval = 5 * time.Second
	// recoverLifecycleCheckInterval is 1 minute to keep timeout scanning cheap,
	// while still detecting stuck recover requests in bounded time.
	recoverLifecycleCheckInterval = time.Minute
	// recoverLifecyclePendingTimeout is 10 minutes to tolerate maintainer
	// migration/bootstrap jitter, but still guarantee eventual changefeed fallback.
	recoverLifecyclePendingTimeout = 10 * time.Minute
)

type recoverDispatcherIdentity struct {
	id              common.DispatcherID
	epoch           uint64
	maintainerEpoch uint64
}

func newRecoverDispatcherIdentity(
	id common.DispatcherID,
	epoch uint64,
	maintainerEpoch uint64,
) recoverDispatcherIdentity {
	return recoverDispatcherIdentity{
		id:              id,
		epoch:           epoch,
		maintainerEpoch: maintainerEpoch,
	}
}

func (r recoverDispatcherIdentity) equal(other recoverDispatcherIdentity) bool {
	return r.id == other.id &&
		r.epoch == other.epoch &&
		r.maintainerEpoch == other.maintainerEpoch
}

// recoverTracker tracks dispatchers with recover requests that are waiting for
// recover lifecycle completion (remove old instance, then recreate new instance).
type recoverTracker struct {
	mu sync.Mutex
	// pending stores dispatchers that are waiting for recover completion.
	// Value records pending lifecycle state for this dispatcher.
	pending map[common.DispatcherID]recoverPendingState

	changefeedID   common.ChangeFeedID
	pendingGauge   prometheus.Gauge
	timeoutCounter prometheus.Counter
}

type recoverPendingState struct {
	identity  recoverDispatcherIdentity
	firstSeen time.Time
	removed   bool
}

func newRecoverTracker(changefeedID common.ChangeFeedID) *recoverTracker {
	tracker := &recoverTracker{
		pending:        make(map[common.DispatcherID]recoverPendingState),
		changefeedID:   changefeedID,
		pendingGauge:   metrics.DispatcherManagerRecoverPendingGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name()),
		timeoutCounter: metrics.DispatcherManagerRecoverPendingTimeoutCount.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name()),
	}
	return tracker
}

// add marks dispatchers as pending recover from now.
// Use this in production path right after recover request is enqueued.
func (t *recoverTracker) add(identities []recoverDispatcherIdentity) {
	t.addAt(identities, time.Now())
}

// addAt marks dispatchers as pending recover at a specified timestamp.
// It exists for deterministic timeout tests.
func (t *recoverTracker) addAt(identities []recoverDispatcherIdentity, at time.Time) {
	if len(identities) == 0 {
		return
	}

	t.mu.Lock()
	for _, identity := range identities {
		dispatcherID := identity.id
		if _, ok := t.pending[dispatcherID]; ok {
			continue
		}
		t.pending[dispatcherID] = recoverPendingState{
			identity:  identity,
			firstSeen: at,
			removed:   false,
		}
	}
	if t.pendingGauge != nil {
		t.pendingGauge.Set(float64(len(t.pending)))
	}
	t.mu.Unlock()
}

// filterNonPending returns dispatcher IDs that are not currently in pending state.
// Caller can use it as a pre-send gate to avoid duplicate recover requests.
func (t *recoverTracker) filterNonPending(dispatchers []recoverable.DispatcherEpoch) []recoverable.DispatcherEpoch {
	if len(dispatchers) == 0 {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	nonPending := make([]recoverable.DispatcherEpoch, 0, len(dispatchers))
	for _, dispatcher := range dispatchers {
		dispatcherID := dispatcher.DispatcherID
		if _, ok := t.pending[dispatcherID]; ok {
			continue
		}
		nonPending = append(nonPending, dispatcher)
	}
	return nonPending
}

// contains checks whether a dispatcher is currently pending recover.
func (t *recoverTracker) contains(dispatcherID common.DispatcherID) bool {
	t.mu.Lock()
	_, ok := t.pending[dispatcherID]
	t.mu.Unlock()
	return ok
}

// markRemoved marks a pending dispatcher as removed.
// It returns true when dispatcher is pending and mark succeeds.
func (t *recoverTracker) markRemoved(dispatcherID common.DispatcherID) bool {
	t.mu.Lock()
	state, ok := t.pending[dispatcherID]
	if !ok {
		t.mu.Unlock()
		return false
	}
	if !state.removed {
		state.removed = true
	}
	t.pending[dispatcherID] = state
	t.mu.Unlock()
	return true
}

// ack clears pending state when dispatcher has been recreated after removal.
// It returns true only for dispatchers in removed stage.
func (t *recoverTracker) ack(dispatcherID common.DispatcherID) bool {
	t.mu.Lock()
	state, ok := t.pending[dispatcherID]
	if !ok || !state.removed {
		t.mu.Unlock()
		return false
	}
	delete(t.pending, dispatcherID)
	if t.pendingGauge != nil {
		t.pendingGauge.Set(float64(len(t.pending)))
	}
	t.mu.Unlock()
	return true
}

// pendingDispatcherIDs returns a snapshot of pending dispatcher IDs.
func (t *recoverTracker) pendingDispatcherIDs() []common.DispatcherID {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.pending) == 0 {
		return nil
	}

	dispatcherIDs := make([]common.DispatcherID, 0, len(t.pending))
	for dispatcherID := range t.pending {
		dispatcherIDs = append(dispatcherIDs, dispatcherID)
	}
	return dispatcherIDs
}

// pendingIdentities returns a snapshot of pending recover identities.
func (t *recoverTracker) pendingIdentities() []recoverDispatcherIdentity {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.pending) == 0 {
		return nil
	}

	identities := make([]recoverDispatcherIdentity, 0, len(t.pending))
	for _, state := range t.pending {
		identities = append(identities, state.identity)
	}
	return identities
}

// remove clears pending state for a dispatcher.
// It returns true when dispatcher was pending before this call
func (t *recoverTracker) remove(dispatcherID common.DispatcherID) bool {
	t.mu.Lock()
	_, existed := t.pending[dispatcherID]
	delete(t.pending, dispatcherID)
	if t.pendingGauge != nil {
		t.pendingGauge.Set(float64(len(t.pending)))
	}
	t.mu.Unlock()
	return existed
}

// hasIdentity checks whether this recover identity is still pending.
func (t *recoverTracker) hasIdentity(identity recoverDispatcherIdentity) bool {
	t.mu.Lock()
	state, ok := t.pending[identity.id]
	t.mu.Unlock()
	if !ok {
		return false
	}
	return state.identity.equal(identity)
}

// removeByIdentity clears pending state only when recover identity matches.
func (t *recoverTracker) removeByIdentity(identity recoverDispatcherIdentity) bool {
	t.mu.Lock()
	state, existed := t.pending[identity.id]
	if !existed {
		t.mu.Unlock()
		return false
	}
	if !state.identity.equal(identity) {
		t.mu.Unlock()
		return false
	}
	delete(t.pending, identity.id)
	if t.pendingGauge != nil {
		t.pendingGauge.Set(float64(len(t.pending)))
	}
	t.mu.Unlock()
	return true
}

// removeIfRemoved clears pending state only when this dispatcher has entered
// removed stage.
func (t *recoverTracker) removeIfRemoved(dispatcherID common.DispatcherID) bool {
	t.mu.Lock()
	state, ok := t.pending[dispatcherID]
	if !ok {
		t.mu.Unlock()
		return false
	}
	if !state.removed {
		t.mu.Unlock()
		return false
	}
	delete(t.pending, dispatcherID)
	if t.pendingGauge != nil {
		t.pendingGauge.Set(float64(len(t.pending)))
	}
	t.mu.Unlock()
	return true
}

// takeExpired removes and returns pending dispatchers that exceed the default
// recover pending timeout.
// Caller can trigger changefeed-level fallback based on returned IDs.
func (t *recoverTracker) takeExpired() []common.DispatcherID {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.pending) == 0 {
		return nil
	}

	expired := make([]common.DispatcherID, 0)
	for dispatcherID, state := range t.pending {
		if now.Sub(state.firstSeen) < recoverLifecyclePendingTimeout {
			continue
		}
		expired = append(expired, dispatcherID)
		delete(t.pending, dispatcherID)
	}
	if len(expired) > 0 && t.timeoutCounter != nil {
		t.timeoutCounter.Add(float64(len(expired)))
	}
	if t.pendingGauge != nil {
		t.pendingGauge.Set(float64(len(t.pending)))
	}
	return expired
}

// close releases metrics labels and disables tracker state updates.
func (t *recoverTracker) close() {
	metrics.DispatcherManagerRecoverPendingGauge.DeleteLabelValues(t.changefeedID.Keyspace(), t.changefeedID.Name())
	metrics.DispatcherManagerRecoverPendingTimeoutCount.DeleteLabelValues(t.changefeedID.Keyspace(), t.changefeedID.Name())

	t.mu.Lock()
	t.pendingGauge = nil
	t.timeoutCounter = nil
	t.pending = nil
	t.mu.Unlock()
}
