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
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// recoverEventChSize is set to 1024 to absorb short recover-event bursts and
	// keep non-blocking reporting from dropping events under transient pressure.
	recoverEventChSize = 1024
	// recoverLifecycleCheckInterval is 1 minute to keep timeout scanning cheap,
	// while still detecting stuck recover requests in bounded time.
	recoverLifecycleCheckInterval = time.Minute
	// recoverLifecyclePendingTimeout is 10 minutes to tolerate maintainer
	// migration/bootstrap jitter, but still guarantee eventual changefeed fallback.
	recoverLifecyclePendingTimeout = 10 * time.Minute
)

// recoverTracker tracks dispatchers with recover requests that are waiting for
// a new working status to confirm lifecycle transition.
type recoverTracker struct {
	mu sync.Mutex
	// pending stores dispatchers that are waiting for recover completion.
	// Value is the timestamp when dispatcher entered pending state.
	pending map[common.DispatcherID]time.Time

	changefeedID   common.ChangeFeedID
	pendingGauge   prometheus.Gauge
	timeoutCounter prometheus.Counter
}

func newRecoverTracker(changefeedID common.ChangeFeedID) *recoverTracker {
	tracker := &recoverTracker{
		pending:        make(map[common.DispatcherID]time.Time),
		changefeedID:   changefeedID,
		pendingGauge:   metrics.DispatcherManagerRecoverPendingGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name()),
		timeoutCounter: metrics.DispatcherManagerRecoverPendingTimeoutCount.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name()),
	}
	return tracker
}

// add marks dispatchers as pending recover from now.
// Use this in production path right after recover request is enqueued.
func (t *recoverTracker) add(dispatcherIDs []common.DispatcherID) {
	t.addAt(dispatcherIDs, time.Now())
}

// addAt marks dispatchers as pending recover at a specified timestamp.
// It exists for deterministic timeout tests.
func (t *recoverTracker) addAt(dispatcherIDs []common.DispatcherID, at time.Time) {
	if len(dispatcherIDs) == 0 {
		return
	}

	t.mu.Lock()
	for _, dispatcherID := range dispatcherIDs {
		t.pending[dispatcherID] = at
	}
	if t.pendingGauge != nil {
		t.pendingGauge.Set(float64(len(t.pending)))
	}
	t.mu.Unlock()
}

// filterNonPending returns dispatcher IDs that are not currently in pending state.
// Caller can use it as a pre-send gate to avoid duplicate recover requests.
func (t *recoverTracker) filterNonPending(dispatcherIDs []common.DispatcherID) []common.DispatcherID {
	if len(dispatcherIDs) == 0 {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	nonPending := make([]common.DispatcherID, 0, len(dispatcherIDs))
	for _, dispatcherID := range dispatcherIDs {
		if _, ok := t.pending[dispatcherID]; ok {
			continue
		}
		nonPending = append(nonPending, dispatcherID)
	}
	return nonPending
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
	for dispatcherID, firstSeen := range t.pending {
		if now.Sub(firstSeen) < recoverLifecyclePendingTimeout {
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
