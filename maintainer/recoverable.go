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

package maintainer

import (
	"time"

	"github.com/pingcap/ticdc/pkg/common"
)

const (
	// recoverableMaxAttempts limits how many times we try dispatcher-level recovery
	// before downgrading to the changefeed-level error path.
	recoverableMaxAttempts = 6
	// recoverableResetInterval resets the restart budget after the dispatcher stays
	// healthy (no recoverable) for a while.
	recoverableResetInterval = 10 * time.Minute
)

type recoverableState struct {
	lastSeen time.Time

	restartAttempts int
}

type recoverableRestartDecision int

const (
	recoverableDispatcherRestartDecisionRestart recoverableRestartDecision = iota
	recoverableDispatcherRestartDecisionDowngrade
)

func (m *Maintainer) getRecoverableDispatcherRestartState(
	dispatcherID common.DispatcherID,
) *recoverableState {
	if m.recoverableDispatcherRestarts.dispatchers == nil {
		m.recoverableDispatcherRestarts.dispatchers = make(map[common.DispatcherID]*recoverableState)
	}
	state, ok := m.recoverableDispatcherRestarts.dispatchers[dispatcherID]
	if !ok {
		state = &recoverableState{}
		m.recoverableDispatcherRestarts.dispatchers[dispatcherID] = state
	}
	return state
}

func (m *Maintainer) getRecoverableDispatcherRestartDecision(dispatcherID common.DispatcherID) (recoverableRestartDecision, int) {
	now := time.Now()
	m.recoverableDispatcherRestarts.Lock()
	defer m.recoverableDispatcherRestarts.Unlock()

	state := m.getRecoverableDispatcherRestartState(dispatcherID)
	if !state.lastSeen.IsZero() && now.Sub(state.lastSeen) >= recoverableResetInterval {
		*state = recoverableState{}
	}
	state.lastSeen = now

	if state.restartAttempts >= recoverableMaxAttempts {
		return recoverableDispatcherRestartDecisionDowngrade, state.restartAttempts
	}
	// Intentionally no per-dispatcher time-based skip/backoff here.
	// Recover requests are deduplicated by dispatcher+epoch at sink side.
	// For maintainer, once a request arrives for a new epoch, we should execute it.
	// Same-epoch duplicates are handled by the in-flight operator existence check.
	return recoverableDispatcherRestartDecisionRestart, state.restartAttempts
}

func (m *Maintainer) recordRecoverableDispatcherRestart(dispatcherID common.DispatcherID) {
	m.recoverableDispatcherRestarts.Lock()
	defer m.recoverableDispatcherRestarts.Unlock()

	state := m.getRecoverableDispatcherRestartState(dispatcherID)
	state.lastSeen = time.Now()
	state.restartAttempts++
}
