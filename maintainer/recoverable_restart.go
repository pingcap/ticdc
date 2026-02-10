package maintainer

import (
	"time"

	"github.com/pingcap/ticdc/pkg/common"
)

const (
	// recoverableDispatcherRestartBackoffInit is the minimal interval between consecutive
	// dispatcher restarts triggered by recoverable sink errors.
	recoverableDispatcherRestartBackoffInit = 10 * time.Second
	// recoverableDispatcherRestartBackoffMax caps the exponential backoff interval.
	recoverableDispatcherRestartBackoffMax = time.Minute
	// recoverableDispatcherRestartMaxAttempts limits how many times we try dispatcher-level recovery
	// before downgrading to the changefeed-level error path.
	recoverableDispatcherRestartMaxAttempts = 6
	// recoverableDispatcherRestartResetInterval resets the restart budget after the dispatcher stays
	// healthy (no recoverable sink errors) for a while.
	recoverableDispatcherRestartResetInterval = 10 * time.Minute
)

type recoverableDispatcherRestartState struct {
	firstSeen time.Time
	lastSeen  time.Time

	lastRestart     time.Time
	restartAttempts int
}

type recoverableDispatcherRestartDecision int

const (
	recoverableDispatcherRestartDecisionRestart recoverableDispatcherRestartDecision = iota
	recoverableDispatcherRestartDecisionSkip
	recoverableDispatcherRestartDecisionDowngrade
)

func (m *Maintainer) getRecoverableDispatcherRestartState(
	dispatcherID common.DispatcherID,
	now time.Time,
) *recoverableDispatcherRestartState {
	if m.recoverableDispatcherRestarts.dispatchers == nil {
		m.recoverableDispatcherRestarts.dispatchers = make(map[common.DispatcherID]*recoverableDispatcherRestartState)
	}
	state, ok := m.recoverableDispatcherRestarts.dispatchers[dispatcherID]
	if !ok {
		state = &recoverableDispatcherRestartState{
			firstSeen: now,
		}
		m.recoverableDispatcherRestarts.dispatchers[dispatcherID] = state
	}
	return state
}

func (m *Maintainer) getRecoverableDispatcherRestartDecision(
	dispatcherID common.DispatcherID,
	now time.Time,
) (recoverableDispatcherRestartDecision, int, time.Duration) {
	m.recoverableDispatcherRestarts.Lock()
	defer m.recoverableDispatcherRestarts.Unlock()

	state := m.getRecoverableDispatcherRestartState(dispatcherID, now)
	if !state.lastSeen.IsZero() && now.Sub(state.lastSeen) >= recoverableDispatcherRestartResetInterval {
		*state = recoverableDispatcherRestartState{firstSeen: now}
	}
	state.lastSeen = now

	if state.restartAttempts >= recoverableDispatcherRestartMaxAttempts {
		return recoverableDispatcherRestartDecisionDowngrade, state.restartAttempts, recoverableDispatcherRestartBackoff(state.restartAttempts)
	}

	backoff := recoverableDispatcherRestartBackoff(state.restartAttempts)
	if state.restartAttempts > 0 && now.Sub(state.lastRestart) < backoff {
		return recoverableDispatcherRestartDecisionSkip, state.restartAttempts, backoff
	}
	return recoverableDispatcherRestartDecisionRestart, state.restartAttempts, backoff
}

func (m *Maintainer) recordRecoverableDispatcherRestart(dispatcherID common.DispatcherID, now time.Time) {
	m.recoverableDispatcherRestarts.Lock()
	defer m.recoverableDispatcherRestarts.Unlock()

	state := m.getRecoverableDispatcherRestartState(dispatcherID, now)
	if state.firstSeen.IsZero() {
		state.firstSeen = now
	}
	state.lastSeen = now
	state.lastRestart = now
	state.restartAttempts++
}

func recoverableDispatcherRestartBackoff(restartAttempts int) time.Duration {
	if restartAttempts <= 0 {
		return 0
	}

	backoff := recoverableDispatcherRestartBackoffInit
	for i := 1; i < restartAttempts; i++ {
		if backoff >= recoverableDispatcherRestartBackoffMax {
			return recoverableDispatcherRestartBackoffMax
		}
		if backoff > recoverableDispatcherRestartBackoffMax/2 {
			return recoverableDispatcherRestartBackoffMax
		}
		backoff *= 2
	}
	if backoff > recoverableDispatcherRestartBackoffMax {
		return recoverableDispatcherRestartBackoffMax
	}
	return backoff
}
