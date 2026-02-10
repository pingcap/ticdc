package maintainer

import (
	"time"

	"github.com/pingcap/ticdc/pkg/common"
)

const (
	// recoverableDispatcherRestartMaxAttempts limits how many times we try dispatcher-level recovery
	// before downgrading to the changefeed-level error path.
	recoverableDispatcherRestartMaxAttempts = 6
	// recoverableDispatcherRestartResetInterval resets the restart budget after the dispatcher stays
	// healthy (no recoverable sink errors) for a while.
	recoverableDispatcherRestartResetInterval = 10 * time.Minute
)

type recoverableDispatcherRestartState struct {
	lastSeen time.Time

	restartAttempts int
}

type recoverableDispatcherRestartDecision int

const (
	recoverableDispatcherRestartDecisionRestart recoverableDispatcherRestartDecision = iota
	recoverableDispatcherRestartDecisionDowngrade
)

func (m *Maintainer) getRecoverableDispatcherRestartState(
	dispatcherID common.DispatcherID,
) *recoverableDispatcherRestartState {
	if m.recoverableDispatcherRestarts.dispatchers == nil {
		m.recoverableDispatcherRestarts.dispatchers = make(map[common.DispatcherID]*recoverableDispatcherRestartState)
	}
	state, ok := m.recoverableDispatcherRestarts.dispatchers[dispatcherID]
	if !ok {
		state = &recoverableDispatcherRestartState{}
		m.recoverableDispatcherRestarts.dispatchers[dispatcherID] = state
	}
	return state
}

func (m *Maintainer) getRecoverableDispatcherRestartDecision(
	dispatcherID common.DispatcherID,
	now time.Time,
) (recoverableDispatcherRestartDecision, int) {
	m.recoverableDispatcherRestarts.Lock()
	defer m.recoverableDispatcherRestarts.Unlock()

	state := m.getRecoverableDispatcherRestartState(dispatcherID)
	if !state.lastSeen.IsZero() && now.Sub(state.lastSeen) >= recoverableDispatcherRestartResetInterval {
		*state = recoverableDispatcherRestartState{}
	}
	state.lastSeen = now

	if state.restartAttempts >= recoverableDispatcherRestartMaxAttempts {
		return recoverableDispatcherRestartDecisionDowngrade, state.restartAttempts
	}
	// Intentionally no per-dispatcher time-based skip/backoff here.
	// Recover requests are deduplicated by dispatcher+epoch at sink side.
	// For maintainer, once a request arrives for a new epoch, we should execute it.
	// Same-epoch duplicates are handled by the in-flight operator existence check.
	return recoverableDispatcherRestartDecisionRestart, state.restartAttempts
}

func (m *Maintainer) recordRecoverableDispatcherRestart(dispatcherID common.DispatcherID, now time.Time) {
	m.recoverableDispatcherRestarts.Lock()
	defer m.recoverableDispatcherRestarts.Unlock()

	state := m.getRecoverableDispatcherRestartState(dispatcherID)
	state.lastSeen = now
	state.restartAttempts++
}
