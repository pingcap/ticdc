package maintainer

import (
	"time"

	"github.com/pingcap/ticdc/pkg/common"
)

const (
	// recoverableKafkaDispatcherRestartBackoffInit is the minimal interval between consecutive
	// dispatcher restarts triggered by recoverable Kafka errors.
	recoverableKafkaDispatcherRestartBackoffInit = 10 * time.Second
	// recoverableKafkaDispatcherRestartBackoffMax caps the exponential backoff interval.
	recoverableKafkaDispatcherRestartBackoffMax = time.Minute
	// recoverableKafkaDispatcherRestartMaxAttempts limits how many times we try dispatcher-level recovery
	// before downgrading to the changefeed-level error path.
	recoverableKafkaDispatcherRestartMaxAttempts = 6
	// recoverableKafkaDispatcherRestartResetInterval resets the restart budget after the dispatcher stays
	// healthy (no recoverable Kafka errors) for a while.
	recoverableKafkaDispatcherRestartResetInterval = 10 * time.Minute
)

type recoverableKafkaDispatcherRestartState struct {
	firstSeen time.Time
	lastSeen  time.Time

	lastRestart     time.Time
	restartAttempts int
}

func (m *Maintainer) getRecoverableKafkaDispatcherRestartState(
	dispatcherID common.DispatcherID,
	now time.Time,
) *recoverableKafkaDispatcherRestartState {
	if m.recoverableKafkaRestarts.dispatchers == nil {
		m.recoverableKafkaRestarts.dispatchers = make(map[common.DispatcherID]*recoverableKafkaDispatcherRestartState)
	}
	state, ok := m.recoverableKafkaRestarts.dispatchers[dispatcherID]
	if !ok {
		state = &recoverableKafkaDispatcherRestartState{
			firstSeen: now,
		}
		m.recoverableKafkaRestarts.dispatchers[dispatcherID] = state
	}
	return state
}

func (m *Maintainer) shouldDowngradeRecoverableKafkaDispatcherRestart(
	dispatcherID common.DispatcherID,
	now time.Time,
) (bool, int, time.Duration) {
	m.recoverableKafkaRestarts.Lock()
	defer m.recoverableKafkaRestarts.Unlock()

	state := m.getRecoverableKafkaDispatcherRestartState(dispatcherID, now)
	if !state.lastSeen.IsZero() && now.Sub(state.lastSeen) >= recoverableKafkaDispatcherRestartResetInterval {
		*state = recoverableKafkaDispatcherRestartState{firstSeen: now}
	}
	state.lastSeen = now

	if state.restartAttempts >= recoverableKafkaDispatcherRestartMaxAttempts {
		return true, state.restartAttempts, recoverableKafkaDispatcherRestartBackoff(state.restartAttempts)
	}

	backoff := recoverableKafkaDispatcherRestartBackoff(state.restartAttempts)
	if state.restartAttempts > 0 && now.Sub(state.lastRestart) < backoff {
		return true, state.restartAttempts, backoff
	}
	return false, state.restartAttempts, backoff
}

func (m *Maintainer) recordRecoverableKafkaDispatcherRestart(dispatcherID common.DispatcherID, now time.Time) {
	m.recoverableKafkaRestarts.Lock()
	defer m.recoverableKafkaRestarts.Unlock()

	state := m.getRecoverableKafkaDispatcherRestartState(dispatcherID, now)
	if state.firstSeen.IsZero() {
		state.firstSeen = now
	}
	state.lastSeen = now
	state.lastRestart = now
	state.restartAttempts++
}

func recoverableKafkaDispatcherRestartBackoff(restartAttempts int) time.Duration {
	if restartAttempts <= 0 {
		return 0
	}

	backoff := recoverableKafkaDispatcherRestartBackoffInit
	for i := 1; i < restartAttempts; i++ {
		if backoff >= recoverableKafkaDispatcherRestartBackoffMax {
			return recoverableKafkaDispatcherRestartBackoffMax
		}
		if backoff > recoverableKafkaDispatcherRestartBackoffMax/2 {
			return recoverableKafkaDispatcherRestartBackoffMax
		}
		backoff *= 2
	}
	if backoff > recoverableKafkaDispatcherRestartBackoffMax {
		return recoverableKafkaDispatcherRestartBackoffMax
	}
	return backoff
}
