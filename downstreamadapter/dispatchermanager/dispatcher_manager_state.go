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

// dispatcherManagerState transitions:
//
//	Initializing -> Running -> Closing -> Closed
//	      |                       ^
//	      +-----------------------+
type dispatcherManagerState uint32

const (
	// Initialization is in progress; the constructor owns cleanup.
	dispatcherManagerInitializing dispatcherManagerState = iota
	// Initialization is complete; the manager accepts new work.
	dispatcherManagerRunning
	// Shutdown has started; new work and resource registration are rejected.
	dispatcherManagerClosing
	// Shutdown is complete; TryClose returns true.
	// Changefeed deletion cleanup may still run asynchronously.
	dispatcherManagerClosed
)

func (e *DispatcherManager) isInitializing() bool {
	e.lifecycleMu.Lock()
	defer e.lifecycleMu.Unlock()
	return e.state == dispatcherManagerInitializing
}

// IsRunning reports whether the manager is in the Running state.
func (e *DispatcherManager) IsRunning() bool {
	e.lifecycleMu.Lock()
	defer e.lifecycleMu.Unlock()
	return e.state == dispatcherManagerRunning
}

func (e *DispatcherManager) isClosed() bool {
	e.lifecycleMu.Lock()
	defer e.lifecycleMu.Unlock()
	return e.state == dispatcherManagerClosed
}

func (e *DispatcherManager) isClosingOrClosed() bool {
	e.lifecycleMu.Lock()
	defer e.lifecycleMu.Unlock()
	return e.state >= dispatcherManagerClosing
}

// startClosing returns the previous state and serializes shutdown with resource
// publication. Initializing leaves cleanup with the constructor; Running
// transfers it to the caller. Later callers must not start another cleanup.
func (e *DispatcherManager) startClosing() dispatcherManagerState {
	e.lifecycleMu.Lock()
	defer e.lifecycleMu.Unlock()
	state := e.state
	if state < dispatcherManagerClosing {
		e.state = dispatcherManagerClosing
	}
	return state
}

// tryAddRunningTask adds the current handler or periodic task to wg while Running.
// The caller must defer wg.Done(). Shutdown waits for these calls to return
// before setting Closed.
func (e *DispatcherManager) tryAddRunningTask() bool {
	e.lifecycleMu.Lock()
	defer e.lifecycleMu.Unlock()
	if e.state != dispatcherManagerRunning {
		return false
	}
	e.wg.Add(1)
	return true
}
