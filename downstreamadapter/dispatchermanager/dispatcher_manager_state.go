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

type dispatcherManagerState uint32

const (
	// The zero value covers construction before the manager is published.
	dispatcherManagerInitializing dispatcherManagerState = iota
	// All local components and initial background work have been registered.
	dispatcherManagerRunning
	// New work and resource publication are fenced while cleanup completes.
	dispatcherManagerClosing
	// Base cleanup has returned. Remove-only cleanup may still be running.
	dispatcherManagerClosed
)

func (e *DispatcherManager) getState() dispatcherManagerState {
	return dispatcherManagerState(e.state.Load())
}

func (e *DispatcherManager) isWritePathClosed() bool {
	return e.getState() >= dispatcherManagerClosing
}

// startClosing returns the previous state and serializes shutdown with resource
// publication. Initializing leaves cleanup with the constructor; Running
// transfers it to the caller. Later callers must not start another cleanup.
func (e *DispatcherManager) startClosing() dispatcherManagerState {
	e.writePathMu.Lock()
	defer e.writePathMu.Unlock()
	state := e.getState()
	if state < dispatcherManagerClosing {
		e.state.Store(uint32(dispatcherManagerClosing))
	}
	return state
}

// beginWork admits runtime work before shutdown can start waiting for it.
// Every successful call must be paired with wg.Done after the work returns.
func (e *DispatcherManager) beginWork() bool {
	e.writePathMu.Lock()
	defer e.writePathMu.Unlock()
	if e.getState() != dispatcherManagerRunning {
		return false
	}
	e.wg.Add(1)
	return true
}
