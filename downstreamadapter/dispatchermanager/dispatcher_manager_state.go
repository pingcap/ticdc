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
	// New resources are fenced while existing work and resources are stopped.
	dispatcherManagerClosing
	// Scheduled tasks skip subsequent executions during final metrics cleanup.
	dispatcherManagerFinalizing
	// Base cleanup has returned. Remove-only cleanup may still be running.
	dispatcherManagerClosed
)

func (e *DispatcherManager) getState() dispatcherManagerState {
	return dispatcherManagerState(e.state.Load())
}

func (e *DispatcherManager) isWritePathClosed() bool {
	return e.getState() >= dispatcherManagerClosing
}

// startClosing serializes the shutdown decision with resource publication and
// the Initializing -> Running transition. Only its successful caller starts cleanup.
func (e *DispatcherManager) startClosing() bool {
	e.writePathMu.Lock()
	defer e.writePathMu.Unlock()
	if e.isWritePathClosed() {
		return false
	}
	e.state.Store(uint32(dispatcherManagerClosing))
	return true
}
