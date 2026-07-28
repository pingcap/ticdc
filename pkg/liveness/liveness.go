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

package liveness

import "sync/atomic"

// Liveness represents the lifecycle state of a node in the cluster.
//
// It is designed to be monotonic and only allows the following transitions:
// Alive -> Draining -> Stopping.
type Liveness int32

const (
	// CaptureAlive means the capture is alive, and ready to serve.
	CaptureAlive Liveness = 0
	// CaptureDraining means the capture is in a pre-stop phase where it should
	// not accept new scheduling destinations, but may still finish in-flight work.
	CaptureDraining Liveness = 1
	// CaptureStopping means the capture is in the process of graceful shutdown.
	CaptureStopping Liveness = 2
)

// Store upgrades the liveness to the given state.
// It returns true only when the underlying state is changed.
//
// Only step-by-step upgrades are allowed:
// Alive -> Draining -> Stopping.
func (l *Liveness) Store(v Liveness) bool {
	if v < CaptureAlive || v > CaptureStopping {
		return false
	}
	old := l.Load()
	switch old {
	case CaptureAlive:
		if v != CaptureDraining {
			return false
		}
	case CaptureDraining:
		if v != CaptureStopping {
			return false
		}
	case CaptureStopping:
		return false
	default:
		// Defensive: if the stored value is out of the expected enum range,
		// reject transitions to avoid breaking the monotonic state machine.
		return false
	}

	// A single CAS is enough here. If another writer wins the race, the state can
	// only move forward, so retrying cannot make this transition valid again.
	return atomic.CompareAndSwapInt32((*int32)(l), int32(old), int32(v))
}

// Load returns the current liveness.
func (l *Liveness) Load() Liveness {
	return Liveness(atomic.LoadInt32((*int32)(l)))
}

// String returns a stable string representation using an atomic read.
func (l *Liveness) String() string {
	if l == nil {
		return "Unknown"
	}
	switch l.Load() {
	case CaptureAlive:
		return "Alive"
	case CaptureDraining:
		return "Draining"
	case CaptureStopping:
		return "Stopping"
	default:
		return "Unknown"
	}
}
