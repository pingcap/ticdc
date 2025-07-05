// Copyright 2024 PingCAP, Inc.
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

package dynstream

// MemoryControlAlgorithm defines the interface for memory control algorithms.
// It provides methods to determine when to pause/resume paths and areas.
type MemoryControlAlgorithm interface {
	// ShouldPausePath determines if a path should be paused based on its memory usage.
	// Returns pause, resume flags and the memory usage ratio.
	ShouldPausePath(paused bool, pathPendingSize, areaPendingSize, maxPendingSize uint64, pathCount int64) (pause bool, resume bool, memoryUsageRatio float64)

	// ShouldPauseArea determines if an area should be paused based on its memory usage.
	// Returns pause, resume flags and the memory usage ratio.
	ShouldPauseArea(paused bool, pendingSize uint64, maxPendingSize uint64) (pause bool, resume bool, memoryUsageRatio float64)
}

// NewMemoryControlAlgorithm creates a new MemoryControlAlgorithm based on the algorithm type.
func NewMemoryControlAlgorithm(algorithm int) MemoryControlAlgorithm {
	switch algorithm {
	case MemoryControlForEventCollector:
		return &EventCollectorMemoryControl{}
	default:
		return &PullerMemoryControl{}
	}
}

// PullerMemoryControl implements MemoryControlAlgorithm for puller components.
// It controls both area and path level memory usage.
type PullerMemoryControl struct{}

// ShouldPausePath implements MemoryControlAlgorithm.ShouldPausePath for puller components.
func (p *PullerMemoryControl) ShouldPausePath(paused bool, pathPendingSize, _, maxPendingSize uint64, _ int64) (bool, bool, float64) {
	memoryUsageRatio := float64(pathPendingSize) / float64(maxPendingSize)

	switch {
	case paused:
		if memoryUsageRatio < 0.1 {
			return false, true, memoryUsageRatio
		}
	default:
		if memoryUsageRatio >= 0.2 {
			return true, false, memoryUsageRatio
		}
	}

	return false, false, memoryUsageRatio
}

// ShouldPauseArea implements MemoryControlAlgorithm.ShouldPauseArea for puller components.
func (p *PullerMemoryControl) ShouldPauseArea(paused bool, pendingSize uint64, maxPendingSize uint64) (bool, bool, float64) {
	memoryUsageRatio := float64(pendingSize) / float64(maxPendingSize)

	switch {
	case paused:
		if memoryUsageRatio < 0.5 {
			return false, true, memoryUsageRatio
		}
	default:
		if memoryUsageRatio >= 0.8 {
			return true, false, memoryUsageRatio
		}
	}

	return false, false, memoryUsageRatio
}

// EventCollectorMemoryControl implements MemoryControlAlgorithm for event collector components.
// It only controls path level memory usage.
type EventCollectorMemoryControl struct{}

// ShouldPausePath implements MemoryControlAlgorithm.ShouldPausePath for event collector components.
func (e *EventCollectorMemoryControl) ShouldPausePath(paused bool, pathPendingSize, areaPendingSize, maxPendingSize uint64, pathCount int64) (bool, bool, float64) {
	if pathCount == 0 {
		pathCount = 1
	}

	pathMemoryUsageRatio := float64(pathPendingSize) / float64(maxPendingSize)
	areaMemoryUsageRatio := float64(areaPendingSize) / float64(maxPendingSize)

	pauseLimit, resumeLimit := e.calculateThresholds(pathCount, areaMemoryUsageRatio)

	if paused {
		return false, pathMemoryUsageRatio < resumeLimit, pathMemoryUsageRatio
	}
	return pathMemoryUsageRatio > pauseLimit, false, pathMemoryUsageRatio
}

// calculateThresholds calculates dynamic pause and resume memory thresholds for path-level flow control.
// It takes into account both the number of paths and current area memory usage to determine appropriate limits.
// The function implements an adaptive threshold strategy:
// - For 1-2 paths, it uses fixed thresholds
// - For >2 paths, it:
//  1. Applies a penalty factor that increases with path count
//  2. Uses a tiered threshold system based on area memory usage
//  3. Ensures minimum thresholds based on path count
//
// Parameters:
//   - pathCount: number of active paths in the area
//   - areaMemoryUsageRatio: current memory usage ratio of the area (used/max)
//
// Returns:
//   - pauseLimit: memory ratio threshold to trigger path pausing
//   - resumeLimit: memory ratio threshold to trigger path resuming
func (e *EventCollectorMemoryControl) calculateThresholds(pathCount int64, areaMemoryUsageRatio float64) (pauseLimit, resumeLimit float64) {
	if pathCount == 0 {
		pathCount = 1
	}

	// Special case for path count 1 and 2
	switch pathCount {
	case 1:
		return 0.8, 0.4
	case 2:
		return 0.5, 0.25
	}

	// calculate penalty factor based on path count
	penaltyFactor := 1.0 - 0.5/float64(pathCount)

	type threshold struct {
		areaUsage   float64
		pauseLimit  float64
		resumeLimit float64
	}

	baseThresholds := []threshold{
		//
		{0.5, 0.2, 0.1},   // area usage <= 50%
		{0.8, 0.1, 0.05},  // area usage <= 80%
		{1.0, 0.05, 0.01}, // area usage <= 100%
		// Default maxPendingSize is 1024MB, so the default lowest pause limit is 10 MB.
		{1.2, 0.01, 0.005},  // area usage > 100%
		{1.5, 0.005, 0.001}, // area usage > 150%, set the lowest pause limit to 1 MB.
	}

	// find applicable threshold
	var t threshold
	for _, th := range baseThresholds {
		if areaMemoryUsageRatio <= th.areaUsage {
			t = th
			break
		}
	}

	return t.pauseLimit / penaltyFactor, t.resumeLimit / penaltyFactor
}

// ShouldPauseArea implements MemoryControlAlgorithm.ShouldPauseArea for event collector components.
func (e *EventCollectorMemoryControl) ShouldPauseArea(_ bool, pendingSize uint64, maxPendingSize uint64) (bool, bool, float64) {
	memoryUsageRatio := float64(pendingSize) / float64(maxPendingSize)
	return false, false, memoryUsageRatio
}
