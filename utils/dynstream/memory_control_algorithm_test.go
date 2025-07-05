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

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPullerMemoryControl_ShouldPausePath(t *testing.T) {
	tests := []struct {
		name            string
		paused          bool
		pathPendingSize uint64
		areaPendingSize uint64
		maxPendingSize  uint64
		pathCount       int64
		wantPause       bool
		wantResume      bool
		wantRatio       float64
	}{
		{
			name:            "not paused, low memory usage",
			paused:          false,
			pathPendingSize: 10,
			areaPendingSize: 40,
			maxPendingSize:  100,
			pathCount:       1,
			wantPause:       false,
			wantResume:      false,
			wantRatio:       0.1,
		},
		{
			name:            "not paused, high memory usage",
			paused:          false,
			pathPendingSize: 20,
			areaPendingSize: 40,
			maxPendingSize:  100,
			pathCount:       1,
			wantPause:       true,
			wantResume:      false,
			wantRatio:       0.2,
		},
		{
			name:            "paused, low memory usage",
			paused:          true,
			pathPendingSize: 9,
			areaPendingSize: 40,
			maxPendingSize:  100,
			pathCount:       1,
			wantPause:       false,
			wantResume:      true,
			wantRatio:       0.09,
		},
		{
			name:            "paused, high memory usage",
			paused:          true,
			pathPendingSize: 15,
			areaPendingSize: 40,
			maxPendingSize:  100,
			pathCount:       1,
			wantPause:       false,
			wantResume:      false,
			wantRatio:       0.15,
		},
	}

	algorithm := &PullerMemoryControl{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPause, gotResume, gotRatio := algorithm.ShouldPausePath(
				tt.paused,
				tt.pathPendingSize,
				tt.areaPendingSize,
				tt.maxPendingSize,
				tt.pathCount,
			)

			require.Equal(t, tt.wantPause, gotPause, tt.name)
			require.Equal(t, tt.wantResume, gotResume, tt.name)
			require.Equal(t, tt.wantRatio, gotRatio, tt.name)
		})
	}
}

func TestPullerMemoryControl_ShouldPauseArea(t *testing.T) {
	tests := []struct {
		name           string
		paused         bool
		pendingSize    uint64
		maxPendingSize uint64
		wantPause      bool
		wantResume     bool
		wantRatio      float64
	}{
		{
			name:           "not paused, low memory usage",
			paused:         false,
			pendingSize:    10,
			maxPendingSize: 100,
			wantPause:      false,
			wantResume:     false,
			wantRatio:      0.1,
		},
		{
			name:           "not paused, high memory usage",
			paused:         false,
			pendingSize:    80,
			maxPendingSize: 100,
			wantPause:      true,
			wantResume:     false,
			wantRatio:      0.8,
		},
		{
			name:           "paused, low memory usage",
			paused:         true,
			pendingSize:    49,
			maxPendingSize: 100,
			wantPause:      false,
			wantResume:     true,
			wantRatio:      0.49,
		},
		{
			name:           "paused, high memory usage",
			paused:         true,
			pendingSize:    60,
			maxPendingSize: 100,
			wantPause:      false,
			wantResume:     false,
			wantRatio:      0.6,
		},
	}

	algorithm := &PullerMemoryControl{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPause, gotResume, gotRatio := algorithm.ShouldPauseArea(
				tt.paused,
				tt.pendingSize,
				tt.maxPendingSize,
			)

			require.Equal(t, tt.wantPause, gotPause, tt.name)
			require.Equal(t, tt.wantResume, gotResume, tt.name)
			require.Equal(t, tt.wantRatio, gotRatio, tt.name)
		})
	}
}

func TestEventCollectorMemoryControl_ShouldPausePath(t *testing.T) {
	tests := []struct {
		name            string
		paused          bool
		pathPendingSize uint64
		areaPendingSize uint64
		maxPendingSize  uint64
		pathCount       int64
		wantPause       bool
		wantResume      bool
		wantRatio       float64
	}{
		{
			name:            "area usage <= 50%, not paused, should pause",
			paused:          false,
			pathPendingSize: 25, // 25% usage
			areaPendingSize: 40, // 40% usage
			maxPendingSize:  100,
			wantPause:       true,
			wantResume:      false,
			wantRatio:       0.25,
			pathCount:       10,
		},
		{
			name:            "area usage <= 50%, not paused, should not pause",
			paused:          false,
			pathPendingSize: 15, // 15% usage
			areaPendingSize: 40, // 40% usage
			maxPendingSize:  100,
			wantPause:       false,
			wantResume:      false,
			wantRatio:       0.15,
			pathCount:       10,
		},
		{
			name:            "area usage <= 50%, paused, should resume",
			paused:          true,
			pathPendingSize: 5,  // 5% usage
			areaPendingSize: 40, // 40% usage
			maxPendingSize:  100,
			wantPause:       false,
			wantResume:      true,
			wantRatio:       0.05,
			pathCount:       10,
		},
		{
			name:            "area usage > 80%, not paused, should pause",
			paused:          false,
			pathPendingSize: 6,  // 6% usage
			areaPendingSize: 85, // 85% usage
			maxPendingSize:  100,
			wantPause:       true,
			wantResume:      false,
			wantRatio:       0.06,
			pathCount:       10,
		},
		{
			name:            "area usage > 120%, not paused, should pause",
			paused:          false,
			pathPendingSize: 2,   // 2% usage
			areaPendingSize: 125, // 125% usage
			maxPendingSize:  100,
			wantPause:       true,
			wantResume:      false,
			wantRatio:       0.02,
			pathCount:       10,
		},
		{
			name:            "area usage > 120%, paused, should not resume",
			paused:          true,
			pathPendingSize: 2,   // 2% usage
			areaPendingSize: 125, // 125% usage
			maxPendingSize:  100,
			wantPause:       false,
			wantResume:      false,
			wantRatio:       0.02,
			pathCount:       10,
		},
	}

	algorithm := &EventCollectorMemoryControl{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPause, gotResume, gotRatio := algorithm.ShouldPausePath(
				tt.paused,
				tt.pathPendingSize,
				tt.areaPendingSize,
				tt.maxPendingSize,
				tt.pathCount,
			)

			require.Equal(t, tt.wantPause, gotPause, tt.name)
			require.Equal(t, tt.wantResume, gotResume, tt.name)
			require.Equal(t, tt.wantRatio, gotRatio, tt.name)
		})
	}
}

func TestEventCollectorMemoryControl_ShouldPauseArea(t *testing.T) {
	tests := []struct {
		name           string
		paused         bool
		pendingSize    uint64
		maxPendingSize uint64
		wantPause      bool
		wantResume     bool
		wantRatio      float64
	}{
		{
			name:           "low memory usage",
			paused:         false,
			pendingSize:    30,
			maxPendingSize: 100,
			wantPause:      false,
			wantResume:     false,
			wantRatio:      0.3,
		},
		{
			name:           "high memory usage",
			paused:         false,
			pendingSize:    90,
			maxPendingSize: 100,
			wantPause:      false,
			wantResume:     false,
			wantRatio:      0.9,
		},
		{
			name:           "already paused",
			paused:         true,
			pendingSize:    50,
			maxPendingSize: 100,
			wantPause:      false,
			wantResume:     false,
			wantRatio:      0.5,
		},
	}

	algorithm := &EventCollectorMemoryControl{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPause, gotResume, gotRatio := algorithm.ShouldPauseArea(
				tt.paused,
				tt.pendingSize,
				tt.maxPendingSize,
			)

			require.Equal(t, tt.wantPause, gotPause, tt.name)
			require.Equal(t, tt.wantResume, gotResume, tt.name)
			require.Equal(t, tt.wantRatio, gotRatio, tt.name)
		})
	}
}

func TestEventCollectorMemoryControl_CalculateThresholds(t *testing.T) {
	tests := []struct {
		name               string
		pathCount          int64
		areaMemoryRatio    float64
		expectedPauseLimit float64
		expectedResume     float64
		delta              float64
	}{
		// Special path count cases
		{
			name:               "path count 0 should be treated as 1",
			pathCount:          0,
			areaMemoryRatio:    0.3,
			expectedPauseLimit: 0.8,
			expectedResume:     0.4,
			delta:              0,
		},
		{
			name:               "single path",
			pathCount:          1,
			areaMemoryRatio:    0.3,
			expectedPauseLimit: 0.8,
			expectedResume:     0.4,
			delta:              0,
		},
		{
			name:               "two paths",
			pathCount:          2,
			areaMemoryRatio:    0.3,
			expectedPauseLimit: 0.5,
			expectedResume:     0.25,
			delta:              0,
		},
		{
			name:               "four paths",
			pathCount:          4,
			areaMemoryRatio:    0.3,
			expectedPauseLimit: 0.22,
			expectedResume:     0.11,
			delta:              0.01,
		},
		// Area memory usage threshold boundaries
		{
			name:               "area usage exactly 0.5",
			pathCount:          10,
			areaMemoryRatio:    0.5,
			expectedPauseLimit: 0.2,
			expectedResume:     0.1,
			delta:              0.05,
		},
		{
			name:               "area usage exactly 0.8",
			pathCount:          10,
			areaMemoryRatio:    0.8,
			expectedPauseLimit: 0.1,
			expectedResume:     0.05,
			delta:              0.05,
		},
		{
			name:               "area usage exactly 1.0",
			pathCount:          10,
			areaMemoryRatio:    1.0,
			expectedPauseLimit: 0.05,
			expectedResume:     0.01,
			delta:              0.05,
		},
		{
			name:               "area usage > 1.0",
			pathCount:          10,
			areaMemoryRatio:    1.1,
			expectedPauseLimit: 0.01,
			expectedResume:     0.005,
			delta:              0.05,
		},

		// Large number of paths
		{
			name:               "100 paths with low area usage",
			pathCount:          100,
			areaMemoryRatio:    0.3,
			expectedPauseLimit: 0.2,
			expectedResume:     0.1,
			delta:              0.05,
		},
		{
			name:               "1000 paths with high area usage",
			pathCount:          1000,
			areaMemoryRatio:    0.9,
			expectedPauseLimit: 0.05,
			expectedResume:     0.01,
			delta:              0.05,
		},

		// Minimum threshold tests
		{
			name:               "very large path count should respect minimum threshold",
			pathCount:          10000,
			areaMemoryRatio:    1.2,
			expectedPauseLimit: 0.01, // minimum threshold
			expectedResume:     0.005,
			delta:              0.05,
		},
	}

	algorithm := &EventCollectorMemoryControl{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pauseLimit, resumeLimit := algorithm.calculateThresholds(tt.pathCount, tt.areaMemoryRatio)

			require.InDelta(t, tt.expectedPauseLimit, pauseLimit, tt.delta, tt.name)
			require.InDelta(t, tt.expectedResume, resumeLimit, tt.delta, tt.name)

			// Invariant checks
			require.Less(t, resumeLimit, pauseLimit, tt.name)
			require.Greater(t, resumeLimit, 0.0, tt.name)
			require.Greater(t, pauseLimit, 0.0, tt.name)
			require.Less(t, pauseLimit, 1.0, tt.name)
			require.Less(t, resumeLimit, 1.0, tt.name)
		})
	}
}
