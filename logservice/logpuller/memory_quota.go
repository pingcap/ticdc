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

package logpuller

import (
	"context"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/metrics"
	"go.uber.org/zap"
)

const (
	regionScanPauseRatio  = 0.5
	regionScanResumeRatio = 0.4
)

// pullerMemoryQuota applies hard backpressure at the configured quota and
// gates new region scans before the hard limit is reached. Dynamic stream owns
// the memory accounting and calls this controller whenever usage changes.
type pullerMemoryQuota struct {
	mu sync.Mutex

	regionScanPaused bool
	regionScanResume chan struct{}
}

func newPullerMemoryQuota() *pullerMemoryQuota {
	metrics.PullerRegionScanGate.Set(1)
	return &pullerMemoryQuota{
		regionScanResume: make(chan struct{}),
	}
}

// ShouldPausePath implements dynstream.MemoryControlAlgorithm. Puller flow
// control is global, so it does not pause individual subscription paths.
func (q *pullerMemoryQuota) ShouldPausePath(
	_ bool, pathPendingSize int64, _ int64, maxPendingSize uint64, _ int64,
) (bool, bool, float64) {
	return false, false, float64(pathPendingSize) / float64(maxPendingSize)
}

// ShouldPauseArea implements dynstream.MemoryControlAlgorithm.
func (q *pullerMemoryQuota) ShouldPauseArea(
	paused bool, pendingSize int64, maxPendingSize uint64,
) (bool, bool, float64) {
	usageRatio := float64(pendingSize) / float64(maxPendingSize)
	q.updateRegionScanState(usageRatio, pendingSize, maxPendingSize)

	if paused {
		return false, usageRatio < 1, usageRatio
	}
	return usageRatio >= 1, false, usageRatio
}

func (q *pullerMemoryQuota) updateRegionScanState(
	usageRatio float64, pendingSize int64, maxPendingSize uint64,
) {
	q.mu.Lock()
	defer q.mu.Unlock()

	switch {
	case !q.regionScanPaused && usageRatio >= regionScanPauseRatio:
		q.regionScanPaused = true
		q.regionScanResume = make(chan struct{})
		metrics.PullerRegionScanGate.Set(0)
		metrics.PullerRegionScanGateTransition.WithLabelValues("pause").Inc()
		log.Info("puller pauses region scans",
			zap.Int64("memoryUsage", pendingSize),
			zap.Uint64("memoryQuota", maxPendingSize),
			zap.Float64("memoryUsageRatio", usageRatio))
	case q.regionScanPaused && usageRatio < regionScanResumeRatio:
		q.regionScanPaused = false
		close(q.regionScanResume)
		metrics.PullerRegionScanGate.Set(1)
		metrics.PullerRegionScanGateTransition.WithLabelValues("resume").Inc()
		log.Info("puller resumes region scans",
			zap.Int64("memoryUsage", pendingSize),
			zap.Uint64("memoryQuota", maxPendingSize),
			zap.Float64("memoryUsageRatio", usageRatio))
	}
}

func (q *pullerMemoryQuota) waitRegionScanAllowed(ctx context.Context) error {
	for {
		resume, paused := q.regionScanResumeNotify()
		if !paused {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-resume:
		}
	}
}

func (q *pullerMemoryQuota) regionScanResumeNotify() (<-chan struct{}, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.regionScanResume, q.regionScanPaused
}
