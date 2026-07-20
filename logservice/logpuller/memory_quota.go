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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logpuller

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/ticdc/pkg/metrics"
)

const (
	// defaultLogPullerMemoryQuota is the soft memory capacity shared by event
	// accounting and initial-scan admission.
	defaultLogPullerMemoryQuota uint64 = 1024 * 1024 * 1024

	// Admission ratios compare max(accounted event memory, estimated scan
	// memory) with the soft capacity.

	// defaultPauseWarmingRatio pauses new high-lag scans when memory pressure
	// reaches 15% of the soft capacity.
	defaultPauseWarmingRatio = 0.15

	// defaultResumeWarmingRatio resumes high-lag scans after memory pressure
	// falls to 5% of the soft capacity.
	defaultResumeWarmingRatio = 0.05

	// defaultFreezeAllRatio pauses every new scan when memory pressure reaches
	// 80% of the soft capacity.
	defaultFreezeAllRatio = 0.8

	// defaultResumeAllRatio allows new scans again after memory pressure falls
	// to 60% of the soft capacity.
	defaultResumeAllRatio = 0.6

	// defaultHardLimitRatio blocks receiving more events when accounted event
	// memory reaches twice the soft capacity.
	defaultHardLimitRatio = 2.0

	// defaultScanBaseSize is the minimum memory estimate for one admitted scan.
	defaultScanBaseSize uint64 = 8 * 1024 * 1024

	// defaultScanLagUnit is the lag unit used by the logarithmic scan estimate.
	defaultScanLagUnit = 10 * time.Minute

	// defaultScanLagWeight controls how quickly the scan estimate grows with lag.
	defaultScanLagWeight = 0.22

	// defaultMaxScanLagFactor caps one scan estimate at this multiple of the base.
	defaultMaxScanLagFactor = 16
)

type admissionLevel uint8

const (
	admissionNormal admissionLevel = iota
	admissionPauseWarming
	admissionFreezeAllNewScans
)

// eventMemoryNotifier wakes event receivers that are waiting for memory. Each
// notification closes the current ready channel to wake all current waiters,
// then creates a new channel for future waiters.
//
// To avoid missing a notification, a receiver waits in this order:
//
//  1. Register the waiter.
//  2. Read the current ready channel under mu.
//  3. Recheck memory and the span state before blocking on that channel.
//
// If a notification happens just before registration, the final recheck sees
// the released memory or stopped span, so the receiver does not block.
type eventMemoryNotifier struct {
	mu      sync.Mutex
	ready   chan struct{}
	waiters atomic.Int64
}

func newEventMemoryNotifier() *eventMemoryNotifier {
	return &eventMemoryNotifier{ready: make(chan struct{})}
}

func (n *eventMemoryNotifier) wait(
	ctx context.Context,
	span *subscribedSpan,
	tryAcquire func() bool,
) bool {
	n.waiters.Add(1)
	defer n.waiters.Add(-1)
	for {
		n.mu.Lock()
		ready := n.ready
		n.mu.Unlock()

		// This check must stay after waiter registration and loading ready. It
		// closes both windows in which notify could otherwise be lost.
		if tryAcquire() {
			return true
		}
		if span.stopped.Load() {
			return false
		}
		select {
		case <-ready:
		case <-ctx.Done():
			return false
		}
	}
}

func (n *eventMemoryNotifier) notify() {
	// waiters is only a fast-path hint. A waiter that registers after this load
	// rechecks memory and the span state before blocking, so observing a stale
	// zero cannot lose a wakeup. Observing a stale nonzero only causes a harmless
	// extra broadcast.
	if n.waiters.Load() == 0 {
		return
	}
	n.mu.Lock()
	close(n.ready)
	n.ready = make(chan struct{})
	n.mu.Unlock()
}

// memoryQuotaController tracks event memory retained by downstream callbacks
// and estimated memory for admitted initial scans. Event memory is allowed to
// exceed the soft capacity, but the receive path waits at the hard limit. Scan
// admission first pauses uninitialized high-lag spans and freezes all new scans
// only under heavier pressure; both transitions use hysteresis when resuming.
type memoryQuotaController struct {
	capacity uint64
	// used tracks event bytes retained until downstream finishes consuming them.
	// Event accounting is on the receive hot path, so acquiring and releasing
	// memory only use atomic operations while usage is below the hard limit.
	used atomic.Uint64

	// eventNotifier owns the wait protocol used after the hard limit is reached.
	eventNotifier *eventMemoryNotifier

	// scanMu guards scan admission state and scanReady. Scan admission happens
	// once per region rather than once per event batch, so it is intentionally
	// kept simple instead of adding atomics to every field.
	scanMu sync.Mutex
	// scanUsed tracks the estimated memory of all admitted initial scans.
	scanUsed uint64
	level    admissionLevel
	// scanReady is replaced and closed when a memory transition can make a
	// rejected scan eligible. Workers wait on this channel directly, avoiding a
	// synchronous broadcast to every store and request worker.
	scanReady chan struct{}

	pauseWarmingLimit  uint64
	resumeWarmingLimit uint64
	freezeAllLimit     uint64
	resumeAllLimit     uint64
	hardLimit          uint64

	scanEstimate uint64
}

func newMemoryQuotaController(capacity, scanBaseSize uint64) *memoryQuotaController {
	if capacity == 0 {
		capacity = defaultLogPullerMemoryQuota
	}
	if scanBaseSize == 0 {
		scanBaseSize = defaultScanBaseSize
	}
	c := &memoryQuotaController{
		capacity:           capacity,
		level:              admissionNormal,
		pauseWarmingLimit:  uint64(math.Ceil(float64(capacity) * defaultPauseWarmingRatio)),
		resumeWarmingLimit: uint64(float64(capacity) * defaultResumeWarmingRatio),
		freezeAllLimit:     uint64(math.Ceil(float64(capacity) * defaultFreezeAllRatio)),
		resumeAllLimit:     uint64(float64(capacity) * defaultResumeAllRatio),
		hardLimit:          uint64(float64(capacity) * defaultHardLimitRatio),
		scanEstimate:       scanBaseSize,
		eventNotifier:      newEventMemoryNotifier(),
		scanReady:          make(chan struct{}),
	}
	return c
}

// WakeAll wakes quota waiters so they can observe cancellation or a stopped span.
func (c *memoryQuotaController) WakeAll() {
	c.eventNotifier.notify()
	c.NotifyScanAdmission()
}

// AcquireScan admits one region scan and returns its memory estimate.
func (c *memoryQuotaController) AcquireScan(
	region regionInfo,
	currentTs uint64,
) (bytes uint64, retry <-chan struct{}, admitted bool) {
	span := region.subscribedSpan
	if span.stopped.Load() {
		// Let stale tasks reach the worker's stopped-subscription cleanup path
		// without consuming scan quota.
		return 0, nil, true
	}

	c.scanMu.Lock()
	defer c.scanMu.Unlock()
	c.refreshLevelLocked()
	if c.level == admissionFreezeAllNewScans {
		return 0, c.scanReady, false
	}

	warming := isWarmingScan(region, currentTs)
	// Admission is based on the pressure before accounting this scan. This lets
	// one scan make progress even when its estimate alone exceeds the threshold.
	if warming && c.level == admissionPauseWarming {
		return 0, c.scanReady, false
	}
	bytes = c.estimateScanSizeLocked(region, currentTs)
	c.scanUsed += bytes
	c.refreshLevelLocked()
	return bytes, nil, true
}

// ReleaseScan releases the estimate owned by an admitted region scan.
func (c *memoryQuotaController) ReleaseScan(bytes uint64) {
	if bytes == 0 {
		return
	}
	c.scanMu.Lock()
	previousLevel := c.level
	c.scanUsed = subtractFloor(c.scanUsed, bytes)
	c.refreshLevelLocked()
	if c.level < previousLevel {
		c.notifyScanAdmissionLocked()
	}
	c.scanMu.Unlock()
}

// AcquireEvent accounts one event batch. Below the hard limit its hot path is
// a context check and an atomic compare-and-swap; it does not allocate or take
// a mutex.
func (c *memoryQuotaController) AcquireEvent(
	ctx context.Context,
	span *subscribedSpan,
	bytes uint64,
) bool {
	if ctx.Err() != nil {
		return false
	}
	if c.tryAcquireEvent(bytes) {
		return true
	}

	start := time.Now()
	acquired := c.eventNotifier.wait(ctx, span, func() bool {
		return c.tryAcquireEvent(bytes)
	})
	metrics.LogPullerMemoryQuotaEventWaitDuration.Observe(time.Since(start).Seconds())
	return acquired
}

func (c *memoryQuotaController) tryAcquireEvent(bytes uint64) bool {
	for {
		used := c.used.Load()
		if used > 0 && wouldExceed(used, bytes, c.hardLimit) {
			return false
		}
		if bytes > math.MaxUint64-used {
			return false
		}
		if c.used.CompareAndSwap(used, used+bytes) {
			return true
		}
	}
}

// ReleaseEvent releases event memory after downstream has consumed the event.
func (c *memoryQuotaController) ReleaseEvent(bytes uint64) {
	if bytes == 0 {
		return
	}
	used := c.used.Add(^(bytes - 1))
	previousUsed := used + bytes
	if crossesDown(previousUsed, used, c.resumeWarmingLimit) ||
		crossesDown(previousUsed, used, c.resumeAllLimit) {
		c.refreshAdmissionAndNotify()
	}
	c.eventNotifier.notify()
}

// NotifyScanAdmission wakes workers so they can recheck span state and admission.
func (c *memoryQuotaController) NotifyScanAdmission() {
	c.scanMu.Lock()
	c.notifyScanAdmissionLocked()
	c.scanMu.Unlock()
}

// UpdateMetrics reports the current event-memory and scan-admission state.
func (c *memoryQuotaController) UpdateMetrics() {
	c.scanMu.Lock()
	used := c.used.Load()
	scanUsed := c.scanUsed
	level := c.level
	scanEstimate := c.scanEstimate
	c.scanMu.Unlock()

	metrics.LogPullerMemoryQuota.WithLabelValues("max").Set(float64(c.capacity))
	metrics.LogPullerMemoryQuota.WithLabelValues("used").Set(float64(used))
	metrics.LogPullerMemoryQuota.WithLabelValues("scan_used").Set(float64(scanUsed))
	metrics.LogPullerMemoryQuota.WithLabelValues("scan_estimate").Set(float64(scanEstimate))
	metrics.LogPullerMemoryQuota.WithLabelValues("hard_limit").Set(float64(c.hardLimit))
	metrics.LogPullerMemoryQuotaAdmissionLevel.Set(float64(level))
	metrics.LogPullerMemoryQuotaEventWaiterCount.Set(
		float64(c.eventNotifier.waiters.Load()))
}

func (c *memoryQuotaController) notifyScanAdmissionLocked() {
	close(c.scanReady)
	c.scanReady = make(chan struct{})
}

func (c *memoryQuotaController) refreshAdmissionAndNotify() {
	c.scanMu.Lock()
	previousLevel := c.level
	c.refreshLevelLocked()
	if c.level < previousLevel {
		c.notifyScanAdmissionLocked()
	}
	c.scanMu.Unlock()
}

func (c *memoryQuotaController) estimateScanSizeLocked(region regionInfo, currentTs uint64) uint64 {
	raw := float64(c.scanEstimate) * scanLagFactor(region.resolvedTs(), currentTs)
	estimate := uint64(raw)
	if estimate < c.scanEstimate {
		estimate = c.scanEstimate
	}
	maxEstimate := uint64(math.MaxUint64)
	if c.scanEstimate <= math.MaxUint64/defaultMaxScanLagFactor {
		maxEstimate = c.scanEstimate * defaultMaxScanLagFactor
	}
	if estimate > maxEstimate {
		estimate = maxEstimate
	}
	if estimate == 0 {
		estimate = c.scanEstimate
	}
	return estimate
}

func scanLagFactor(startTs, currentTs uint64) float64 {
	lag := regionScanLag(currentTs, startTs)
	if lag <= 0 {
		return 1
	}
	return min(defaultMaxScanLagFactor,
		1+defaultScanLagWeight*math.Log2(1+float64(lag)/float64(defaultScanLagUnit)))
}

func isWarmingScan(region regionInfo, currentTs uint64) bool {
	span := region.subscribedSpan
	if span.initialized.Load() {
		return false
	}
	return regionScanLag(currentTs, region.resolvedTs()) >= lowLagRegionThreshold
}

func (c *memoryQuotaController) refreshLevelLocked() {
	// scanUsed predicts the event memory an initial scan may produce, so adding
	// it to actual event bytes would count the same pressure twice.
	pressure := max(c.used.Load(), c.scanUsed)
	switch c.level {
	case admissionFreezeAllNewScans:
		if pressure <= c.resumeAllLimit {
			if pressure >= c.pauseWarmingLimit {
				c.level = admissionPauseWarming
			} else {
				c.level = admissionNormal
			}
		}
	case admissionPauseWarming:
		switch {
		case pressure >= c.freezeAllLimit:
			c.level = admissionFreezeAllNewScans
		case pressure <= c.resumeWarmingLimit:
			c.level = admissionNormal
		}
	default:
		switch {
		case pressure >= c.freezeAllLimit:
			c.level = admissionFreezeAllNewScans
		case pressure >= c.pauseWarmingLimit:
			c.level = admissionPauseWarming
		}
	}
}

func wouldExceed(used, bytes, limit uint64) bool {
	return bytes > limit || used > limit-bytes
}

func crossesDown(previous, current, threshold uint64) bool {
	return previous > threshold && current <= threshold
}

func subtractFloor(value, delta uint64) uint64 {
	if value < delta {
		return 0
	}
	return value - delta
}
