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

type memoryQuotaLease struct {
	once    sync.Once
	release func()
}

func newMemoryQuotaLease(release func()) *memoryQuotaLease {
	return &memoryQuotaLease{release: release}
}

func (l *memoryQuotaLease) Release() {
	l.once.Do(l.release)
}

type subscriptionQuotaState struct {
	eventLeases map[*memoryQuotaLease]struct{}
	scanLeases  map[*memoryQuotaLease]struct{}
}

func newSubscriptionQuotaState() *subscriptionQuotaState {
	return &subscriptionQuotaState{
		eventLeases: make(map[*memoryQuotaLease]struct{}),
		scanLeases:  make(map[*memoryQuotaLease]struct{}),
	}
}

// memoryQuotaController tracks event memory retained by downstream callbacks
// and estimated memory for admitted initial scans. Event memory is allowed to
// exceed the soft capacity, but the receive path waits at the hard limit. Scan
// admission first pauses uninitialized high-lag spans and freezes all new scans
// only under heavier pressure; both transitions use hysteresis when resuming.
type memoryQuotaController struct {
	mu   sync.Mutex
	cond *sync.Cond

	capacity uint64
	// used tracks event bytes retained until downstream finishes consuming them.
	used uint64

	// scanUsed tracks the estimated memory of all admitted initial scans.
	// warmingScanUsed is the subset used by uninitialized, high-lag spans.
	scanUsed        uint64
	warmingScanUsed uint64
	level           admissionLevel

	pauseWarmingRatio  float64
	resumeWarmingRatio float64
	freezeAllRatio     float64
	resumeAllRatio     float64
	hardLimitRatio     float64

	scanEstimate uint64

	subscriptions map[SubscriptionID]*subscriptionQuotaState
	onAvailable   atomic.Value // func()
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
		pauseWarmingRatio:  defaultPauseWarmingRatio,
		resumeWarmingRatio: defaultResumeWarmingRatio,
		freezeAllRatio:     defaultFreezeAllRatio,
		resumeAllRatio:     defaultResumeAllRatio,
		hardLimitRatio:     defaultHardLimitRatio,
		scanEstimate:       scanBaseSize,
		subscriptions:      make(map[SubscriptionID]*subscriptionQuotaState),
	}
	c.cond = sync.NewCond(&c.mu)
	c.onAvailable.Store(func() {})
	return c
}

func (c *memoryQuotaController) setOnAvailable(fn func()) {
	c.onAvailable.Store(fn)
}

func (c *memoryQuotaController) notifyAvailable() {
	c.onAvailable.Load().(func())()
}

func (c *memoryQuotaController) wakeAll() {
	c.mu.Lock()
	c.cond.Broadcast()
	c.mu.Unlock()
}

func (c *memoryQuotaController) snapshot() (used, capacity uint64, level admissionLevel) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used, c.capacity, c.level
}

func (c *memoryQuotaController) scanSnapshot() (
	scanUsed uint64,
	warmingScanUsed uint64,
	warmingScanBudget uint64,
	scanEstimate uint64,
	hardLimit uint64,
) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.scanUsed, c.warmingScanUsed, c.warmingScanBudgetLocked(),
		c.scanEstimate, c.hardLimitLocked()
}

func (c *memoryQuotaController) addSubscription(span *subscribedSpan) {
	c.mu.Lock()
	c.subscriptions[span.subID] = newSubscriptionQuotaState()
	c.mu.Unlock()
}

func (c *memoryQuotaController) removeSubscription(span *subscribedSpan) {
	c.mu.Lock()
	state, ok := c.subscriptions[span.subID]
	if !ok {
		c.mu.Unlock()
		return
	}
	delete(c.subscriptions, span.subID)
	leases := make([]*memoryQuotaLease, 0, len(state.eventLeases)+len(state.scanLeases))
	for lease := range state.eventLeases {
		leases = append(leases, lease)
	}
	for lease := range state.scanLeases {
		leases = append(leases, lease)
	}
	// Wake event receivers so they can observe that the subscription was removed.
	c.cond.Broadcast()
	c.mu.Unlock()

	for _, lease := range leases {
		lease.Release()
	}
	// Removing a subscription can make a pending request eligible even when the
	// subscription itself did not own any lease.
	c.notifyAvailable()
}

func (c *memoryQuotaController) markSubscriptionInitialized() {
	c.notifyAvailable()
}

func (c *memoryQuotaController) acquireScan(
	region regionInfo,
	currentTs uint64,
) (*memoryQuotaLease, bool) {
	span := region.subscribedSpan
	c.mu.Lock()
	state, ok := c.subscriptions[span.subID]
	if !ok {
		// The subscription has already been removed. Let the request continue to
		// the worker, where the normal stopped-subscription path will discard it.
		c.mu.Unlock()
		return newMemoryQuotaLease(func() {}), true
	}
	c.refreshLevelLocked()
	if c.level == admissionFreezeAllNewScans {
		c.mu.Unlock()
		return nil, false
	}

	bytes := c.estimateScanSizeLocked(region, currentTs)
	warming := isWarmingScan(region, currentTs)
	if c.isWarmingScanBlockedLocked(warming, bytes) {
		c.mu.Unlock()
		return nil, false
	}

	lease := &memoryQuotaLease{}
	lease.release = func() {
		c.mu.Lock()
		previousLevel := c.level
		c.scanUsed = subtractFloor(c.scanUsed, bytes)
		if warming {
			c.warmingScanUsed = subtractFloor(c.warmingScanUsed, bytes)
		}
		delete(state.scanLeases, lease)
		c.refreshLevelLocked()
		shouldNotifyAdmission := c.level < previousLevel ||
			(warming && c.level == admissionNormal)
		c.mu.Unlock()

		if shouldNotifyAdmission {
			c.notifyAvailable()
		}
	}
	c.scanUsed += bytes
	if warming {
		c.warmingScanUsed += bytes
	}
	state.scanLeases[lease] = struct{}{}
	c.refreshLevelLocked()
	c.mu.Unlock()
	return lease, true
}

func (c *memoryQuotaController) trackEvent(
	ctx context.Context,
	span *subscribedSpan,
	bytes uint64,
) *memoryQuotaLease {
	c.mu.Lock()
	if ctx.Err() != nil {
		c.mu.Unlock()
		return nil
	}
	state := c.subscriptions[span.subID]
	if state == nil {
		c.mu.Unlock()
		return nil
	}

	for c.used > 0 && wouldExceed(c.used, bytes, c.hardLimitLocked()) {
		c.cond.Wait()
		if ctx.Err() != nil {
			c.mu.Unlock()
			return nil
		}
		if c.subscriptions[span.subID] != state {
			c.mu.Unlock()
			return nil
		}
	}

	c.used += bytes
	c.refreshLevelLocked()
	lease := &memoryQuotaLease{}
	lease.release = func() {
		c.mu.Lock()
		previousLevel := c.level
		c.used = subtractFloor(c.used, bytes)
		delete(state.eventLeases, lease)
		c.refreshLevelLocked()
		shouldNotifyAdmission := c.level < previousLevel
		c.cond.Broadcast()
		c.mu.Unlock()

		if shouldNotifyAdmission {
			c.notifyAvailable()
		}
	}
	state.eventLeases[lease] = struct{}{}
	c.mu.Unlock()
	return lease
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

func (c *memoryQuotaController) isWarmingScanBlockedLocked(warming bool, bytes uint64) bool {
	if !warming {
		return false
	}
	if c.level == admissionPauseWarming {
		return true
	}
	return wouldExceed(c.warmingScanUsed, bytes, c.warmingScanBudgetLocked())
}

func (c *memoryQuotaController) warmingScanBudgetLocked() uint64 {
	budget := uint64(float64(c.capacity) * c.pauseWarmingRatio)
	return max(budget, c.scanEstimate)
}

func (c *memoryQuotaController) hardLimitLocked() uint64 {
	return uint64(float64(c.capacity) * c.hardLimitRatio)
}

func (c *memoryQuotaController) refreshLevelLocked() {
	// scanUsed predicts the event memory an initial scan may produce, so adding
	// it to actual event bytes would count the same pressure twice.
	pressure := max(c.used, c.scanUsed)
	usage := float64(pressure) / float64(c.capacity)
	switch c.level {
	case admissionFreezeAllNewScans:
		if usage <= c.resumeAllRatio {
			if usage >= c.pauseWarmingRatio {
				c.level = admissionPauseWarming
			} else {
				c.level = admissionNormal
			}
		}
	case admissionPauseWarming:
		switch {
		case usage >= c.freezeAllRatio:
			c.level = admissionFreezeAllNewScans
		case usage <= c.resumeWarmingRatio:
			c.level = admissionNormal
		}
	default:
		switch {
		case usage >= c.freezeAllRatio:
			c.level = admissionFreezeAllNewScans
		case usage >= c.pauseWarmingRatio:
			c.level = admissionPauseWarming
		}
	}
}

func wouldExceed(used, bytes, limit uint64) bool {
	return bytes > limit || used > limit-bytes
}

func subtractFloor(value, delta uint64) uint64 {
	if value < delta {
		return 0
	}
	return value - delta
}
