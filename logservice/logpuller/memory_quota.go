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
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/tikv/client-go/v2/oracle"
)

const (
	defaultLogPullerMemoryQuota uint64 = 1024 * 1024 * 1024

	defaultPauseWarmingRatio  = 0.2
	defaultResumeWarmingRatio = 0.1
	defaultFreezeAllRatio     = 0.9
	defaultResumeAllRatio     = 0.7

	defaultScanBaseSize            uint64 = 4 * 1024 * 1024
	defaultWarmingScanLagThreshold        = 30 * time.Minute
	defaultScanLagUnit                    = 10 * time.Minute
	defaultScanLagWeight                  = 0.15
	defaultMaxScanLagFactor               = 16
)

type subscriptionKind uint8

const (
	subscriptionKindChangefeed subscriptionKind = iota
	subscriptionKindSystem
)

// SubscriptionMeta carries subscription ownership information for log puller
// local resource control.
type SubscriptionMeta struct {
	ChangefeedID common.ChangeFeedID
	Kind         subscriptionKind
}

func NewChangefeedSubscriptionMeta(changefeedID common.ChangeFeedID) SubscriptionMeta {
	return SubscriptionMeta{
		ChangefeedID: changefeedID,
		Kind:         subscriptionKindChangefeed,
	}
}

func NewSystemSubscriptionMeta() SubscriptionMeta {
	return SubscriptionMeta{Kind: subscriptionKindSystem}
}

func (m SubscriptionMeta) isSystem() bool {
	return m.Kind == subscriptionKindSystem
}

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

func (l *memoryQuotaLease) Release() {
	l.once.Do(l.release)
}

type subscriptionQuotaState struct {
	eventLeases map[*memoryQuotaLease]uint64
	scanLeases  map[*memoryQuotaLease]uint64
	scanUsed    uint64
}

func newSubscriptionQuotaState() *subscriptionQuotaState {
	return &subscriptionQuotaState{
		eventLeases: make(map[*memoryQuotaLease]uint64),
		scanLeases:  make(map[*memoryQuotaLease]uint64),
	}
}

type memoryQuotaController struct {
	mu sync.Mutex

	capacity uint64
	// used tracks log puller bytes currently waiting for downstream callback.
	// It is observed for admission decisions, but the receive path never waits
	// on it after an event has arrived from TiKV.
	used uint64
	// scanUsed tracks estimated bytes of admitted warming region scans that
	// have not finished initialization yet.
	scanUsed        uint64
	warmingScanUsed uint64
	level           admissionLevel

	pauseWarmingRatio  float64
	resumeWarmingRatio float64
	freezeAllRatio     float64
	resumeAllRatio     float64

	scanEstimate uint64

	subscriptions map[SubscriptionID]*subscriptionQuotaState

	onAvailable atomic.Value // func()
}

func newMemoryQuotaController(capacity uint64, scanBaseSize uint64) *memoryQuotaController {
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
		scanEstimate:       scanBaseSize,
		subscriptions:      make(map[SubscriptionID]*subscriptionQuotaState),
	}
	return c
}

func (c *memoryQuotaController) SetOnAvailable(fn func()) {
	c.onAvailable.Store(fn)
}

func (c *memoryQuotaController) onMemoryAvailable() {
	if fn, ok := c.onAvailable.Load().(func()); ok && fn != nil {
		fn()
	}
}

func (c *memoryQuotaController) Snapshot() (used uint64, capacity uint64, level admissionLevel) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used, c.capacity, c.level
}

func (c *memoryQuotaController) ScanSnapshot() (
	scanUsed uint64,
	warmingScanUsed uint64,
	warmingScanBudget uint64,
	scanEstimate uint64,
) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.scanUsed, c.warmingScanUsed, c.warmingScanBudgetLocked(), c.scanEstimate
}

func (c *memoryQuotaController) addSubscription(span *subscribedSpan) {
	if span.meta.isSystem() {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.subscriptions[span.subID] = newSubscriptionQuotaState()
}

func (c *memoryQuotaController) removeSubscription(span *subscribedSpan) {
	if span.meta.isSystem() {
		return
	}

	var leases []*memoryQuotaLease
	c.mu.Lock()
	state := c.subscriptions[span.subID]
	for lease := range state.eventLeases {
		leases = append(leases, lease)
	}
	for lease := range state.scanLeases {
		leases = append(leases, lease)
	}
	delete(c.subscriptions, span.subID)
	c.mu.Unlock()

	for _, lease := range leases {
		lease.Release()
	}
}

func (c *memoryQuotaController) markSubscriptionInitialized(span *subscribedSpan) {
	c.onMemoryAvailable()
}

func (c *memoryQuotaController) acquireScan(region regionInfo, currentTs uint64) (*memoryQuotaLease, bool, string) {
	span := region.subscribedSpan
	if span.meta.isSystem() {
		return nil, true, ""
	}

	c.mu.Lock()
	c.refreshLevelLocked()
	if c.level == admissionFreezeAllNewScans {
		c.mu.Unlock()
		return nil, false, deferReasonMemoryFreeze
	}
	bytes := c.estimateScanSizeLocked(region, currentTs)
	warming := isWarmingScan(region, currentTs)
	if c.isWarmingScanBlockedLocked(warming, bytes) {
		c.mu.Unlock()
		return nil, false, deferReasonMemoryWarming
	}

	state := c.getSubscriptionStateLocked(span)
	lease := &memoryQuotaLease{}
	lease.release = func() {
		c.mu.Lock()
		if c.scanUsed >= bytes {
			c.scanUsed -= bytes
		} else {
			c.scanUsed = 0
		}
		if warming {
			if c.warmingScanUsed >= bytes {
				c.warmingScanUsed -= bytes
			} else {
				c.warmingScanUsed = 0
			}
		}
		if state.scanUsed >= bytes {
			state.scanUsed -= bytes
		} else {
			state.scanUsed = 0
		}
		delete(state.scanLeases, lease)
		c.refreshLevelLocked()
		c.mu.Unlock()

		c.onMemoryAvailable()
	}
	c.scanUsed += bytes
	if warming {
		c.warmingScanUsed += bytes
	}
	state.scanUsed += bytes
	state.scanLeases[lease] = bytes
	c.refreshLevelLocked()
	c.mu.Unlock()
	return lease, true, ""
}

func (c *memoryQuotaController) trackEvent(span *subscribedSpan, bytes uint64) *memoryQuotaLease {
	if span.meta.isSystem() || bytes == 0 {
		return nil
	}

	c.mu.Lock()
	c.used += bytes
	c.refreshLevelLocked()
	state := c.getSubscriptionStateLocked(span)

	lease := &memoryQuotaLease{}
	lease.release = func() {
		c.mu.Lock()
		if c.used >= bytes {
			c.used -= bytes
		} else {
			c.used = 0
		}
		delete(state.eventLeases, lease)
		c.refreshLevelLocked()
		c.mu.Unlock()

		c.onMemoryAvailable()
	}
	state.eventLeases[lease] = bytes
	c.mu.Unlock()
	return lease
}

func (c *memoryQuotaController) getSubscriptionStateLocked(
	span *subscribedSpan,
) *subscriptionQuotaState {
	return c.subscriptions[span.subID]
}

func (c *memoryQuotaController) estimateScanSizeLocked(region regionInfo, currentTs uint64) uint64 {
	raw := float64(c.scanEstimate) * scanLagFactor(region.resolvedTs(), currentTs)

	estimate := uint64(raw)
	minEstimate := c.scanEstimate
	if estimate < minEstimate {
		estimate = minEstimate
	}

	maxEstimate := c.scanEstimate * defaultMaxScanLagFactor
	if estimate > maxEstimate {
		estimate = maxEstimate
	}
	if estimate == 0 {
		estimate = c.scanEstimate
	}
	return estimate
}

func scanLagFactor(startTs uint64, currentTs uint64) float64 {
	lag := scanLagDuration(startTs, currentTs)
	if lag <= 0 {
		return 1
	}
	return min(defaultMaxScanLagFactor, 1+defaultScanLagWeight*math.Log2(1+float64(lag)/float64(defaultScanLagUnit)))
}

func isWarmingScan(region regionInfo, currentTs uint64) bool {
	span := region.subscribedSpan
	if span.initialized.Load() {
		return false
	}
	return scanLagDuration(region.resolvedTs(), currentTs) >= defaultWarmingScanLagThreshold
}

func scanLagDuration(startTs uint64, currentTs uint64) time.Duration {
	if startTs == 0 || currentTs <= startTs {
		return 0
	}
	lag := oracle.GetTimeFromTS(currentTs).Sub(oracle.GetTimeFromTS(startTs))
	if lag <= 0 {
		return 0
	}
	return lag
}

func (c *memoryQuotaController) isWarmingScanBlockedLocked(warming bool, bytes uint64) bool {
	if !warming {
		return false
	}
	if c.level == admissionPauseWarming {
		return true
	}
	return c.warmingScanUsed+bytes > c.warmingScanBudgetLocked()
}

func (c *memoryQuotaController) warmingScanBudgetLocked() uint64 {
	if c.capacity == 0 {
		return 0
	}
	budget := uint64(float64(c.capacity) * c.pauseWarmingRatio)
	return max(budget, c.scanEstimate)
}

func (c *memoryQuotaController) refreshLevelLocked() {
	if c.capacity == 0 {
		c.level = admissionNormal
		return
	}
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
		default:
			c.level = admissionNormal
		}
	}
}
