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
	"sync/atomic"

	"github.com/pingcap/ticdc/pkg/common"
)

const (
	defaultLogPullerMemoryQuota uint64 = 1024 * 1024 * 1024

	defaultPauseWarmingRatio  = 0.2
	defaultResumeWarmingRatio = 0.1
	defaultFreezeAllRatio     = 0.9
	defaultResumeAllRatio     = 0.7
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

type changefeedPhase uint8

const (
	changefeedPhaseWarming changefeedPhase = iota
	changefeedPhaseNormal
)

type memoryQuotaLease struct {
	once    sync.Once
	release func()
}

func (l *memoryQuotaLease) Release() {
	if l == nil {
		return
	}
	l.once.Do(l.release)
}

type changefeedQuotaState struct {
	id common.ChangeFeedID

	mu      sync.Mutex
	phase   changefeedPhase
	spans   map[SubscriptionID]*subscribedSpan
	leases  map[*memoryQuotaLease]SubscriptionID
	memUsed uint64
}

func newChangefeedQuotaState(id common.ChangeFeedID) *changefeedQuotaState {
	return &changefeedQuotaState{
		id:     id,
		phase:  changefeedPhaseWarming,
		spans:  make(map[SubscriptionID]*subscribedSpan),
		leases: make(map[*memoryQuotaLease]SubscriptionID),
	}
}

type memoryQuotaController struct {
	mu   sync.Mutex
	cond *sync.Cond

	capacity uint64
	used     uint64
	level    admissionLevel

	pauseWarmingRatio  float64
	resumeWarmingRatio float64
	freezeAllRatio     float64
	resumeAllRatio     float64

	changefeeds map[common.ChangeFeedID]*changefeedQuotaState

	onAvailable atomic.Value // func()
}

func newMemoryQuotaController(capacity uint64) *memoryQuotaController {
	if capacity == 0 {
		capacity = defaultLogPullerMemoryQuota
	}
	c := &memoryQuotaController{
		capacity:           capacity,
		level:              admissionNormal,
		pauseWarmingRatio:  defaultPauseWarmingRatio,
		resumeWarmingRatio: defaultResumeWarmingRatio,
		freezeAllRatio:     defaultFreezeAllRatio,
		resumeAllRatio:     defaultResumeAllRatio,
		changefeeds:        make(map[common.ChangeFeedID]*changefeedQuotaState),
	}
	c.cond = sync.NewCond(&c.mu)
	return c
}

func (c *memoryQuotaController) SetOnAvailable(fn func()) {
	c.onAvailable.Store(fn)
}

func (c *memoryQuotaController) WakeAll() {
	c.mu.Lock()
	c.cond.Broadcast()
	c.mu.Unlock()
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

func (c *memoryQuotaController) addSubscription(span *subscribedSpan) {
	if span.meta.isSystem() {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	state := c.getOrCreateChangefeedStateLocked(span.meta.ChangefeedID)
	state.mu.Lock()
	state.spans[span.subID] = span
	state.mu.Unlock()
}

func (c *memoryQuotaController) removeSubscription(span *subscribedSpan) {
	if span == nil || span.meta.isSystem() {
		return
	}

	var leases []*memoryQuotaLease
	c.mu.Lock()
	state := c.changefeeds[span.meta.ChangefeedID]
	if state != nil {
		state.mu.Lock()
		delete(state.spans, span.subID)
		for lease, subID := range state.leases {
			if subID == span.subID {
				leases = append(leases, lease)
			}
		}
		if len(state.spans) == 0 {
			delete(c.changefeeds, span.meta.ChangefeedID)
		}
		state.mu.Unlock()
	}
	c.mu.Unlock()

	for _, lease := range leases {
		lease.Release()
	}
}

func (c *memoryQuotaController) markSubscriptionInitialized(span *subscribedSpan) {
	if span == nil || span.meta.isSystem() {
		return
	}

	c.mu.Lock()
	state := c.changefeeds[span.meta.ChangefeedID]
	c.mu.Unlock()
	if state == nil {
		return
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.phase == changefeedPhaseNormal {
		return
	}
	for _, subSpan := range state.spans {
		if !subSpan.initialized.Load() {
			return
		}
	}
	state.phase = changefeedPhaseNormal
	c.onMemoryAvailable()
}

func (c *memoryQuotaController) allowNewScan(span *subscribedSpan) (bool, string) {
	if span == nil || span.meta.isSystem() {
		return true, ""
	}

	c.mu.Lock()
	c.refreshLevelLocked()
	level := c.level
	state := c.getOrCreateChangefeedStateLocked(span.meta.ChangefeedID)
	c.mu.Unlock()

	if level == admissionFreezeAllNewScans {
		return false, deferReasonMemoryFreeze
	}
	if level == admissionPauseWarming {
		state.mu.Lock()
		phase := state.phase
		state.mu.Unlock()
		if phase == changefeedPhaseWarming {
			return false, deferReasonMemoryWarming
		}
	}
	return true, ""
}

func (c *memoryQuotaController) acquireEvent(
	ctx context.Context,
	span *subscribedSpan,
	bytes uint64,
) *memoryQuotaLease {
	if span == nil || span.meta.isSystem() || bytes == 0 {
		return nil
	}

	c.mu.Lock()
	for c.used+bytes > c.capacity && c.used > 0 {
		if ctx.Err() != nil {
			c.mu.Unlock()
			return nil
		}
		c.cond.Wait()
	}
	c.used += bytes
	c.refreshLevelLocked()
	state := c.getOrCreateChangefeedStateLocked(span.meta.ChangefeedID)

	lease := &memoryQuotaLease{}
	lease.release = func() {
		c.mu.Lock()
		if c.used >= bytes {
			c.used -= bytes
		} else {
			c.used = 0
		}
		c.refreshLevelLocked()
		c.cond.Broadcast()
		c.mu.Unlock()

		state.mu.Lock()
		delete(state.leases, lease)
		if state.memUsed >= bytes {
			state.memUsed -= bytes
		} else {
			state.memUsed = 0
		}
		state.mu.Unlock()

		c.onMemoryAvailable()
	}

	state.mu.Lock()
	state.leases[lease] = span.subID
	state.memUsed += bytes
	state.mu.Unlock()
	c.mu.Unlock()
	return lease
}

func (c *memoryQuotaController) getOrCreateChangefeedStateLocked(
	changefeedID common.ChangeFeedID,
) *changefeedQuotaState {
	state := c.changefeeds[changefeedID]
	if state != nil {
		return state
	}
	state = newChangefeedQuotaState(changefeedID)
	c.changefeeds[changefeedID] = state
	return state
}

func (c *memoryQuotaController) refreshLevelLocked() {
	if c.capacity == 0 {
		c.level = admissionNormal
		return
	}
	usage := float64(c.used) / float64(c.capacity)
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
