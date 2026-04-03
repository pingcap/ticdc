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
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"go.uber.org/zap"
)

const (
	notLeaderRetryBaseDelay      = 50 * time.Millisecond
	rpcCtxRetryBaseDelay         = 100 * time.Millisecond
	busyRetryBaseDelay           = 100 * time.Millisecond
	storeFailureRetryBaseDelay   = 200 * time.Millisecond
	shortRetryMaxDelay           = 1 * time.Second
	busyRetryMaxDelay            = 3 * time.Second
	storeFailureRetryMaxDelay    = 5 * time.Second
	rangeReloadDebounceInterval  = 25 * time.Millisecond
	rangeReloadImmediateFlushMax = 128
)

type delayedAction func(context.Context)

type delayedTask struct {
	run delayedAction
	at  time.Time
}

type delayedTaskHeap []delayedTask

func (h delayedTaskHeap) Len() int { return len(h) }

func (h delayedTaskHeap) Less(i, j int) bool {
	return h[i].at.Before(h[j].at)
}

func (h delayedTaskHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *delayedTaskHeap) Push(x any) {
	*h = append(*h, x.(delayedTask))
}

func (h *delayedTaskHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

type retryScheduler struct {
	client *subscriptionClient
	addCh  chan delayedTask
}

func newRetryScheduler(client *subscriptionClient) *retryScheduler {
	return &retryScheduler{
		client: client,
		addCh:  make(chan delayedTask, 4096),
	}
}

func (s *retryScheduler) schedule(delay time.Duration, action delayedAction) {
	if delay <= 0 {
		action(s.client.ctx)
		return
	}

	task := delayedTask{run: action, at: time.Now().Add(delay)}
	select {
	case <-s.client.ctx.Done():
	case s.addCh <- task:
	}
}

func (s *retryScheduler) run(ctx context.Context) error {
	var (
		taskHeap delayedTaskHeap
		timer    *time.Timer
		timerCh  <-chan time.Time
	)
	heap.Init(&taskHeap)
	resetTimer := func(d time.Duration) {
		if d < 0 {
			d = 0
		}
		if timer == nil {
			timer = time.NewTimer(d)
		} else {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(d)
		}
		timerCh = timer.C
	}
	disableTimer := func() {
		if timer != nil {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
		timerCh = nil
	}
	for {
		if taskHeap.Len() > 0 {
			resetTimer(time.Until(taskHeap[0].at))
		} else {
			disableTimer()
		}

		select {
		case <-ctx.Done():
			disableTimer()
			return ctx.Err()
		case task := <-s.addCh:
			heap.Push(&taskHeap, task)
		case <-timerCh:
			now := time.Now()
			for taskHeap.Len() > 0 {
				task := taskHeap[0]
				if task.at.After(now) {
					break
				}
				heap.Pop(&taskHeap)
				task.run(ctx)
			}
		}
	}
}

type retryBackoffReason string

const (
	retryBackoffNotLeader    retryBackoffReason = "not-leader"
	retryBackoffRPCContext   retryBackoffReason = "rpc-context"
	retryBackoffBusy         retryBackoffReason = "busy"
	retryBackoffStoreFailure retryBackoffReason = "store-failure"
)

type retryBackoffKey struct {
	subID    SubscriptionID
	regionID uint64
	reason   retryBackoffReason
}

type retryBackoffManager struct {
	mu       sync.Mutex
	attempts map[retryBackoffKey]int
}

func newRetryBackoffManager() *retryBackoffManager {
	return &retryBackoffManager{attempts: make(map[retryBackoffKey]int)}
}

func (m *retryBackoffManager) nextDelay(key retryBackoffKey, base, max time.Duration) time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	attempt := m.attempts[key]
	if attempt < 0 {
		attempt = 0
	}
	m.attempts[key] = attempt + 1

	delay := base
	for i := 0; i < attempt; i++ {
		if delay >= max/2 {
			return max
		}
		delay *= 2
	}
	if delay > max {
		return max
	}
	return delay
}

func (m *retryBackoffManager) resetRegion(subID SubscriptionID, regionID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key := range m.attempts {
		if key.subID == subID && key.regionID == regionID {
			delete(m.attempts, key)
		}
	}
}

type storeBackoffState struct {
	consecutiveFailures int
	cooldownUntil       time.Time
}

type storeBackoffManager struct {
	mu     sync.Mutex
	states map[string]storeBackoffState
}

func newStoreBackoffManager() *storeBackoffManager {
	return &storeBackoffManager{states: make(map[string]storeBackoffState)}
}

func (m *storeBackoffManager) markFailure(storeAddr string) time.Duration {
	if storeAddr == "" {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.states[storeAddr]
	state.consecutiveFailures++
	delay := storeFailureRetryBaseDelay
	for i := 1; i < state.consecutiveFailures; i++ {
		if delay >= storeFailureRetryMaxDelay/2 {
			delay = storeFailureRetryMaxDelay
			break
		}
		delay *= 2
	}
	if delay > storeFailureRetryMaxDelay {
		delay = storeFailureRetryMaxDelay
	}
	state.cooldownUntil = time.Now().Add(delay)
	m.states[storeAddr] = state
	return delay
}

func (m *storeBackoffManager) markSuccess(storeAddr string) {
	if storeAddr == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.states, storeAddr)
}

func (m *storeBackoffManager) cooldownRemaining(storeAddr string) time.Duration {
	if storeAddr == "" {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	state, ok := m.states[storeAddr]
	if !ok {
		return 0
	}
	remaining := time.Until(state.cooldownUntil)
	if remaining <= 0 {
		delete(m.states, storeAddr)
		return 0
	}
	return remaining
}

func (s *subscriptionClient) schedulePriorityTaskAfter(task PriorityTask, delay time.Duration) {
	s.retryScheduler.schedule(delay, func(ctx context.Context) {
		select {
		case <-ctx.Done():
			return
		default:
		}
		s.regionTaskQueue.Push(task)
	})
}

func (s *subscriptionClient) scheduleRegionRequestAfter(
	ctx context.Context,
	region regionInfo,
	priority TaskType,
	delay time.Duration,
) {
	s.retryScheduler.schedule(delay, func(taskCtx context.Context) {
		s.scheduleRegionRequest(taskCtx, region, priority)
	})
}

func (s *subscriptionClient) scheduleRangeTaskAfter(task rangeTask, delay time.Duration) {
	s.retryScheduler.schedule(delay, func(ctx context.Context) {
		select {
		case <-ctx.Done():
		case s.rangeTaskCh <- task:
		}
	})
}

func (s *subscriptionClient) addReloadRangeTask(
	span heartbeatpb.TableSpan,
	subscribedSpan *subscribedSpan,
	filterLoop bool,
	priority TaskType,
) {
	if s.rangeReloadAggregator == nil {
		s.scheduleRangeTaskAfter(rangeTask{
			span:           span,
			subscribedSpan: subscribedSpan,
			filterLoop:     filterLoop,
			priority:       priority,
		}, 0)
		return
	}
	s.rangeReloadAggregator.add(span, subscribedSpan, filterLoop, priority)
}

func (s *subscriptionClient) getRetryBackoffDelay(
	subID SubscriptionID,
	regionID uint64,
	reason retryBackoffReason,
) time.Duration {
	key := retryBackoffKey{subID: subID, regionID: regionID, reason: reason}
	switch reason {
	case retryBackoffNotLeader:
		return s.retryBackoff.nextDelay(key, notLeaderRetryBaseDelay, shortRetryMaxDelay)
	case retryBackoffRPCContext:
		return s.retryBackoff.nextDelay(key, rpcCtxRetryBaseDelay, shortRetryMaxDelay)
	case retryBackoffBusy:
		return s.retryBackoff.nextDelay(key, busyRetryBaseDelay, busyRetryMaxDelay)
	case retryBackoffStoreFailure:
		return s.retryBackoff.nextDelay(key, storeFailureRetryBaseDelay, storeFailureRetryMaxDelay)
	default:
		return busyRetryBaseDelay
	}
}

func (s *subscriptionClient) resetRetryBackoff(subID SubscriptionID, regionID uint64) {
	if s == nil || s.retryBackoff == nil {
		return
	}
	s.retryBackoff.resetRegion(subID, regionID)
}

func (s *subscriptionClient) markStoreFailure(storeAddr string) time.Duration {
	if s == nil || s.storeBackoff == nil {
		return 0
	}
	delay := s.storeBackoff.markFailure(storeAddr)
	if delay > 0 {
		log.Info("subscription client mark store cooldown",
			zap.String("addr", storeAddr),
			zap.Duration("delay", delay))
	}
	return delay
}

func (s *subscriptionClient) markStoreSuccess(storeAddr string) {
	if s == nil || s.storeBackoff == nil {
		return
	}
	s.storeBackoff.markSuccess(storeAddr)
}

func (s *subscriptionClient) getStoreCooldown(storeAddr string) time.Duration {
	if s == nil || s.storeBackoff == nil {
		return 0
	}
	return s.storeBackoff.cooldownRemaining(storeAddr)
}
