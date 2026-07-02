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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/utils/priorityqueue"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type regionRequestQuota struct {
	once    sync.Once
	release func()
}

func (p *regionRequestQuota) Release() {
	p.once.Do(p.release)
}

type storeQuota struct {
	mu       sync.Mutex
	capacity int
	used     int

	onRelease func()
}

func newStoreQuota(capacity int, onRelease func()) *storeQuota {
	return &storeQuota{
		capacity:  capacity,
		onRelease: onRelease,
	}
}

func (q *storeQuota) TryAcquire() (*regionRequestQuota, bool) {
	q.mu.Lock()
	if q.used >= q.capacity {
		q.mu.Unlock()
		return nil, false
	}
	q.used++
	q.mu.Unlock()

	return &regionRequestQuota{
		release: func() {
			q.mu.Lock()
			q.used--
			q.mu.Unlock()
			if q.onRelease != nil {
				q.onRelease()
			}
		},
	}, true
}

func (q *storeQuota) Snapshot() (used int, capacity int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.used, q.capacity
}

// requestedStore is the scheduler's local state for one TiKV store.
type requestedStore struct {
	scheduler *regionRequestScheduler

	storeAddr string
	quota     *storeQuota
	// nextWorker is the round-robin cursor used to select the next worker to attempt.
	nextWorker atomic.Uint32
	// requestWorkers are fully created before requestedStore is published
	// and remain immutable afterwards.
	requestWorkers []*regionRequestWorker

	// pendingTasks holds tasks that have been routed to this store but are
	// waiting for store quota or worker request-cache capacity. It is mutated
	// only by the single regionRequestScheduler.Run loop.
	pendingTasks *priorityqueue.PriorityQueue[*regionPriorityTask]
	// pendingTaskCount mirrors pendingTasks.Len() for metrics. It lets the
	// metrics updater observe the size without touching the scheduler-owned
	// priority queue.
	pendingTaskCount atomic.Int64

	notifyMu       sync.Mutex
	notifyEnqueued bool
}

func newRequestedStore(scheduler *regionRequestScheduler, storeAddr string) *requestedStore {
	pendingRegionRequestQueueSize := scheduler.config.PendingRegionRequestQueueSize
	regionRequestWorkerPerStore := scheduler.config.RegionRequestWorkerPerStore
	perWorkerQueueSize := pendingRegionRequestQueueSize / int(regionRequestWorkerPerStore)
	if perWorkerQueueSize <= 0 {
		log.Warn("pending region request queue size is smaller than the number of workers, adjust per worker queue size to 1",
			zap.Int("pendingRegionRequestQueueSize", pendingRegionRequestQueueSize),
			zap.Uint("regionRequestWorkerPerStore", regionRequestWorkerPerStore))
		perWorkerQueueSize = 1
	}
	perStoreQuotaSize := perWorkerQueueSize * int(regionRequestWorkerPerStore)

	rs := &requestedStore{
		scheduler:      scheduler,
		storeAddr:      storeAddr,
		requestWorkers: make([]*regionRequestWorker, 0, regionRequestWorkerPerStore),
		pendingTasks:   priorityqueue.New[*regionPriorityTask](),
	}
	rs.quota = newStoreQuota(perStoreQuotaSize, rs.NotifyAvailable)
	for range regionRequestWorkerPerStore {
		requestCache := newRequestCache(perWorkerQueueSize, rs.NotifyAvailable)
		requestWorker := newRegionRequestWorker(
			storeAddr,
			requestCache,
			scheduler.upstream,
			scheduler.eventSink,
			scheduler.failureHandler,
		)
		rs.requestWorkers = append(rs.requestWorkers, requestWorker)
	}
	return rs
}

func (rs *requestedStore) Run(ctx context.Context, eg *errgroup.Group) {
	for _, worker := range rs.requestWorkers {
		eg.Go(func() error {
			return worker.Run(ctx)
		})
	}
}

func (rs *requestedStore) Close() {
	for _, worker := range rs.requestWorkers {
		worker.requestCache.close()
	}
}

func (rs *requestedStore) PushPendingTask(task *regionPriorityTask) {
	rs.pendingTasks.Push(task)
	rs.pendingTaskCount.Add(1)
}

func (rs *requestedStore) TryPopPendingTask() (*regionPriorityTask, bool) {
	task, ok := rs.pendingTasks.TryPop()
	if ok {
		rs.pendingTaskCount.Add(-1)
	}
	return task, ok
}

func (rs *requestedStore) PendingTaskCount() int {
	return int(rs.pendingTaskCount.Load())
}

func (rs *requestedStore) AddRegion(
	ctx context.Context, region regionInfo, force bool, quota *regionRequestQuota,
) (bool, *regionRequestWorker, error) {
	workers := rs.requestWorkers

	start := int(rs.nextWorker.Add(1)) % len(workers)
	for i := range len(workers) {
		worker := workers[(start+i)%len(workers)]
		ok, err := worker.requestCache.add(ctx, region, force, quota)
		if err != nil || ok {
			return ok, worker, err
		}
	}
	return false, nil, nil
}

func (rs *requestedStore) NotifyAvailable() {
	rs.notifyMu.Lock()
	if rs.notifyEnqueued {
		rs.notifyMu.Unlock()
		return
	}
	rs.notifyEnqueued = true
	rs.notifyMu.Unlock()

	rs.scheduler.storeAvailable.Push(rs)
	rs.scheduler.notifyScheduler()
}

func (rs *requestedStore) MarkAvailableDequeued() {
	rs.notifyMu.Lock()
	rs.notifyEnqueued = false
	rs.notifyMu.Unlock()
}
