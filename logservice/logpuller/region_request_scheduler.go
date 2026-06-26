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
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/utils/priorityqueue"
	kvclientv2 "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// regionRequestScheduler owns region request admission from the global
// priority queue to per-store request workers.
type regionRequestScheduler struct {
	config   *SubscriptionClientConfig
	upstream *upstreamHandle

	eventSink      *regionEventSink
	failureHandler *regionFailureHandler

	queue *priorityqueue.PriorityQueue[*regionPriorityTask]
	seq   atomic.Uint64

	stores sync.Map
}

func newRegionRequestScheduler(client *subscriptionClient) *regionRequestScheduler {
	return &regionRequestScheduler{
		config:         client.config,
		upstream:       client.upstream,
		eventSink:      client.eventSink,
		failureHandler: client.failureHandler,
		queue:          priorityqueue.New[*regionPriorityTask](),
	}
}

func (s *regionRequestScheduler) submit(taskType TaskType, region regionInfo) {
	task := newRegionPriorityTask(taskType, region, s.upstream.pdClock.CurrentTS(), s.seq.Add(1))
	s.queue.Push(task)
}

func (s *regionRequestScheduler) close() {
	s.queue.Close()
	s.releaseAdmittedRegionRequests()
}

func (s *regionRequestScheduler) updateMetrics() {
	count := 0
	s.stores.Range(func(_, value any) bool {
		store := value.(*requestedStore)
		for _, worker := range store.requestWorkers {
			count += worker.requestCache.getPendingCount()
		}
		return true
	})
	metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("pending").
		Set(float64(count))
}

func (s *regionRequestScheduler) broadcastDeregister(subID SubscriptionID, filterLoop bool) {
	s.stores.Range(func(_ any, value any) bool {
		rs := value.(*requestedStore)
		for _, worker := range rs.requestWorkers {
			worker.controlQueue.push(deregisterRequest{
				subID:      subID,
				filterLoop: filterLoop,
			})
		}
		return true
	})
}

func (s *regionRequestScheduler) attachRPCContextForRegion(ctx context.Context, region regionInfo) (regionInfo, bool) {
	bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
	rpcCtx, err := s.upstream.regionCache.GetTiKVRPCContext(bo, region.verID, kvclientv2.ReplicaReadLeader, 0)
	if rpcCtx != nil {
		region.rpcCtx = rpcCtx
		return region, true
	}
	if err != nil {
		log.Debug("subscription client get rpc context fail",
			zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)),
			zap.Uint64("regionID", region.verID.GetID()),
			zap.Error(err))
	}
	s.failureHandler.Report(newRegionErrorInfo(region, &rpcCtxUnavailableErr{verID: region.verID}))
	return region, false
}

// releaseAdmittedRegionRequests releases region requests admitted to store
// workers during scheduler shutdown.
func (s *regionRequestScheduler) releaseAdmittedRegionRequests() {
	s.stores.Range(func(_, value any) bool {
		rs := value.(*requestedStore)
		rs.releaseAdmittedRegionRequests()
		return true
	})
}

// Run receives region tasks from the global priority queue and admits them to
// the corresponding TiKV store worker.
func (s *regionRequestScheduler) Run(ctx context.Context, eg *errgroup.Group) error {
	cfg := config.GetGlobalServerConfig()
	pendingRegionRequestQueueSize := cfg.Debug.Puller.PendingRegionRequestQueueSize
	// Store creation is serialized by the single scheduler loop.
	getStore := func(storeAddr string) *requestedStore {
		var rs *requestedStore
		if v, ok := s.stores.Load(storeAddr); ok {
			rs = v.(*requestedStore)
			return rs
		}

		perWorkerQueueSize := pendingRegionRequestQueueSize / int(s.config.RegionRequestWorkerPerStore)
		if perWorkerQueueSize <= 0 {
			log.Warn("pending region request queue size is smaller than the number of workers, adjust per worker queue size to 1",
				zap.Int("pendingRegionRequestQueueSize", pendingRegionRequestQueueSize),
				zap.Uint("regionRequestWorkerPerStore", s.config.RegionRequestWorkerPerStore))
			perWorkerQueueSize = 1
		}

		rs = &requestedStore{
			scheduler:      s,
			storeAddr:      storeAddr,
			requestWorkers: make([]*regionRequestWorker, 0, s.config.RegionRequestWorkerPerStore),
		}
		for i := uint(0); i < s.config.RegionRequestWorkerPerStore; i++ {
			requestWorker := newRegionRequestWorker(
				ctx,
				eg,
				rs,
				perWorkerQueueSize,
				s.upstream,
				s.eventSink,
				s.failureHandler,
			)
			rs.requestWorkers = append(rs.requestWorkers, requestWorker)
		}
		s.stores.Store(storeAddr, rs)
		return rs
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		regionTask, err := s.queue.Pop(ctx)
		if err != nil {
			if errors.Is(err, priorityqueue.ErrClosed) {
				return nil
			}
			return err
		}

		region := regionTask.GetRegionInfo()
		if region.isStopped() {
			s.broadcastDeregister(region.subscribedSpan.subID, region.filterLoop)
			continue
		}

		promotedStore := regionTask.deferredStore.Load()
		region, ok := s.attachRPCContextForRegion(ctx, region)
		if !ok {
			if promotedStore != nil {
				promotedStore.finishPromotedTask(regionTask)
				promotedStore.promoteDeferredTask()
			}
			continue
		}

		store := getStore(region.rpcCtx.Addr)
		if promotedStore != nil && promotedStore != store {
			promotedStore.finishPromotedTask(regionTask)
			promotedStore.promoteDeferredTask()
		}
		if store.hasDeferredTaskAhead(regionTask) {
			store.deferTask(regionTask)
			store.maybePromoteDeferredTask()
			continue
		}
		force := regionTask.Priority() <= forcedPriorityBase

		var worker *regionRequestWorker
		ok, worker, err = store.addRegion(ctx, region, force)
		if err != nil {
			log.Warn("subscription client add region request failed",
				zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)),
				zap.Uint64("regionID", region.verID.GetID()),
				zap.Error(err))
			return err
		}

		if !ok {
			store.deferTask(regionTask)
			store.maybePromoteDeferredTask()
			continue
		}
		store.finishPromotedTask(regionTask)

		log.Debug("subscription client will request a region",
			zap.Uint64("workID", worker.workerID),
			zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)),
			zap.Uint64("regionID", region.verID.GetID()),
			zap.String("addr", store.storeAddr))
	}
}

// requestedStore is the scheduler's local state for one TiKV store.
type requestedStore struct {
	scheduler *regionRequestScheduler

	storeAddr string
	// nextWorker is the round-robin cursor used to select the next worker to attempt.
	nextWorker atomic.Uint32
	// requestWorkers are fully created before requestedStore is published
	// and remain immutable afterwards.
	requestWorkers []*regionRequestWorker

	deferredTasks struct {
		sync.Mutex
		// tasks are blocked because this store currently has no worker capacity.
		tasks []*regionPriorityTask
		// promoted is the deferred task that has been pushed back to the global
		// queue but has not been admitted by this store yet.
		promoted *regionPriorityTask
	}
}

func (rs *requestedStore) addRegion(
	ctx context.Context, region regionInfo, force bool,
) (bool, *regionRequestWorker, error) {
	workers := rs.requestWorkers

	start := int(rs.nextWorker.Add(1)) % len(workers)
	for i := range len(workers) {
		worker := workers[(start+i)%len(workers)]
		ok, err := worker.add(ctx, region, force)
		if err != nil || ok {
			return ok, worker, err
		}
	}
	return false, nil, nil
}

// hasDeferredTaskAhead reports whether an earlier deferred task should be
// retried before task for this store.
func (rs *requestedStore) hasDeferredTaskAhead(task *regionPriorityTask) bool {
	rs.deferredTasks.Lock()
	defer rs.deferredTasks.Unlock()

	if rs.deferredTasks.promoted == task {
		// The promoted task was already removed from the deferred queue and is
		// being retried through the global priority queue.
		return false
	}
	return rs.deferredTasks.promoted != nil || len(rs.deferredTasks.tasks) > 0
}

func (rs *requestedStore) deferTask(task *regionPriorityTask) {
	rs.deferredTasks.Lock()
	if rs.deferredTasks.promoted == task {
		rs.deferredTasks.promoted = nil
		task.deferredStore.Store(nil)
		rs.deferredTasks.tasks = append([]*regionPriorityTask{task}, rs.deferredTasks.tasks...)
	} else {
		rs.deferredTasks.tasks = append(rs.deferredTasks.tasks, task)
	}
	rs.deferredTasks.Unlock()
}

func (rs *requestedStore) maybePromoteDeferredTask() {
	rs.deferredTasks.Lock()
	if rs.deferredTasks.promoted != nil || len(rs.deferredTasks.tasks) == 0 {
		rs.deferredTasks.Unlock()
		return
	}
	force := rs.deferredTasks.tasks[0].Priority() <= forcedPriorityBase
	rs.deferredTasks.Unlock()

	if rs.hasRequestCapacity(force) {
		rs.promoteDeferredTask()
	}
}

func (rs *requestedStore) hasRequestCapacity(force bool) bool {
	for _, worker := range rs.requestWorkers {
		if worker.requestCache.canAdd(force) {
			return true
		}
	}
	return false
}

func (rs *requestedStore) releaseAdmittedRegionRequests() {
	for _, worker := range rs.requestWorkers {
		worker.releaseAdmittedRegionRequests()
	}
}

func (rs *requestedStore) finishPromotedTask(task *regionPriorityTask) {
	rs.deferredTasks.Lock()
	if rs.deferredTasks.promoted == task {
		rs.deferredTasks.promoted = nil
		task.deferredStore.Store(nil)
	}
	rs.deferredTasks.Unlock()
}

func (rs *requestedStore) promoteDeferredTask() {
	rs.deferredTasks.Lock()
	if rs.deferredTasks.promoted != nil || len(rs.deferredTasks.tasks) == 0 {
		rs.deferredTasks.Unlock()
		return
	}
	task := rs.deferredTasks.tasks[0]
	copy(rs.deferredTasks.tasks, rs.deferredTasks.tasks[1:])
	rs.deferredTasks.tasks[len(rs.deferredTasks.tasks)-1] = nil
	rs.deferredTasks.tasks = rs.deferredTasks.tasks[:len(rs.deferredTasks.tasks)-1]
	rs.deferredTasks.promoted = task
	task.deferredStore.Store(rs)
	rs.deferredTasks.Unlock()

	rs.scheduler.queue.Push(task)
}
