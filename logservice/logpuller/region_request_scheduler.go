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
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/utils/chann"
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

	// queue stores newly submitted tasks before they are routed to a TiKV store.
	queue *priorityqueue.PriorityQueue[*regionPriorityTask]
	// seq is assigned to each task and used as a FIFO tie-breaker when
	// multiple tasks have the same priority.
	seq atomic.Uint64

	// schedulerNotify wakes Run to re-check queue or storeAvailable.
	schedulerNotify chan struct{}
	// storeAvailable is an unbounded ready-store queue. A store is pushed here
	// when its quota is released, so deferred tasks for that store can be
	// retried without scanning all stores or dropping notifications.
	storeAvailable *chann.UnlimitedChannel[*requestedStore, any]

	// stores maps TiKV store address to its scheduler-local state.
	stores sync.Map
}

func newRegionRequestScheduler(client *subscriptionClient) *regionRequestScheduler {
	return &regionRequestScheduler{
		config:          client.config,
		upstream:        client.upstream,
		eventSink:       client.eventSink,
		failureHandler:  client.failureHandler,
		queue:           priorityqueue.New[*regionPriorityTask](),
		schedulerNotify: make(chan struct{}, 1),
		storeAvailable:  chann.NewUnlimitedChannelDefault[*requestedStore](),
	}
}

// Run admits region tasks from two sources: new tasks from the global priority
// queue, and deferred tasks from stores whose quota has become available.
func (s *regionRequestScheduler) Run(ctx context.Context, eg *errgroup.Group) error {
	// Store creation is serialized by the single scheduler loop.
	getStore := func(storeAddr string) *requestedStore {
		var rs *requestedStore
		if v, ok := s.stores.Load(storeAddr); ok {
			rs = v.(*requestedStore)
			return rs
		}

		rs = newRequestedStore(ctx, eg, s, storeAddr)
		s.stores.Store(storeAddr, rs)
		return rs
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Run is the only consumer of storeAvailable, so Len > 0 means this
		// GetWithContext will not block.
		if s.storeAvailable.Len() > 0 {
			store, ok, err := s.storeAvailable.GetWithContext(context.Background())
			if err != nil || !ok {
				continue
			}
			store.MarkAvailableDequeued()
			if err := s.handleDeferredTasks(ctx, store); err != nil {
				return err
			}
			continue
		}

		regionTask, ok := s.queue.TryPop()
		if ok {
			if err := s.handleNewTask(ctx, getStore, regionTask); err != nil {
				return err
			}
			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.schedulerNotify:
		}
	}
}

func (s *regionRequestScheduler) Submit(taskType TaskType, region regionInfo) {
	task := newRegionPriorityTask(taskType, region, s.upstream.pdClock.CurrentTS(), s.seq.Add(1))
	if s.queue.Push(task) {
		s.notifyScheduler()
	}
}

func (s *regionRequestScheduler) BroadcastDeregister(subID SubscriptionID, filterLoop bool) {
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

func (s *regionRequestScheduler) UpdateMetrics() {
	count := 0
	s.stores.Range(func(_, value any) bool {
		store := value.(*requestedStore)
		for _, worker := range store.requestWorkers {
			count += worker.requestCache.PendingCount()
		}
		return true
	})
	metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("pending").
		Set(float64(count))
}

func (s *regionRequestScheduler) Close() {
	s.queue.Close()
	s.notifyScheduler()
	s.stores.Range(func(_, value any) bool {
		store := value.(*requestedStore)
		store.Close()
		return true
	})
}

func (s *regionRequestScheduler) notifyScheduler() {
	select {
	case s.schedulerNotify <- struct{}{}:
	default:
	}
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

type getRequestedStoreFunc func(storeAddr string) *requestedStore

func (s *regionRequestScheduler) handleDeferredTasks(ctx context.Context, store *requestedStore) error {
	for {
		task, ok := store.pendingTasks.TryPop()
		if !ok {
			return nil
		}

		region, ok := s.attachRPCContextForRegion(ctx, task.GetRegionInfo())
		if !ok {
			continue
		}
		task.regionInfo = region

		if region.rpcCtx.Addr != store.storeAddr {
			s.failureHandler.Report(newRegionErrorInfo(region, &rpcCtxChangedError{
				verID: region.verID,
				from:  store.storeAddr,
				to:    region.rpcCtx.Addr,
			}))
			continue
		}

		ok, err := s.tryAdmitTask(ctx, store, task, region)
		if err != nil {
			return err
		}
		if !ok {
			store.pendingTasks.Push(task)
			return nil
		}
	}
}

func (s *regionRequestScheduler) handleNewTask(
	ctx context.Context,
	getStore getRequestedStoreFunc,
	task *regionPriorityTask,
) error {
	region, ok := s.attachRPCContextForRegion(ctx, task.GetRegionInfo())
	if !ok {
		return nil
	}
	task.regionInfo = region

	store := getStore(region.rpcCtx.Addr)
	if store.pendingTasks.Len() > 0 {
		store.pendingTasks.Push(task)
		store.NotifyAvailable()
		return nil
	}

	ok, err := s.tryAdmitTask(ctx, store, task, region)
	if err != nil {
		return err
	}
	if !ok {
		store.pendingTasks.Push(task)
	}
	return nil
}

func (s *regionRequestScheduler) tryAdmitTask(
	ctx context.Context,
	store *requestedStore,
	task *regionPriorityTask,
	region regionInfo,
) (bool, error) {
	force := task.Priority() <= forcedPriorityBase
	acquiredQuota, ok := store.quota.TryAcquire()
	if !ok {
		return false, nil
	}
	ok, worker, err := store.AddRegion(ctx, region, force, acquiredQuota)
	if err != nil {
		acquiredQuota.Release()
		log.Warn("subscription client add region request failed",
			zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)),
			zap.Uint64("regionID", region.verID.GetID()),
			zap.Error(err))
		return false, err
	}
	if !ok {
		acquiredQuota.Release()
		return false, nil
	}

	log.Debug("subscription client will request a region",
		zap.Uint64("workID", worker.workerID),
		zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)),
		zap.Uint64("regionID", region.verID.GetID()),
		zap.String("addr", store.storeAddr))
	return true, nil
}

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

	notifyMu       sync.Mutex
	notifyEnqueued bool
}

func newRequestedStore(
	ctx context.Context,
	eg *errgroup.Group,
	scheduler *regionRequestScheduler,
	storeAddr string,
) *requestedStore {
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
		requestWorker := newRegionRequestWorker(
			ctx,
			eg,
			rs,
			perWorkerQueueSize,
			scheduler.upstream,
			scheduler.eventSink,
			scheduler.failureHandler,
		)
		rs.requestWorkers = append(rs.requestWorkers, requestWorker)
	}
	return rs
}

func (rs *requestedStore) Close() {
	for _, worker := range rs.requestWorkers {
		worker.ReleaseAdmittedRegionRequests()
	}
}

func (rs *requestedStore) AddRegion(
	ctx context.Context, region regionInfo, force bool, quota *regionRequestQuota,
) (bool, *regionRequestWorker, error) {
	workers := rs.requestWorkers

	start := int(rs.nextWorker.Add(1)) % len(workers)
	for i := range len(workers) {
		worker := workers[(start+i)%len(workers)]
		ok, err := worker.Add(ctx, region, force, quota)
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
