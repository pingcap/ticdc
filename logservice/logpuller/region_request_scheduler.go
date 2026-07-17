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
	"sync"
	"sync/atomic"

	"github.com/pingcap/log"
<<<<<<< HEAD
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
=======
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
>>>>>>> 23171df8f (logpuller: extract region request scheduler from subscription client)
	"github.com/pingcap/ticdc/utils/priorityqueue"
	kvclientv2 "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
<<<<<<< HEAD
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
)

const regionRequestWorkerPerStore = 8

// regionRequestScheduler routes locked region requests through the global
// priority queue to a worker connected to the region's TiKV store. Range
// resolution and retry policy remain owned by subscriptionClient and
// regionFailureHandler respectively.
type regionRequestScheduler struct {
	upstream       *upstreamHandle
	eventSink      *regionEventSink
	failureHandler *regionFailureHandler

	// taskQueue orders all regions before they are assigned to a TiKV store.
	taskQueue *priorityqueue.PriorityQueue[*regionPriorityTask]
	// sequence is the FIFO tie-breaker for regions in the same priority class.
	sequence atomic.Uint64
	// stores maps TiKV addresses to regionRequestStore. Stores are created only
	// by Run, but are also read by metrics and deregistration goroutines.
=======
	"golang.org/x/sync/errgroup"
)

// regionRequestScheduler routes locked Region requests through the global
// priority queue to a worker connected to the Region's TiKV store. Range
// resolution and retry policy remain owned by subscriptionClient and
// regionFailureHandler respectively.
type regionRequestScheduler struct {
	client *subscriptionClient

	// taskQueue orders all Regions before they are assigned to a TiKV store.
	taskQueue *priorityqueue.PriorityQueue[*regionPriorityTask]
	// sequence is the FIFO tie-breaker for Regions in the same priority class.
	sequence atomic.Uint64
	// stores maps TiKV addresses to requestedStore. Stores are created only by
	// run, but are also read by metrics and deregistration goroutines.
>>>>>>> 23171df8f (logpuller: extract region request scheduler from subscription client)
	stores sync.Map

	// workerCount is the configured number of request workers per store.
	workerCount int
	// workerWindow is each worker's share of the configured store window.
	workerWindow int
	// maxWindowMultiplier is passed to each worker's admission controller.
	maxWindowMultiplier int
}

<<<<<<< HEAD
func newRegionRequestScheduler(
	upstream *upstreamHandle,
	eventSink *regionEventSink,
	failureHandler *regionFailureHandler,
) *regionRequestScheduler {
	pullerConfig := config.GetGlobalServerConfig().Debug.Puller
	workerCount := regionRequestWorkerPerStore
	workerWindow := (pullerConfig.PendingRegionRequestQueueSize + workerCount - 1) / workerCount
	return &regionRequestScheduler{
		upstream:            upstream,
		eventSink:           eventSink,
		failureHandler:      failureHandler,
=======
func newRegionRequestScheduler(client *subscriptionClient) *regionRequestScheduler {
	pullerConfig := config.GetGlobalServerConfig().Debug.Puller
	workerCount := int(client.config.RegionRequestWorkerPerStore)
	if workerCount <= 0 {
		workerCount = 1
	}
	workerWindow := (pullerConfig.PendingRegionRequestQueueSize + workerCount - 1) / workerCount
	return &regionRequestScheduler{
		client:              client,
>>>>>>> 23171df8f (logpuller: extract region request scheduler from subscription client)
		taskQueue:           priorityqueue.New[*regionPriorityTask](),
		workerCount:         workerCount,
		workerWindow:        workerWindow,
		maxWindowMultiplier: pullerConfig.RegionRequestMaxWindowMultiplier,
	}
}

<<<<<<< HEAD
func (s *regionRequestScheduler) Submit(region regionInfo) {
<<<<<<< HEAD
	if log.GetLevel() <= zapcore.DebugLevel {
		log.Debug("cdc region scan task enqueued",
			zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)),
			zap.Int64("tableID", region.subscribedSpan.span.TableID),
			zap.Uint64("startTs", region.subscribedSpan.startTs),
			zap.Uint64("regionID", region.verID.GetID()),
			zap.Uint64("regionEpochVersion", region.verID.GetVer()),
			zap.Uint64("regionEpochConfVer", region.verID.GetConfVer()),
			zap.String("priority", normalizeScanPriority(region.scanPriority).String()),
			zap.String("scanPriority", region.scanPriority.String()),
			zap.String("span", common.FormatTableSpan(&region.span)))
	}
	s.taskQueue.Push(newRegionPriorityTask(region, s.sequence.Add(1)))
=======
	s.taskQueue.Push(newRegionPriorityTask(
		region, s.upstream.pdClock.CurrentTS(), s.sequence.Add(1)))
>>>>>>> 9903a1be7 (refactor)
}

func (s *regionRequestScheduler) Run(ctx context.Context, workerGroup *errgroup.Group) error {
	defer func() {
		s.stores.Range(func(_, value any) bool {
			value.(*regionRequestStore).close()
			return true
		})
	}()

=======
func (s *regionRequestScheduler) submit(region regionInfo) {
	s.taskQueue.Push(NewRegionPriorityTask(
		region, s.client.pdClock.CurrentTS(), s.sequence.Add(1)))
}

func (s *regionRequestScheduler) run(ctx context.Context, group *errgroup.Group) error {
	defer s.closeStores()
>>>>>>> 23171df8f (logpuller: extract region request scheduler from subscription client)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		task, err := s.taskQueue.Pop(ctx)
		if err != nil {
			if errors.Is(err, priorityqueue.ErrClosed) {
				return nil
			}
			return err
		}

<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9903a1be7 (refactor)
		region, err := s.attachRPCContext(ctx, task.regionInfo)
		if err != nil {
			s.failureHandler.Report(newRegionErrorInfo(region, err))
			continue
		}
		if region.subscribedSpan.stopped.Load() {
			s.failureHandler.Report(newRegionErrorInfo(region, &requestCancelledErr{}))
			continue
		}

		store := s.getOrCreateStore(ctx, workerGroup, region.rpcCtx.Addr)
		task.regionInfo = region
		if !store.submit(task) {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			s.failureHandler.Report(newRegionErrorInfo(region, &storeStreamErr{}))
			continue
		}
		if log.GetLevel() <= zapcore.DebugLevel {
			log.Debug("subscription client will request a region",
				zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)),
				zap.Uint64("regionID", region.verID.GetID()),
				zap.String("addr", region.rpcCtx.Addr))
		}
=======
		region, ok := s.attachRPCContext(ctx, task.GetRegionInfo())
		if !ok {
			continue
		}

		store := s.getOrCreateStore(ctx, group, region.rpcCtx.Addr)
		task.updateRegion(region, s.client.pdClock.CurrentTS())
		if !store.submit(task) {
			return context.Canceled
		}

		log.Debug("subscription client will request a region",
			zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)),
			zap.Uint64("regionID", region.verID.GetID()),
			zap.String("addr", store.storeAddr))
>>>>>>> 23171df8f (logpuller: extract region request scheduler from subscription client)
	}
}

func (s *regionRequestScheduler) attachRPCContext(
	ctx context.Context,
	region regionInfo,
<<<<<<< HEAD
) (regionInfo, error) {
	bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
	rpcCtx, err := s.upstream.regionCache.GetTiKVRPCContext(
		bo, region.verID, kvclientv2.ReplicaReadLeader, 0)
	if rpcCtx != nil {
		region.rpcCtx = rpcCtx
		return region, nil
	}
	if err != nil {
		log.Debug("region request scheduler failed to get RPC context",
=======
) (regionInfo, bool) {
	bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
	rpcCtx, err := s.client.regionCache.GetTiKVRPCContext(
		bo, region.verID, kvclientv2.ReplicaReadLeader, 0)
	if rpcCtx != nil {
		region.rpcCtx = rpcCtx
		return region, true
	}
	if err != nil {
		log.Debug("subscription client get rpc context fail",
>>>>>>> 23171df8f (logpuller: extract region request scheduler from subscription client)
			zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)),
			zap.Uint64("regionID", region.verID.GetID()),
			zap.Error(err))
	}
<<<<<<< HEAD
	return region, &rpcCtxUnavailableErr{verID: region.verID}
=======
	s.client.onRegionFail(newRegionErrorInfo(region, &rpcCtxUnavailableErr{verID: region.verID}))
	return region, false
>>>>>>> 23171df8f (logpuller: extract region request scheduler from subscription client)
}

func (s *regionRequestScheduler) getOrCreateStore(
	ctx context.Context,
<<<<<<< HEAD
	workerGroup *errgroup.Group,
	storeAddr string,
) *regionRequestStore {
	if value, ok := s.stores.Load(storeAddr); ok {
		return value.(*regionRequestStore)
	}

	store := newRegionRequestStore(
		s.upstream,
		s.eventSink,
		s.failureHandler,
		storeAddr,
		s.workerCount,
		s.workerWindow,
		s.maxWindowMultiplier,
	)
	// The scheduler run loop is the only writer. Publish the store after its
	// immutable worker list is complete, then start its workers.
	s.stores.Store(storeAddr, store)
	store.startWorkers(ctx, workerGroup)
	return store
}

func (s *regionRequestScheduler) BroadcastDeregister(
=======
	group *errgroup.Group,
	storeAddr string,
) *requestedStore {
	if value, ok := s.stores.Load(storeAddr); ok {
		return value.(*requestedStore)
	}

	store := newRequestedStore(
		s.client, storeAddr, s.workerCount, s.workerWindow, s.maxWindowMultiplier)
	// run is the only writer. Publish the store after its immutable worker list
	// is complete, then start its workers.
	s.stores.Store(storeAddr, store)
	store.run(ctx, group)
	return store
}

func (s *regionRequestScheduler) broadcastDeregister(
>>>>>>> 23171df8f (logpuller: extract region request scheduler from subscription client)
	subID SubscriptionID,
	filterLoop bool,
) {
	s.stores.Range(func(_, value any) bool {
<<<<<<< HEAD
		value.(*regionRequestStore).broadcastDeregister(subID, filterLoop)
=======
		value.(*requestedStore).broadcastDeregister(subID, filterLoop)
>>>>>>> 23171df8f (logpuller: extract region request scheduler from subscription client)
		return true
	})
}

func (s *regionRequestScheduler) requestedRegionCount() int {
	count := 0
	s.stores.Range(func(_, value any) bool {
	<<<<<<< HEAD
		count += value.(*regionRequestStore).inflightCount()
	=======
		count += value.(*requestedStore).inflightCount()
	>>>>>>> 23171df8f (logpuller: extract region request scheduler from subscription client)
		return true
	})
	return count
}

<<<<<<< HEAD
func (s *regionRequestScheduler) UpdateMetrics() {
	metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("pending").
		Set(float64(s.requestedRegionCount()))
}

func (s *regionRequestScheduler) Close() {
	s.taskQueue.Close()
}
=======
func (s *regionRequestScheduler) close() {
	s.taskQueue.Close()
}

func (s *regionRequestScheduler) closeStores() {
	s.stores.Range(func(_, value any) bool {
		value.(*requestedStore).close()
		return true
	})
}
>>>>>>> 23171df8f (logpuller: extract region request scheduler from subscription client)
