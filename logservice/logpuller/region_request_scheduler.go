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
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/utils/priorityqueue"
	kvclientv2 "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
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
	memoryQuota    *memoryQuotaController

	// taskQueue orders all regions before they are assigned to a TiKV store.
	taskQueue *priorityqueue.PriorityQueue[*regionPriorityTask]
	// sequence is the FIFO tie-breaker for regions in the same priority class.
	sequence atomic.Uint64
	// stores maps TiKV addresses to regionRequestStore. Stores are created only
	// by Run, but are also read by metrics and deregistration goroutines.
	stores sync.Map

	// workerCount is the configured number of request workers per store.
	workerCount int
	// workerWindow is each worker's share of the configured store window.
	workerWindow int
	// maxWindowMultiplier is passed to each worker's admission controller.
	maxWindowMultiplier int
}

func newRegionRequestScheduler(
	upstream *upstreamHandle,
	eventSink *regionEventSink,
	failureHandler *regionFailureHandler,
	memoryQuota *memoryQuotaController,
) *regionRequestScheduler {
	pullerConfig := config.GetGlobalServerConfig().Debug.Puller
	workerCount := regionRequestWorkerPerStore
	workerWindow := (pullerConfig.PendingRegionRequestQueueSize + workerCount - 1) / workerCount
	return &regionRequestScheduler{
		upstream:            upstream,
		eventSink:           eventSink,
		failureHandler:      failureHandler,
		memoryQuota:         memoryQuota,
		taskQueue:           priorityqueue.New[*regionPriorityTask](),
		workerCount:         workerCount,
		workerWindow:        workerWindow,
		maxWindowMultiplier: pullerConfig.RegionRequestMaxWindowMultiplier,
	}
}

func (s *regionRequestScheduler) Submit(region regionInfo) {
	s.taskQueue.Push(newRegionPriorityTask(
		region, s.upstream.pdClock.CurrentTS(), s.sequence.Add(1)))
}

func (s *regionRequestScheduler) Run(ctx context.Context, workerGroup *errgroup.Group) error {
	defer s.closeStores()
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

		region, err := s.attachRPCContext(ctx, task.regionInfo)
		if err != nil {
			s.failureHandler.Report(newRegionErrorInfo(region, err))
			continue
		}

		store := s.getOrCreateStore(ctx, workerGroup, region.rpcCtx.Addr)
		task.updateRegion(region, s.upstream.pdClock.CurrentTS())
		if !store.submit(task) {
			return context.Canceled
		}
	}
}

func (s *regionRequestScheduler) attachRPCContext(
	ctx context.Context,
	region regionInfo,
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
			zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)),
			zap.Uint64("regionID", region.verID.GetID()),
			zap.Error(err))
	}
	return region, &rpcCtxUnavailableErr{verID: region.verID}
}

func (s *regionRequestScheduler) getOrCreateStore(
	ctx context.Context,
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
		s.memoryQuota,
	)
	// The scheduler run loop is the only writer. Publish the store after its
	// immutable worker list is complete, then start its workers.
	s.stores.Store(storeAddr, store)
	store.startWorkers(ctx, workerGroup)
	return store
}

func (s *regionRequestScheduler) BroadcastDeregister(
	subID SubscriptionID,
	filterLoop bool,
) {
	s.stores.Range(func(_, value any) bool {
		value.(*regionRequestStore).broadcastDeregister(subID, filterLoop)
		return true
	})
}

func (s *regionRequestScheduler) inflightCount() int {
	count := 0
	s.stores.Range(func(_, value any) bool {
		count += value.(*regionRequestStore).inflightCount()
		return true
	})
	return count
}

func (s *regionRequestScheduler) UpdateMetrics() {
	metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("inflight").
		Set(float64(s.inflightCount()))
}

func (s *regionRequestScheduler) Close() {
	s.taskQueue.Close()
}

func (s *regionRequestScheduler) closeStores() {
	s.stores.Range(func(_, value any) bool {
		value.(*regionRequestStore).close()
		return true
	})
}
