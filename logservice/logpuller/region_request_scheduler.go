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
	"github.com/pingcap/ticdc/utils/priorityqueue"
	kvclientv2 "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// regionRequestScheduler routes locked Region requests through the global
// priority queue to a worker connected to the Region's TiKV store. Range
// resolution and retry policy remain owned by subscriptionClient and
// regionFailureHandler respectively.
type regionRequestScheduler struct {
	upstream       *upstreamHandle
	eventSink      *regionEventSink
	failureHandler *regionFailureHandler

	// taskQueue orders all Regions before they are assigned to a TiKV store.
	taskQueue *priorityqueue.PriorityQueue[*regionPriorityTask]
	// sequence is the FIFO tie-breaker for Regions in the same priority class.
	sequence atomic.Uint64
	// stores maps TiKV addresses to requestedStore. Stores are created only by
	// run, but are also read by metrics and deregistration goroutines.
	stores sync.Map

	// workerCount is the configured number of request workers per store.
	workerCount int
	// workerWindow is each worker's share of the configured store window.
	workerWindow int
	// maxWindowMultiplier is passed to each worker's admission controller.
	maxWindowMultiplier int
}

func newRegionRequestScheduler(
	clientConfig *SubscriptionClientConfig,
	upstream *upstreamHandle,
	eventSink *regionEventSink,
	failureHandler *regionFailureHandler,
) *regionRequestScheduler {
	pullerConfig := config.GetGlobalServerConfig().Debug.Puller
	workerCount := int(clientConfig.RegionRequestWorkerPerStore)
	if workerCount <= 0 {
		workerCount = 1
	}
	workerWindow := (pullerConfig.PendingRegionRequestQueueSize + workerCount - 1) / workerCount
	return &regionRequestScheduler{
		upstream:            upstream,
		eventSink:           eventSink,
		failureHandler:      failureHandler,
		taskQueue:           priorityqueue.New[*regionPriorityTask](),
		workerCount:         workerCount,
		workerWindow:        workerWindow,
		maxWindowMultiplier: pullerConfig.RegionRequestMaxWindowMultiplier,
	}
}

func (s *regionRequestScheduler) submit(region regionInfo) {
	s.taskQueue.Push(NewRegionPriorityTask(
		region, s.upstream.pdClock.CurrentTS(), s.sequence.Add(1)))
}

func (s *regionRequestScheduler) run(ctx context.Context, group *errgroup.Group) error {
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

		region, ok := s.attachRPCContext(ctx, task.GetRegionInfo())
		if !ok {
			continue
		}

		store := s.getOrCreateStore(ctx, group, region.rpcCtx.Addr)
		task.updateRegion(region, s.upstream.pdClock.CurrentTS())
		if !store.submit(task) {
			return context.Canceled
		}

		log.Debug("subscription client will request a region",
			zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)),
			zap.Uint64("regionID", region.verID.GetID()),
			zap.String("addr", store.storeAddr))
	}
}

func (s *regionRequestScheduler) attachRPCContext(
	ctx context.Context,
	region regionInfo,
) (regionInfo, bool) {
	bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
	rpcCtx, err := s.upstream.regionCache.GetTiKVRPCContext(
		bo, region.verID, kvclientv2.ReplicaReadLeader, 0)
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

func (s *regionRequestScheduler) getOrCreateStore(
	ctx context.Context,
	group *errgroup.Group,
	storeAddr string,
) *requestedStore {
	if value, ok := s.stores.Load(storeAddr); ok {
		return value.(*requestedStore)
	}

	store := newRequestedStore(
		s.upstream,
		s.eventSink,
		s.failureHandler,
		storeAddr,
		s.workerCount,
		s.workerWindow,
		s.maxWindowMultiplier,
	)
	// run is the only writer. Publish the store after its immutable worker list
	// is complete, then start its workers.
	s.stores.Store(storeAddr, store)
	store.run(ctx, group)
	return store
}

func (s *regionRequestScheduler) broadcastDeregister(
	subID SubscriptionID,
	filterLoop bool,
) {
	s.stores.Range(func(_, value any) bool {
		value.(*requestedStore).broadcastDeregister(subID, filterLoop)
		return true
	})
}

func (s *regionRequestScheduler) inflightCount() int {
	count := 0
	s.stores.Range(func(_, value any) bool {
		count += value.(*requestedStore).inflightCount()
		return true
	})
	return count
}

func (s *regionRequestScheduler) close() {
	s.taskQueue.Close()
}

func (s *regionRequestScheduler) closeStores() {
	s.stores.Range(func(_, value any) bool {
		value.(*requestedStore).close()
		return true
	})
}
