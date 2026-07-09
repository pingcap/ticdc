// Copyright 2023 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/logservice/txnutil"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/spanz"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/utils/priorityqueue"
	"github.com/prometheus/client_golang/prometheus"
	kvclientv2 "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	// Maximum total sleep time(in ms), 20 seconds.
	tikvRequestMaxBackoff = 20000

	// TiCDC always interacts with region leader, every time something goes wrong,
	// failed region will be reloaded via `BatchLoadRegionsWithKeyRange` API. So we
	// don't need to force reload region anymore.
	regionScheduleReload = false

	loadRegionRetryInterval time.Duration = 100 * time.Millisecond
	resolveLockMinInterval  time.Duration = 10 * time.Second
	resolveLockTickInterval time.Duration = 2 * time.Second
	resolveLockFence        time.Duration = 4 * time.Second
)

var (
	metricResolveLockSuccessCounter = metrics.SubscriptionClientResolveLockCounter.WithLabelValues("success")
	metricResolveLockFailureCounter = metrics.SubscriptionClientResolveLockCounter.WithLabelValues("failure")

	metricSubscriptionClientDSChannelSize     = metrics.DynamicStreamEventChanSize.WithLabelValues("event-store")
	metricSubscriptionClientDSPendingQueueLen = metrics.DynamicStreamPendingQueueLen.WithLabelValues("event-store")
)

// To generate an ID for a new subscription.
var subscriptionIDGen atomic.Uint64

// subscriptionID is a unique identifier for a subscription.
// It is used as `RequestId` in region requests to remote store.
type SubscriptionID uint64

const InvalidSubscriptionID SubscriptionID = 0

type resolveLockTask struct {
	keyspaceID uint32
	regionID   uint64
	targetTs   uint64
	state      *regionlock.LockedRangeState
}

// rangeTask represents a task to subscribe a range span of a table.
// It can be a part of a table or a whole table, it also can be a part of a region.
type rangeTask struct {
	span           heartbeatpb.TableSpan
	subscribedSpan *subscribedSpan
	filterLoop     bool
	priority       TaskType
}

type SubscriptionClientConfig struct {
	// The number of region request workers to send region task for every tikv store
	RegionRequestWorkerPerStore uint
}

type sharedClientMetrics struct {
	batchResolvedSize prometheus.Observer
}

// subscriptionClient is used to subscribe events of table ranges from TiKV.
// All exported Methods are thread-safe.
type SubscriptionClient interface {
	common.SubModule
	// allocate a unique id for the subscription
	AllocSubscriptionID() SubscriptionID
	// subscribe a table span
	Subscribe(
		subID SubscriptionID,
		span heartbeatpb.TableSpan,
		startTs uint64,
		consumeKVEvents func(raw []common.RawKVEntry, wakeCallback func()) bool,
		advanceResolvedTs func(ts uint64),
		advanceInterval int64,
		bdrMode bool,
	)
	// unsubscribe a table span
	Unsubscribe(subID SubscriptionID)
}

type subscriptionClient struct {
	ctx       context.Context
	cancel    context.CancelFunc
	config    *SubscriptionClientConfig
	metrics   sharedClientMetrics
	clusterID uint64

	pd           pd.Client
	regionCache  *tikv.RegionCache
	pdClock      pdutil.Clock
	lockResolver txnutil.LockResolver

	stores sync.Map

	// the credential to connect tikv
	credential *security.Credential

	eventSink      *regionEventSink
	failureHandler *regionFailureHandler
	spanRegistry   *spanRegistry

	// rangeTaskCh is used to receive range tasks.
	// The tasks will be handled in `handleRangeTask` goroutine.
	rangeTaskCh chan rangeTask
	// regionTaskQueue is used to receive region tasks with priority.
	// The region will be handled in `handleRegions` goroutine.
	regionTaskQueue *priorityqueue.PriorityQueue[PriorityTask]
	// resolveLockTaskCh is used to receive resolve lock tasks.
	// The tasks will be handled in `handleResolveLockTasks` goroutine.
	resolveLockTaskCh      chan resolveLockTask
	resolveLockRateLimiter *resolveLockRateLimiter
}

// NewSubscriptionClient creates a client.
func NewSubscriptionClient(
	config *SubscriptionClientConfig,
	pd pd.Client,
	lockResolver txnutil.LockResolver,
	credential *security.Credential,
) SubscriptionClient {
	subClient := &subscriptionClient{
		config: config,

		stores:       sync.Map{},
		pd:           pd,
		regionCache:  appcontext.GetService[*tikv.RegionCache](appcontext.RegionCache),
		pdClock:      appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock),
		lockResolver: lockResolver,

		credential: credential,

		rangeTaskCh:            make(chan rangeTask, 1024),
		regionTaskQueue:        priorityqueue.New[PriorityTask](),
		resolveLockTaskCh:      make(chan resolveLockTask, 1024),
		resolveLockRateLimiter: newResolveLockRateLimiter(),
	}
	subClient.ctx, subClient.cancel = context.WithCancel(context.Background())
	subClient.eventSink = newRegionEventSink(subClient.ctx, subClient)
	subClient.failureHandler = newRegionFailureHandler(subClient)
	subClient.spanRegistry = newSpanRegistry(subClient.pd, subClient.pdClock)

	subClient.initMetrics()
	return subClient
}

func (s *subscriptionClient) Name() string {
	return appcontext.SubscriptionClient
}

// AllocsubscriptionID gets an ID can be used in `Subscribe`.
func (s *subscriptionClient) AllocSubscriptionID() SubscriptionID {
	return SubscriptionID(subscriptionIDGen.Add(1))
}

func (s *subscriptionClient) initMetrics() {
	// TODO: fix metrics
	s.metrics.batchResolvedSize = metrics.BatchResolvedEventSize.WithLabelValues("event-store")
}

func (s *subscriptionClient) updateMetrics(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			pendingRegionReqCount := 0
			s.stores.Range(func(_, value any) bool {
				store := value.(*requestedStore)
				store.requestWorkers.RLock()
				for _, worker := range store.requestWorkers.s {
					worker.requestCache.clearStaleRequest()
					pendingRegionReqCount += worker.requestCache.getPendingCount()
				}
				store.requestWorkers.RUnlock()
				return true
			})

			metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("pending").Set(float64(pendingRegionReqCount))
			s.eventSink.UpdateMetrics()
			s.spanRegistry.UpdateMetrics()
		}
	}
}

// Subscribe the given table span.
// NOTE: `span.TableID` must be set correctly.
// It new a subscribedSpan and store it in `s.spanRegistry`,
// and send a rangeTask to `s.rangeTaskCh`.
// The rangeTask will be handled in `handleRangeTasks` goroutine.
func (s *subscriptionClient) Subscribe(
	subID SubscriptionID,
	span heartbeatpb.TableSpan,
	startTs uint64,
	consumeKVEvents func(raw []common.RawKVEntry, wakeCallback func()) bool,
	advanceResolvedTs func(ts uint64),
	advanceInterval int64,
	bdrMode bool,
) {
	if span.TableID == 0 {
		log.Panic("subscription client subscribe with zero TableID")
		return
	}

	rt := s.newSubscribedSpan(subID, span, startTs, consumeKVEvents, advanceResolvedTs, advanceInterval, bdrMode)
	s.spanRegistry.Add(rt)
	s.eventSink.AddPath(rt)

	select {
	case <-s.ctx.Done():
		log.Warn("subscribes span failed, the subscription client has closed")
	case s.rangeTaskCh <- rangeTask{span: span, subscribedSpan: rt, filterLoop: rt.filterLoop, priority: TaskLowPrior}:
		log.Info("subscribes span done", zap.Uint64("subscriptionID", uint64(subID)),
			zap.Int64("tableID", span.TableID), zap.Uint64("startTs", startTs),
			zap.String("startKey", spanz.HexKey(span.StartKey)), zap.String("endKey", spanz.HexKey(span.EndKey)))
	}
}

// Unsubscribe the given table span. All covered regions will be deregistered asynchronously.
// NOTE: `span.TableID` must be set correctly.
func (s *subscriptionClient) Unsubscribe(subID SubscriptionID) {
	// NOTE: `subID` is cleared from `s.spanRegistry` in `onTableDrained`.
	rt := s.spanRegistry.Get(subID)
	if rt == nil {
		log.Warn("unknown subscription", zap.Uint64("subscriptionID", uint64(subID)))
		return
	}
	s.setTableStopped(rt)

	log.Info("unsubscribe span success",
		zap.Uint64("subscriptionID", uint64(rt.subID)),
		zap.Bool("exists", rt != nil))
}

func (s *subscriptionClient) wakeSubscription(subID SubscriptionID) {
	s.eventSink.Wake(subID)
}

func (s *subscriptionClient) pushRegionEventToDS(subID SubscriptionID, event regionEvent) {
	s.eventSink.Push(subID, event)
}

func (s *subscriptionClient) Run(ctx context.Context) error {
	// s.consume = consume
	if s.pd == nil {
		log.Warn("subscription client should be in test mode, skip run")
		return nil
	}
	s.clusterID = s.pd.GetClusterID(ctx)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error { return s.updateMetrics(ctx) })
	g.Go(func() error { return s.eventSink.Run(ctx) })
	g.Go(func() error { return s.handleRangeTasks(ctx) })
	g.Go(func() error { return s.handleRegions(ctx, g) })
	g.Go(func() error { return s.failureHandler.Run(ctx) })
	g.Go(func() error { return s.handleResolveLockTasks(ctx) })
	g.Go(func() error { return s.spanRegistry.Run(ctx) })

	log.Info("subscription client starts")
	defer log.Info("subscription client exits")
	return g.Wait()
}

// Close closes the client. Must be called after `Run` returns.
func (s *subscriptionClient) Close(ctx context.Context) error {
	s.cancel()
	s.eventSink.Close()
	s.regionTaskQueue.Close()
	return nil
}

func (s *subscriptionClient) setTableStopped(rt *subscribedSpan) {
	log.Info("subscription client starts to stop table",
		zap.Uint64("subscriptionID", uint64(rt.subID)))

	// Set stopped to true so we can stop handling region events from the table.
	// Then send a special singleRegionInfo to regionRouter to deregister the table
	// from all TiKV instances.
	if rt.stopped.CompareAndSwap(false, true) {
		s.regionTaskQueue.Push(NewRegionPriorityTask(TaskHighPrior, regionInfo{subscribedSpan: rt, filterLoop: rt.filterLoop}, s.pdClock.CurrentTS()))
		if rt.rangeLock.Stop() {
			s.onTableDrained(rt)
		}
	}
}

func (s *subscriptionClient) onTableDrained(rt *subscribedSpan) {
	log.Info("subscription client stop span is finished",
		zap.Uint64("subscriptionID", uint64(rt.subID)))

	err := s.eventSink.RemovePath(rt.subID)
	if err != nil {
		log.Warn("subscription client remove path failed",
			zap.Uint64("subscriptionID", uint64(rt.subID)),
			zap.Error(err))
	}
	s.spanRegistry.Remove(rt.subID)
}

// Note: don't block the caller, otherwise there may be deadlock
func (s *subscriptionClient) onRegionFail(errInfo regionErrorInfo) {
	s.failureHandler.Report(errInfo)
}

// requestedStore represents a store that has been connected.
type requestedStore struct {
	storeAddr string
	// Use to select a worker to send request.
	nextWorker atomic.Uint32

	requestWorkers struct {
		sync.RWMutex
		s []*regionRequestWorker
	}
}

func (rs *requestedStore) getRequestWorker() *regionRequestWorker {
	rs.requestWorkers.RLock()
	defer rs.requestWorkers.RUnlock()

	index := rs.nextWorker.Add(1) % uint32(len(rs.requestWorkers.s))
	return rs.requestWorkers.s[index]
}

// handleRegions receives regionInfo from regionTaskQueue and attach rpcCtx to them,
// then send them to corresponding requestedStore.
func (s *subscriptionClient) handleRegions(ctx context.Context, eg *errgroup.Group) error {
	cfg := config.GetGlobalServerConfig()
	pendingRegionRequestQueueSize := cfg.Debug.Puller.PendingRegionRequestQueueSize
	getStore := func(storeAddr string) *requestedStore {
		var rs *requestedStore
		if v, ok := s.stores.Load(storeAddr); ok {
			rs = v.(*requestedStore)
			return rs
		}

		rs = &requestedStore{storeAddr: storeAddr}
		rs.requestWorkers.s = make([]*regionRequestWorker, 0, s.config.RegionRequestWorkerPerStore)
		s.stores.Store(storeAddr, rs)

		perWorkerQueueSize := pendingRegionRequestQueueSize / int(s.config.RegionRequestWorkerPerStore)
		if perWorkerQueueSize <= 0 {
			log.Warn("pending region request queue size is smaller than the number of workers, adjust per worker queue size to 1",
				zap.Int("pendingRegionRequestQueueSize", pendingRegionRequestQueueSize),
				zap.Uint("regionRequestWorkerPerStore", s.config.RegionRequestWorkerPerStore))
			perWorkerQueueSize = 1
		}

		rs.requestWorkers.Lock()
		for i := uint(0); i < s.config.RegionRequestWorkerPerStore; i++ {
			requestWorker := newRegionRequestWorker(ctx, s, s.credential, eg, rs, perWorkerQueueSize)
			rs.requestWorkers.s = append(rs.requestWorkers.s, requestWorker)
		}
		rs.requestWorkers.Unlock()
		return rs
	}

	defer func() {
		s.stores.Range(func(_, value any) bool {
			rs := value.(*requestedStore)

			rs.requestWorkers.RLock()
			for _, w := range rs.requestWorkers.s {
				w.requestCache.clear()
			}
			rs.requestWorkers.RUnlock()

			return true
		})
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		// Use blocking Pop to wait for tasks
		regionTask, err := s.regionTaskQueue.Pop(ctx)
		if err != nil {
			if errors.Is(err, priorityqueue.ErrClosed) {
				return nil
			}
			return err
		}

		region := regionTask.GetRegionInfo()
		if region.isStopped() {
			enqueued, err := s.enqueueRegionToAllStores(ctx, region)
			if err != nil {
				return err
			}
			if !enqueued {
				log.Debug("enqueue stop request failed, retry later",
					zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)))
				s.regionTaskQueue.Push(regionTask)
			}
			continue
		}

		region, ok := s.attachRPCContextForRegion(ctx, region)
		// If attachRPCContextForRegion fails, the region will be re-scheduled.
		if !ok {
			continue
		}

		store := getStore(region.rpcCtx.Addr)
		worker := store.getRequestWorker()
		force := regionTask.Priority() <= forcedPriorityBase

		ok, err = worker.add(ctx, region, force)
		if err != nil {
			log.Warn("subscription client add region request failed",
				zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)),
				zap.Uint64("regionID", region.verID.GetID()),
				zap.Error(err))
			return err
		}

		if !ok {
			s.regionTaskQueue.Push(regionTask)
			continue
		}

		log.Debug("subscription client will request a region",
			zap.Uint64("workID", worker.workerID),
			zap.Uint64("subscriptionID", uint64(region.subscribedSpan.subID)),
			zap.Uint64("regionID", region.verID.GetID()),
			zap.String("addr", store.storeAddr))
	}
}

func (s *subscriptionClient) enqueueRegionToAllStores(ctx context.Context, region regionInfo) (bool, error) {
	enqueued := true
	var firstErr error
	s.stores.Range(func(_ any, value any) bool {
		rs := value.(*requestedStore)
		rs.requestWorkers.RLock()
		workers := rs.requestWorkers.s
		rs.requestWorkers.RUnlock()
		for _, worker := range workers {
			ok, err := worker.add(ctx, region, true)
			if err != nil {
				firstErr = err
				enqueued = false
				return false
			}
			if !ok {
				enqueued = false
				// It is likely the store is busy, no need to try other workers in this store now.
				break
			}
		}
		return true
	})
	return enqueued, firstErr
}

func (s *subscriptionClient) attachRPCContextForRegion(ctx context.Context, region regionInfo) (regionInfo, bool) {
	bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
	rpcCtx, err := s.regionCache.GetTiKVRPCContext(bo, region.verID, kvclientv2.ReplicaReadLeader, 0)
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
	s.onRegionFail(newRegionErrorInfo(region, &rpcCtxUnavailableErr{verID: region.verID}))
	return region, false
}

func (s *subscriptionClient) handleRangeTasks(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	// Limit the concurrent number of goroutines to convert range tasks to region tasks.
	g.SetLimit(1024)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task := <-s.rangeTaskCh:
			g.Go(func() error {
				return s.divideSpanAndScheduleRegionRequests(ctx, task.span, task.subscribedSpan, task.filterLoop, task.priority)
			})
		}
	}
}

// divideSpanAndScheduleRegionRequests processes the specified span by dividing it into
// manageable regions and schedules requests to subscribe to these regions.
// 1. Load regions from PD.
// 2. Find the intersection of each region.span and the subscribedSpan.span.
// 3. Schedule a region request to subscribe the region.
func (s *subscriptionClient) divideSpanAndScheduleRegionRequests(
	ctx context.Context,
	span heartbeatpb.TableSpan,
	subscribedSpan *subscribedSpan,
	filterLoop bool,
	taskType TaskType,
) error {
	// Limit the number of regions loaded at a time to make the load more stable.
	limit := 1024
	nextSpan := span
	backoffBeforeLoad := false
	for {
		if backoffBeforeLoad {
			if err := util.Hang(ctx, loadRegionRetryInterval); err != nil {
				return err
			}
			backoffBeforeLoad = false
		}
		log.Debug("subscription client is going to load regions",
			zap.Uint64("subscriptionID", uint64(subscribedSpan.subID)),
			zap.Any("span", common.FormatTableSpan(&nextSpan)))

		backoff := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
		regions, err := s.regionCache.BatchLoadRegionsWithKeyRange(backoff, nextSpan.StartKey, nextSpan.EndKey, limit)
		if err != nil {
			log.Warn("subscription client load regions failed",
				zap.Uint64("subscriptionID", uint64(subscribedSpan.subID)),
				zap.Any("span", common.FormatTableSpan(&nextSpan)),
				zap.Error(err))
			backoffBeforeLoad = true
			continue
		}
		regionMetas := make([]*metapb.Region, 0, len(regions))
		for _, region := range regions {
			if meta := region.GetMeta(); meta != nil {
				regionMetas = append(regionMetas, meta)
			}
		}
		regionMetas = regionlock.CutRegionsLeftCoverSpan(regionMetas, nextSpan)
		if len(regionMetas) == 0 {
			log.Warn("subscription client load regions with holes",
				zap.Uint64("subscriptionID", uint64(subscribedSpan.subID)),
				zap.Any("span", common.FormatTableSpan(&nextSpan)))
			backoffBeforeLoad = true
			continue
		}

		for _, regionMeta := range regionMetas {
			regionSpan := heartbeatpb.TableSpan{
				StartKey:   regionMeta.StartKey,
				EndKey:     regionMeta.EndKey,
				KeyspaceID: subscribedSpan.span.KeyspaceID,
			}
			// NOTE: the End key return by the PD API will be nil to represent the biggest key.
			// So we need to fix it by calling spanz.HackSpan.
			regionSpan = common.HackTableSpan(regionSpan)

			// Find the intersection of the regionSpan returned by PD and the subscribedSpan.span.
			// The intersection is the span that needs to be subscribed.
			intersectSpan := common.GetIntersectSpan(subscribedSpan.span, regionSpan)
			if common.IsEmptySpan(intersectSpan) {
				log.Panic("subscription client check spans intersect shouldn't fail",
					zap.Uint64("subscriptionID", uint64(subscribedSpan.subID)))
			}

			verID := tikv.NewRegionVerID(regionMeta.Id, regionMeta.RegionEpoch.ConfVer, regionMeta.RegionEpoch.Version)
			regionInfo := newRegionInfo(verID, intersectSpan, nil, subscribedSpan, filterLoop)

			// Schedule a region request to subscribe the region.
			s.scheduleRegionRequest(ctx, regionInfo, taskType)

			nextSpan.StartKey = regionMeta.EndKey
			// If the nextSpan.StartKey is larger than the subscribedSpan.span.EndKey,
			// it means all span of the subscribedSpan have been requested. So we return.
			if common.EndCompare(nextSpan.StartKey, span.EndKey) >= 0 {
				return nil
			}
		}
	}
}

// scheduleRegionRequest locks the region's range and send the region to regionTaskQueue,
// which will be handled by handleRegions.
func (s *subscriptionClient) scheduleRegionRequest(ctx context.Context, region regionInfo, priority TaskType) {
	lockRangeResult := region.subscribedSpan.rangeLock.LockRange(
		ctx, region.span.StartKey, region.span.EndKey, region.verID.GetID(), region.verID.GetVer())

	if lockRangeResult.Status == regionlock.LockRangeStatusWait {
		lockRangeResult = lockRangeResult.WaitFn()
	}

	switch lockRangeResult.Status {
	case regionlock.LockRangeStatusSuccess:
		region.lockedRangeState = lockRangeResult.LockedRangeState
		s.regionTaskQueue.Push(NewRegionPriorityTask(priority, region, s.pdClock.CurrentTS()))
	case regionlock.LockRangeStatusStale:
		for _, r := range lockRangeResult.RetryRanges {
			s.scheduleRangeRequest(ctx, r, region.subscribedSpan, region.filterLoop, priority)
		}
	default:
		return
	}
}

func (s *subscriptionClient) scheduleRangeRequest(
	ctx context.Context, span heartbeatpb.TableSpan,
	subscribedSpan *subscribedSpan,
	filterLoop bool,
	priority TaskType,
) {
	select {
	case <-ctx.Done():
	case s.rangeTaskCh <- rangeTask{span: span, subscribedSpan: subscribedSpan, filterLoop: filterLoop, priority: priority}:
	}
}

func (s *subscriptionClient) handleResolveLockTasks(ctx context.Context) error {
	doResolve := func(task resolveLockTask) {
		keyspaceID := task.keyspaceID
		regionID := task.regionID
		state := task.state
		targetTs := task.targetTs
		key := resolveLockKey{keyspaceID: keyspaceID, regionID: regionID}

		if !state.Initialized.Load() || state.ResolvedTs.Load() >= targetTs {
			s.resolveLockRateLimiter.cancel(key)
			return
		}

		err := s.lockResolver.Resolve(ctx, keyspaceID, regionID, targetTs)
		s.resolveLockRateLimiter.finish(key, time.Now())
		if err != nil {
			metricResolveLockFailureCounter.Inc()
			log.Warn("subscription client resolve lock fail",
				zap.Uint32("keyspaceID", keyspaceID),
				zap.Uint64("regionID", regionID),
				zap.Uint64("targetTs", targetTs),
				zap.Any("state", state),
				zap.Error(err))
		} else {
			metricResolveLockSuccessCounter.Inc()
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task := <-s.resolveLockTaskCh:
			doResolve(task)
		}
	}
}

func (s *subscriptionClient) newSubscribedSpan(
	subID SubscriptionID,
	span heartbeatpb.TableSpan,
	startTs uint64,
	consumeKVEvents func(raw []common.RawKVEntry, wakeCallback func()) bool,
	advanceResolvedTs func(ts uint64),
	advanceInterval int64,
	filterLoop bool,
) *subscribedSpan {
	return newSubscribedSpan(
		s.ctx,
		s.resolveLockRateLimiter,
		s.resolveLockTaskCh,
		subID,
		span,
		startTs,
		consumeKVEvents,
		advanceResolvedTs,
		advanceInterval,
		filterLoop,
	)
}

func (s *subscriptionClient) GetResolvedTsLag() float64 {
	if s.spanRegistry == nil {
		return 0
	}
	return s.spanRegistry.GetResolvedTsLag()
}
