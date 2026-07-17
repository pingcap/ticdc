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
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/logservice/txnutil"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/spanz"
	"github.com/pingcap/ticdc/pkg/util"
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
	wasInitialized bool
}

// upstreamHandle contains the stable TiKV and PD dependencies shared by the
// region request pipeline.
type upstreamHandle struct {
	pd          pd.Client
	regionCache *tikv.RegionCache
	pdClock     pdutil.Clock
	credential  *security.Credential
	clusterID   uint64
}

// initialize loads the cluster metadata needed by Region request workers. It
// must run before the scheduler starts any workers.
func (u *upstreamHandle) initialize(ctx context.Context) {
	u.clusterID = u.pd.GetClusterID(ctx)
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
	ctx      context.Context
	cancel   context.CancelFunc
	upstream *upstreamHandle

	lockResolver txnutil.LockResolver

	// failureHandler handles failed regions and owns reschedule/retry decisions.
	failureHandler *regionFailureHandler
	// eventSink delivers region events and owns dynstream interaction.
	eventSink *regionEventSink
	// spanRegistry tracks subscribed spans and owns span-level background tasks.
	spanRegistry *spanRegistry
	// regionScheduler assigns locked Region requests to per-store workers.
	regionScheduler *regionRequestScheduler

	// rangeTaskCh is used to receive range tasks.
	// The tasks will be handled in `handleRangeTask` goroutine.
	rangeTaskCh chan rangeTask
	// resolveLockTaskCh is used to receive resolve lock tasks.
	// The tasks will be handled in `handleResolveLockTasks` goroutine.
	resolveLockTaskCh      chan resolveLockTask
	resolveLockRateLimiter *resolveLockRateLimiter
}

// NewSubscriptionClient creates a client.
func NewSubscriptionClient(
	pd pd.Client,
	lockResolver txnutil.LockResolver,
	credential *security.Credential,
) SubscriptionClient {
	subClient := &subscriptionClient{
		upstream: &upstreamHandle{
			pd:          pd,
			regionCache: appcontext.GetService[*tikv.RegionCache](appcontext.RegionCache),
			pdClock:     appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock),
			credential:  credential,
		},
		lockResolver: lockResolver,

		rangeTaskCh:            make(chan rangeTask, 1024),
		resolveLockTaskCh:      make(chan resolveLockTask, 1024),
		resolveLockRateLimiter: newResolveLockRateLimiter(),
	}
	subClient.ctx, subClient.cancel = context.WithCancel(context.Background())
	subClient.failureHandler = newRegionFailureHandler(
		subClient.upstream.regionCache,
		subClient.onTableDrained,
		subClient.scheduleRegionRequest,
		subClient.scheduleRangeRequest,
	)
	subClient.eventSink = newRegionEventSink(subClient.ctx, subClient.failureHandler)
	subClient.spanRegistry = newSpanRegistry(subClient.upstream.pd, subClient.upstream.pdClock)
	subClient.regionScheduler = newRegionRequestScheduler(
		subClient.upstream,
		subClient.eventSink,
		subClient.failureHandler,
	)
	return subClient
}

func (s *subscriptionClient) Name() string {
	return appcontext.SubscriptionClient
}

// AllocsubscriptionID gets an ID can be used in `Subscribe`.
func (s *subscriptionClient) AllocSubscriptionID() SubscriptionID {
	return SubscriptionID(subscriptionIDGen.Add(1))
}

func (s *subscriptionClient) updateMetrics(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			inflightRegionRequestCount := s.regionScheduler.inflightCount()

			metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("inflight").Set(float64(inflightRegionRequestCount))
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

	rt := newSubscribedSpan(
		s.ctx,
		s.resolveLockRateLimiter,
		s.resolveLockTaskCh,
		subID,
		span,
		startTs,
		consumeKVEvents,
		advanceResolvedTs,
		advanceInterval,
		bdrMode,
	)
	s.spanRegistry.Add(rt)
	s.eventSink.AddPath(rt)

	select {
	case <-s.ctx.Done():
		log.Warn("subscribes span failed, the subscription client has closed")
	case s.rangeTaskCh <- rangeTask{span: span, subscribedSpan: rt, filterLoop: rt.filterLoop}:
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

func (s *subscriptionClient) Run(ctx context.Context) error {
	// s.consume = consume
	if s.upstream == nil || s.upstream.pd == nil {
		log.Warn("subscription client should be in test mode, skip run")
		return nil
	}
	s.upstream.initialize(ctx)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error { return s.updateMetrics(ctx) })
	g.Go(func() error { return s.eventSink.Run(ctx) })
	g.Go(func() error { return s.handleRangeTasks(ctx) })
	g.Go(func() error { return s.regionScheduler.run(ctx, g) })
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
	s.regionScheduler.close()
	return nil
}

func (s *subscriptionClient) setTableStopped(rt *subscribedSpan) {
	log.Info("subscription client starts to stop table",
		zap.Uint64("subscriptionID", uint64(rt.subID)))

	// Set stopped to true so we can stop handling region events from the table,
	// then notify every existing worker to deregister the subscription.
	if rt.stopped.CompareAndSwap(false, true) {
		s.regionScheduler.broadcastDeregister(rt.subID, rt.filterLoop)
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
				return s.divideSpanAndScheduleRegionRequests(ctx, task.span, task.subscribedSpan, task.filterLoop, task.wasInitialized)
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
	wasInitialized bool,
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
		regions, err := s.upstream.regionCache.BatchLoadRegionsWithKeyRange(
			backoff, nextSpan.StartKey, nextSpan.EndKey, limit)
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
			regionInfo.wasInitialized = wasInitialized

			// Schedule a region request to subscribe the region.
			s.scheduleRegionRequest(ctx, regionInfo)

			nextSpan.StartKey = regionMeta.EndKey
			// If the nextSpan.StartKey is larger than the subscribedSpan.span.EndKey,
			// it means all span of the subscribedSpan have been requested. So we return.
			if common.EndCompare(nextSpan.StartKey, span.EndKey) >= 0 {
				return nil
			}
		}
	}
}

// scheduleRegionRequest locks the Region's range before submitting it to the
// request scheduler.
func (s *subscriptionClient) scheduleRegionRequest(ctx context.Context, region regionInfo) {
	if region.lockedRangeState != nil && region.lockedRangeState.Initialized.Load() {
		region.wasInitialized = true
	}
	lockRangeResult := region.subscribedSpan.rangeLock.LockRange(
		ctx, region.span.StartKey, region.span.EndKey, region.verID.GetID(), region.verID.GetVer())

	if lockRangeResult.Status == regionlock.LockRangeStatusWait {
		lockRangeResult = lockRangeResult.WaitFn()
	}

	switch lockRangeResult.Status {
	case regionlock.LockRangeStatusSuccess:
		region.lockedRangeState = lockRangeResult.LockedRangeState
		s.regionScheduler.submit(region)
	case regionlock.LockRangeStatusStale:
		for _, r := range lockRangeResult.RetryRanges {
			s.scheduleRangeRequest(ctx, r, region.subscribedSpan, region.filterLoop, region.wasInitialized)
		}
	default:
		return
	}
}

func (s *subscriptionClient) scheduleRangeRequest(
	ctx context.Context, span heartbeatpb.TableSpan,
	subscribedSpan *subscribedSpan,
	filterLoop bool,
	wasInitialized bool,
) {
	select {
	case <-ctx.Done():
	case s.rangeTaskCh <- rangeTask{
		span: span, subscribedSpan: subscribedSpan,
		filterLoop: filterLoop, wasInitialized: wasInitialized,
	}:
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
