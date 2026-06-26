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
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/spanz"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	// Maximum total sleep time(in ms), 20 seconds.
	tikvRequestMaxBackoff = 20000

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

const kvEventsCacheMaxSize = 32

// subscribedSpan represents a span to subscribe.
// It contains a sub span of a table(or the total span of a table),
// the startTs of the table, and the output event channel.
type subscribedSpan struct {
	subID   SubscriptionID
	startTs uint64
	// Whether to filter out the value written by TiCDC itself.
	// It should be `true` in BDR mode.
	filterLoop bool

	// The target span
	span heartbeatpb.TableSpan
	// The range lock of the span,
	// it is used to prevent duplicate requests to the same region range,
	// and it also used to calculate this table's resolvedTs.
	rangeLock *regionlock.RangeLock

	consumeKVEvents func(events []common.RawKVEntry, wakeCallback func()) bool

	advanceResolvedTs func(ts uint64)

	advanceInterval int64

	kvEventsCache []common.RawKVEntry

	// To handle span removing.
	stopped atomic.Bool

	// To handle stale lock resolvings.
	tryResolveLock     func(regionID uint64, state *regionlock.LockedRangeState)
	staleLocksTargetTs atomic.Uint64

	lastAdvanceTime atomic.Int64

	initialized       atomic.Bool
	resolvedTsUpdated atomic.Int64
	resolvedTs        atomic.Uint64
}

func (span *subscribedSpan) clearKVEventsCache() {
	if cap(span.kvEventsCache) > kvEventsCacheMaxSize {
		span.kvEventsCache = nil
	} else {
		span.kvEventsCache = span.kvEventsCache[:0]
	}
}

type SubscriptionClientConfig struct {
	// The number of region request workers to send region task for every tikv store
	RegionRequestWorkerPerStore uint
}

type upstreamHandle struct {
	pd          pd.Client
	credential  *security.Credential
	pdClock     pdutil.Clock
	regionCache *tikv.RegionCache
	clusterID   uint64
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
	config   *SubscriptionClientConfig
	upstream *upstreamHandle

	lockResolver txnutil.LockResolver

	eventSink       *regionEventSink
	failureReporter *regionFailureReporter

	totalSpans struct {
		sync.RWMutex
		spanMap map[SubscriptionID]*subscribedSpan
	}

	// rangeTaskCh is used to receive range tasks.
	// The tasks will be handled in `handleRangeTask` goroutine.
	rangeTaskCh     chan rangeTask
	regionScheduler *regionRequestScheduler
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
	regionCache := appcontext.GetService[*tikv.RegionCache](appcontext.RegionCache)
	pdClock := appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock)
	subClient := &subscriptionClient{
		config: config,
		upstream: &upstreamHandle{
			pd:          pd,
			credential:  credential,
			pdClock:     pdClock,
			regionCache: regionCache,
		},
		lockResolver: lockResolver,

		rangeTaskCh:            make(chan rangeTask, 1024),
		resolveLockTaskCh:      make(chan resolveLockTask, 1024),
		resolveLockRateLimiter: newResolveLockRateLimiter(),
	}
	subClient.ctx, subClient.cancel = context.WithCancel(context.Background())
	subClient.totalSpans.spanMap = make(map[SubscriptionID]*subscribedSpan)

	subClient.failureReporter = newRegionFailureReporter(
		subClient.upstream,
		subClient.onTableDrained,
		subClient.scheduleRegionRequest,
		subClient.scheduleRangeRequest,
	)
	subClient.eventSink = newRegionEventSink(subClient.ctx, subClient.failureReporter)
	subClient.regionScheduler = newRegionRequestScheduler(subClient)
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
			resolvedTsLag := s.GetResolvedTsLag()
			if resolvedTsLag > 0 {
				metrics.LogPullerResolvedTsLag.Set(resolvedTsLag)
			}
			dsMetrics := s.eventSink.Metrics()
			metricSubscriptionClientDSChannelSize.Set(float64(dsMetrics.EventChanSize))
			metricSubscriptionClientDSPendingQueueLen.Set(float64(dsMetrics.PendingQueueLen))
			if len(dsMetrics.MemoryControl.AreaMemoryMetrics) > 1 {
				log.Panic("subscription client should have only one area")
			}
			if len(dsMetrics.MemoryControl.AreaMemoryMetrics) > 0 {
				areaMetric := dsMetrics.MemoryControl.AreaMemoryMetrics[0]
				metrics.DynamicStreamMemoryUsage.WithLabelValues(
					"log-puller",
					"max",
					"default",
					"default",
				).Set(float64(areaMetric.MaxMemory()))
				metrics.DynamicStreamMemoryUsage.WithLabelValues(
					"log-puller",
					"used",
					"default",
					"default",
				).Set(float64(areaMetric.MemoryUsage()))
			}

			s.regionScheduler.updateMetrics()

			count := 0
			s.totalSpans.RLock()
			for _, rt := range s.totalSpans.spanMap {
				count += rt.rangeLock.Len()
			}
			s.totalSpans.RUnlock()
			metrics.SubscriptionClientSubscribedRegionCount.Set(float64(count))
		}
	}
}

// Subscribe the given table span.
// NOTE: `span.TableID` must be set correctly.
// It new a subscribedSpan and store it in `s.totalSpans`,
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
	s.totalSpans.Lock()
	s.totalSpans.spanMap[subID] = rt
	s.totalSpans.Unlock()

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
	// NOTE: `subID` is cleared from `s.totalSpans` in `onTableDrained`.
	s.totalSpans.Lock()
	rt := s.totalSpans.spanMap[subID]
	s.totalSpans.Unlock()
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
	s.upstream.clusterID = s.upstream.pd.GetClusterID(ctx)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error { return s.updateMetrics(ctx) })
	g.Go(func() error { return s.eventSink.Run(ctx) })
	g.Go(func() error { return s.failureReporter.Run(ctx) })
	g.Go(func() error { return s.handleRangeTasks(ctx) })
	g.Go(func() error { return s.regionScheduler.Run(ctx, g) })
	g.Go(func() error { return s.runResolveLockChecker(ctx) })
	g.Go(func() error { return s.handleResolveLockTasks(ctx) })
	g.Go(func() error { return s.logSlowRegions(ctx) })

	log.Info("subscription client starts")
	defer log.Info("subscription client exits")
	return g.Wait()
}

// Close closes the client. Must be called after `Run` returns.
func (s *subscriptionClient) Close(ctx context.Context) error {
	s.cancel()
	s.eventSink.Close()
	if s.regionScheduler != nil {
		s.regionScheduler.close()
	}
	return nil
}

func (s *subscriptionClient) setTableStopped(rt *subscribedSpan) {
	log.Info("subscription client starts to stop table",
		zap.Uint64("subscriptionID", uint64(rt.subID)))

	// Set stopped to true so we can stop handling region events from the table.
	// Then send a special singleRegionInfo to regionRouter to deregister the table
	// from all TiKV instances.
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
	s.totalSpans.Lock()
	defer s.totalSpans.Unlock()
	delete(s.totalSpans.spanMap, rt.subID)
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
		regions, err := s.upstream.regionCache.BatchLoadRegionsWithKeyRange(backoff, nextSpan.StartKey, nextSpan.EndKey, limit)
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

// scheduleRegionRequest locks the region's range and submits it to the region
// request scheduler.
func (s *subscriptionClient) scheduleRegionRequest(ctx context.Context, region regionInfo, priority TaskType) {
	lockRangeResult := region.subscribedSpan.rangeLock.LockRange(
		ctx, region.span.StartKey, region.span.EndKey, region.verID.GetID(), region.verID.GetVer())

	if lockRangeResult.Status == regionlock.LockRangeStatusWait {
		lockRangeResult = lockRangeResult.WaitFn()
	}

	switch lockRangeResult.Status {
	case regionlock.LockRangeStatusSuccess:
		region.lockedRangeState = lockRangeResult.LockedRangeState
		s.regionScheduler.submit(priority, region)
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

type subscriptionAndTargetTs struct {
	subSpan  *subscribedSpan
	targetTs uint64
}

func (s *subscriptionClient) runResolveLockChecker(ctx context.Context) error {
	resolveLockTicker := time.NewTicker(resolveLockTickInterval)
	defer resolveLockTicker.Stop()
	maxCacheSize := 1024
	subSpanAndTsCache := make([]subscriptionAndTargetTs, 0, maxCacheSize)
	// getResolvedTargetTs returns the targetTs to resolve stale locks. 0 means no need to resolve.
	getResolvedTargetTs := func(subSpan *subscribedSpan, currentTime time.Time, currentTs uint64) uint64 {
		resolvedTsUpdated := time.Unix(subSpan.resolvedTsUpdated.Load(), 0)
		if !subSpan.initialized.Load() || time.Since(resolvedTsUpdated) < resolveLockFence {
			return 0
		}
		resolvedTs := subSpan.resolvedTs.Load()
		resolvedTime := oracle.GetTimeFromTS(resolvedTs)
		if currentTime.Sub(resolvedTime) < resolveLockFence {
			return 0
		}
		return min(currentTs, oracle.GoTimeToTS(resolvedTime.Add(resolveLockFence)))
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-resolveLockTicker.C:
		}

		physical, logic, err := s.upstream.pd.GetTS(ctx)
		if err != nil {
			log.Warn("get ts from pd failed", zap.Error(err))
			continue
		}
		currentTs := oracle.ComposeTS(physical, logic)
		currentTime := s.upstream.pdClock.CurrentTime()
		s.totalSpans.Lock()
		for _, subSpan := range s.totalSpans.spanMap {
			if subSpan != nil {
				targetTs := getResolvedTargetTs(subSpan, currentTime, currentTs)
				if targetTs > 0 {
					subSpanAndTsCache = append(subSpanAndTsCache, subscriptionAndTargetTs{
						subSpan:  subSpan,
						targetTs: targetTs,
					})
				}
			}
		}
		s.totalSpans.Unlock()
		for _, subSpanAndTs := range subSpanAndTsCache {
			subSpanAndTs.subSpan.resolveStaleLocks(subSpanAndTs.targetTs)
		}
		subSpanAndTsCache = subSpanAndTsCache[:0]
		if cap(subSpanAndTsCache) > maxCacheSize {
			subSpanAndTsCache = make([]subscriptionAndTargetTs, 0, maxCacheSize)
		}
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

func (s *subscriptionClient) logSlowRegions(ctx context.Context) error {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		currTime := s.upstream.pdClock.CurrentTime()
		s.totalSpans.RLock()
		slowInitializeRegion := 0
		for subscriptionID, rt := range s.totalSpans.spanMap {
			attr := rt.rangeLock.IterAll(nil)
			ckptTime := oracle.GetTimeFromTS(attr.SlowestRegion.ResolvedTs)
			if attr.SlowestRegion.Initialized {
				if currTime.Sub(ckptTime) > 6*resolveLockMinInterval {
					log.Info("subscription client finds a initialized slow region",
						zap.Uint64("subscriptionID", uint64(subscriptionID)),
						zap.Any("slowRegion", attr.SlowestRegion))
				}
			} else if currTime.Sub(attr.SlowestRegion.Created) > 10*time.Minute {
				slowInitializeRegion++
				log.Info("subscription client initializes a region too slow",
					zap.Uint64("subscriptionID", uint64(subscriptionID)),
					zap.Any("slowRegion", attr.SlowestRegion))
			} else if currTime.Sub(ckptTime) > 10*time.Minute {
				log.Info("subscription client finds a uninitialized slow region",
					zap.Uint64("subscriptionID", uint64(subscriptionID)),
					zap.Any("slowRegion", attr.SlowestRegion))
			}
			if len(attr.UnLockedRanges) > 0 {
				log.Info("subscription client holes exist",
					zap.Uint64("subscriptionID", uint64(subscriptionID)),
					zap.Any("holes", attr.UnLockedRanges))
			}
		}
		s.totalSpans.RUnlock()
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
	rangeLock := regionlock.NewRangeLock(uint64(subID), span.StartKey, span.EndKey, startTs)

	rt := &subscribedSpan{
		subID:      subID,
		span:       span,
		startTs:    startTs,
		filterLoop: filterLoop,
		rangeLock:  rangeLock,

		consumeKVEvents:   consumeKVEvents,
		advanceResolvedTs: advanceResolvedTs,
		advanceInterval:   advanceInterval,
	}
	rt.initialized.Store(false)
	rt.resolvedTsUpdated.Store(time.Now().Unix())
	rt.resolvedTs.Store(startTs)

	rt.tryResolveLock = func(regionID uint64, state *regionlock.LockedRangeState) {
		targetTs := rt.staleLocksTargetTs.Load()
		if !state.Initialized.Load() || state.ResolvedTs.Load() >= targetTs {
			return
		}
		key := resolveLockKey{keyspaceID: span.KeyspaceID, regionID: regionID}
		if !s.resolveLockRateLimiter.trySchedule(key, time.Now()) {
			return
		}
		select {
		case <-s.ctx.Done():
			s.resolveLockRateLimiter.cancel(key)
		case s.resolveLockTaskCh <- resolveLockTask{
			keyspaceID: span.KeyspaceID,
			regionID:   regionID,
			targetTs:   targetTs,
			state:      state,
		}:
		// it is ok to ignore resolve lock task when the channel is full
		default:
			s.resolveLockRateLimiter.cancel(key)
			metrics.SubscriptionClientResolveLockTaskDropCounter.Inc()
		}
	}
	return rt
}

func (s *subscriptionClient) GetResolvedTsLag() float64 {
	pullerMinResolvedTs := uint64(0)
	s.totalSpans.RLock()
	for _, rt := range s.totalSpans.spanMap {
		resolvedTs := rt.resolvedTs.Load()
		if pullerMinResolvedTs == 0 || resolvedTs < pullerMinResolvedTs {
			pullerMinResolvedTs = resolvedTs
		}
	}
	s.totalSpans.RUnlock()
	if pullerMinResolvedTs == 0 {
		return 0
	}
	pdTime := s.upstream.pdClock.CurrentTime()
	phyResolvedTs := oracle.ExtractPhysical(pullerMinResolvedTs)
	lag := float64(oracle.GetPhysical(pdTime)-phyResolvedTs) / 1e3
	return lag
}

func (r *subscribedSpan) resolveStaleLocks(targetTs uint64) {
	util.MustCompareAndMonotonicIncrease(&r.staleLocksTargetTs, targetTs)
	res := r.rangeLock.IterAll(r.tryResolveLock)
	log.Debug("subscription client finds slow locked ranges",
		zap.Uint64("subscriptionID", uint64(r.subID)),
		zap.Any("ranges", res))
}
