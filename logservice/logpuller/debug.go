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
	"encoding/hex"
	"sort"
	"time"

	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/tikv/client-go/v2/oracle"
)

const defaultSlowSubscriptionLimit = 20

const (
	pullerDebugStateCreated uint32 = iota
	pullerDebugStateRunning
	pullerDebugStateStopped
)

// DebugInfoProvider exposes read-only snapshots of the local log puller.
// Implementations must not perform remote requests while collecting a snapshot.
type DebugInfoProvider interface {
	GetPullerDebugInfo() PullerDebugInfo
	GetPullerDebugSubscriptions() []PullerSubscriptionDebugInfo
	GetPullerDebugSubscription(
		subID SubscriptionID,
		options PullerSubscriptionDebugOptions,
	) (PullerSubscriptionDetail, bool)
	GetPullerDebugStores() []PullerStoreDebugInfo
	GetPullerDebugStore(address string) (PullerStoreDebugInfo, bool)
}

// PullerDebugInfo is a lightweight overview of one node's log puller.
type PullerDebugInfo struct {
	SnapshotAt        time.Time                     `json:"snapshot_at"`
	State             string                        `json:"state"`
	Channels          PullerChannelDebugInfo        `json:"channels"`
	Subscriptions     PullerSubscriptionStats       `json:"subscriptions"`
	Scheduler         PullerSchedulerDebugInfo      `json:"scheduler"`
	Failure           PullerFailureDebugInfo        `json:"failure"`
	EventSink         PullerEventSinkDebugInfo      `json:"event_sink"`
	Memory            PullerMemoryDebugInfo         `json:"memory"`
	SlowSubscriptions []PullerSubscriptionDebugInfo `json:"slow_subscriptions"`
}

// PullerChannelDebugInfo reports the two bounded input channels.
type PullerChannelDebugInfo struct {
	RangeTask       PullerQueueDebugInfo `json:"range_task"`
	ResolveLockTask PullerQueueDebugInfo `json:"resolve_lock_task"`
}

// PullerQueueDebugInfo reports an in-memory queue's current size.
type PullerQueueDebugInfo struct {
	Length   int `json:"length"`
	Capacity int `json:"capacity,omitempty"`
}

// PullerSubscriptionStats contains aggregate subscription progress.
type PullerSubscriptionStats struct {
	Total            int   `json:"total"`
	Initialized      int   `json:"initialized"`
	Uninitialized    int   `json:"uninitialized"`
	Stopping         int   `json:"stopping"`
	LockedRegions    int   `json:"locked_regions"`
	MaxResolvedTsLag int64 `json:"max_resolved_ts_lag_ms"`
}

// PullerSchedulerDebugInfo reports scheduler and worker queue sizes.
type PullerSchedulerDebugInfo struct {
	GlobalPending     int `json:"global_pending"`
	StoreCount        int `json:"store_count"`
	WorkerCount       int `json:"worker_count"`
	WorkerPending     int `json:"worker_pending"`
	InflightScan      int `json:"inflight_scan"`
	TrackedRegions    int `json:"tracked_regions"`
	DeregisterPending int `json:"deregister_pending"`
}

// PullerFailureDebugInfo reports queued failures and active recovery state.
type PullerFailureDebugInfo struct {
	PendingErrors      int    `json:"pending_errors"`
	DrainedSpans       int    `json:"drained_spans"`
	RecoveringRanges   int    `json:"recovering_ranges"`
	MaxRecoveryAttempt uint32 `json:"max_recovery_attempt"`
}

// PullerEventSinkDebugInfo reports the dynamic stream backlog.
type PullerEventSinkDebugInfo struct {
	EventChannelSize int `json:"event_channel_size"`
	PendingQueue     int `json:"pending_queue"`
	Paths            int `json:"paths"`
}

// PullerMemoryDebugInfo reports event memory and initial-scan admission state.
type PullerMemoryDebugInfo struct {
	CapacityBytes               uint64 `json:"capacity_bytes"`
	EventUsedBytes              uint64 `json:"event_used_bytes"`
	ScanUsedBytes               uint64 `json:"scan_used_bytes"`
	HardLimitBytes              uint64 `json:"hard_limit_bytes"`
	PauseLowPriorityLimitBytes  uint64 `json:"pause_low_priority_limit_bytes"`
	ResumeLowPriorityLimitBytes uint64 `json:"resume_low_priority_limit_bytes"`
	AdmissionLevel              string `json:"admission_level"`
	EventWaiters                int64  `json:"event_waiters"`
	ScanWaiters                 int64  `json:"scan_waiters"`
}

// PullerSubscriptionDebugInfo is the lightweight form used by list APIs.
type PullerSubscriptionDebugInfo struct {
	SubscriptionID       SubscriptionID `json:"subscription_id,string"`
	KeyspaceID           uint32         `json:"keyspace_id"`
	TableID              int64          `json:"table_id,string"`
	StartTs              uint64         `json:"start_ts,string"`
	ResolvedTs           uint64         `json:"resolved_ts,string"`
	ResolvedTsTime       *time.Time     `json:"resolved_ts_time,omitempty"`
	ResolvedTsLagMillis  int64          `json:"resolved_ts_lag_ms"`
	ResolvedTsUpdatedAt  *time.Time     `json:"resolved_ts_updated_at,omitempty"`
	ResolvedTsStaleForMs int64          `json:"resolved_ts_stale_for_ms"`
	Initialized          bool           `json:"initialized"`
	Stopped              bool           `json:"stopped"`
	EverCaughtUp         bool           `json:"ever_caught_up"`
	FilterLoop           bool           `json:"filter_loop"`
	LockedRegions        int            `json:"locked_regions"`
}

// PullerSubscriptionDebugOptions controls the potentially expensive Region walk.
type PullerSubscriptionDebugOptions struct {
	RegionMode  string
	RegionLimit int
	IncludeKeys bool
}

// PullerSubscriptionDetail contains one subscription and optional Region details.
type PullerSubscriptionDetail struct {
	SnapshotAt               time.Time                       `json:"snapshot_at"`
	Subscription             PullerSubscriptionDebugInfo     `json:"subscription"`
	AdvanceInterval          int64                           `json:"advance_interval_ms"`
	LastAdvanceAt            *time.Time                      `json:"last_advance_at,omitempty"`
	StaleLockTarget          uint64                          `json:"stale_locks_target_ts,string"`
	Ranges                   PullerRangeStats                `json:"ranges"`
	Pipeline                 PullerSubscriptionPipelineInfo  `json:"pipeline"`
	Regions                  []PullerRegionDebugInfo         `json:"regions,omitempty"`
	UncoveredRanges          []PullerUncoveredRangeDebugInfo `json:"uncovered_ranges,omitempty"`
	RegionsTruncated         bool                            `json:"regions_truncated"`
	UncoveredRangesTruncated bool                            `json:"uncovered_ranges_truncated"`
}

// PullerRangeStats summarizes the RangeLock owned by one subscription.
type PullerRangeStats struct {
	LockedRegions        int                    `json:"locked_regions"`
	InitializedRegions   int                    `json:"initialized_regions"`
	UninitializedRegions int                    `json:"uninitialized_regions"`
	UncoveredRanges      int                    `json:"uncovered_range_count"`
	FastestRegion        *PullerRegionDebugInfo `json:"fastest_region,omitempty"`
	SlowestRegion        *PullerRegionDebugInfo `json:"slowest_region,omitempty"`
}

// PullerSubscriptionPipelineInfo reports the states that can be attributed safely.
type PullerSubscriptionPipelineInfo struct {
	InitialScanning  int `json:"initial_scanning"`
	StreamingRegions int `json:"streaming_regions"`
	RecoveringRanges int `json:"recovering_ranges"`
}

// PullerRegionDebugInfo contains progress for one locked Region range.
type PullerRegionDebugInfo struct {
	RegionID        uint64     `json:"region_id,string"`
	ResolvedTs      uint64     `json:"resolved_ts,string"`
	ResolvedTsTime  *time.Time `json:"resolved_ts_time,omitempty"`
	ResolvedTsLagMs int64      `json:"resolved_ts_lag_ms"`
	Initialized     bool       `json:"initialized"`
	CreatedAt       time.Time  `json:"created_at"`
	AgeMillis       int64      `json:"age_ms"`
	StoreAddress    string     `json:"store_address,omitempty"`
	WorkerID        uint64     `json:"worker_id,string,omitempty"`
	Phase           string     `json:"phase"`
}

// PullerUncoveredRangeDebugInfo reports a range not currently owned by a Region request.
type PullerUncoveredRangeDebugInfo struct {
	ResolvedTs uint64 `json:"resolved_ts,string"`
	StartKey   string `json:"start_key,omitempty"`
	EndKey     string `json:"end_key,omitempty"`
}

// PullerStoreDebugInfo reports the workers connected to one TiKV address.
type PullerStoreDebugInfo struct {
	Address           string                  `json:"address"`
	WorkerCount       int                     `json:"worker_count"`
	PendingRequests   int                     `json:"pending_requests"`
	InflightScans     int                     `json:"inflight_scans"`
	TrackedRegions    int                     `json:"tracked_regions"`
	DeregisterPending int                     `json:"deregister_pending"`
	Workers           []PullerWorkerDebugInfo `json:"workers,omitempty"`
}

// PullerWorkerDebugInfo reports queue sizes owned by one request worker.
type PullerWorkerDebugInfo struct {
	WorkerID             uint64 `json:"worker_id,string"`
	PendingRequests      int    `json:"pending_requests"`
	InflightScans        int    `json:"inflight_scans"`
	TrackedRegions       int    `json:"tracked_regions"`
	TrackedSubscriptions int    `json:"tracked_subscriptions"`
	DeregisterPending    int    `json:"deregister_pending"`
}

type pullerTrackedRegion struct {
	storeAddress string
	workerID     uint64
	initialized  bool
}

// GetPullerDebugInfo returns a lightweight snapshot without walking every Region.
func (s *subscriptionClient) GetPullerDebugInfo() PullerDebugInfo {
	now := time.Now()
	info := PullerDebugInfo{
		SnapshotAt: now,
		State:      "created",
	}
	switch s.runState.Load() {
	case pullerDebugStateCreated:
		info.State = "created"
	case pullerDebugStateRunning:
		info.State = "running"
	case pullerDebugStateStopped:
		info.State = "stopped"
	}
	if s.rangeTaskCh != nil {
		info.Channels.RangeTask = PullerQueueDebugInfo{
			Length: len(s.rangeTaskCh), Capacity: cap(s.rangeTaskCh),
		}
	}
	if s.resolveLockTaskCh != nil {
		info.Channels.ResolveLockTask = PullerQueueDebugInfo{
			Length: len(s.resolveLockTaskCh), Capacity: cap(s.resolveLockTaskCh),
		}
	}

	subscriptions := s.GetPullerDebugSubscriptions()
	for _, subscription := range subscriptions {
		info.Subscriptions.Total++
		info.Subscriptions.LockedRegions += subscription.LockedRegions
		if subscription.Stopped {
			info.Subscriptions.Stopping++
		} else if subscription.Initialized {
			info.Subscriptions.Initialized++
		} else {
			info.Subscriptions.Uninitialized++
		}
		info.Subscriptions.MaxResolvedTsLag = max(
			info.Subscriptions.MaxResolvedTsLag, subscription.ResolvedTsLagMillis)
	}
	sort.Slice(subscriptions, func(i, j int) bool {
		if subscriptions[i].ResolvedTsLagMillis == subscriptions[j].ResolvedTsLagMillis {
			return subscriptions[i].SubscriptionID < subscriptions[j].SubscriptionID
		}
		return subscriptions[i].ResolvedTsLagMillis > subscriptions[j].ResolvedTsLagMillis
	})
	if len(subscriptions) > defaultSlowSubscriptionLimit {
		subscriptions = subscriptions[:defaultSlowSubscriptionLimit]
	}
	info.SlowSubscriptions = subscriptions

	stores := s.GetPullerDebugStores()
	info.Scheduler.StoreCount = len(stores)
	for _, store := range stores {
		info.Scheduler.WorkerCount += store.WorkerCount
		info.Scheduler.WorkerPending += store.PendingRequests
		info.Scheduler.InflightScan += store.InflightScans
		info.Scheduler.TrackedRegions += store.TrackedRegions
		info.Scheduler.DeregisterPending += store.DeregisterPending
	}
	if s.regionScheduler != nil && s.regionScheduler.taskQueue != nil {
		info.Scheduler.GlobalPending = s.regionScheduler.taskQueue.Len()
	}
	info.Failure = s.debugFailureInfo()
	info.EventSink = s.debugEventSinkInfo()
	info.Memory = s.debugMemoryInfo()
	return info
}

// GetPullerDebugSubscriptions returns lightweight snapshots sorted by subscription ID.
func (s *subscriptionClient) GetPullerDebugSubscriptions() []PullerSubscriptionDebugInfo {
	if s.spanRegistry == nil {
		return []PullerSubscriptionDebugInfo{}
	}
	now := time.Now()
	if s.spanRegistry.pdClock != nil {
		now = s.spanRegistry.pdClock.CurrentTime()
	}

	s.spanRegistry.RLock()
	spans := make([]*subscribedSpan, 0, len(s.spanRegistry.spans))
	for _, span := range s.spanRegistry.spans {
		spans = append(spans, span)
	}
	s.spanRegistry.RUnlock()

	result := make([]PullerSubscriptionDebugInfo, 0, len(spans))
	for _, span := range spans {
		result = append(result, debugSubscriptionInfo(span, now))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].SubscriptionID < result[j].SubscriptionID
	})
	return result
}

// GetPullerDebugSubscription returns details for one local subscription.
func (s *subscriptionClient) GetPullerDebugSubscription(
	subID SubscriptionID,
	options PullerSubscriptionDebugOptions,
) (PullerSubscriptionDetail, bool) {
	if s.spanRegistry == nil {
		return PullerSubscriptionDetail{}, false
	}
	span := s.spanRegistry.Get(subID)
	if span == nil {
		return PullerSubscriptionDetail{}, false
	}

	now := time.Now()
	if s.spanRegistry.pdClock != nil {
		now = s.spanRegistry.pdClock.CurrentTime()
	}
	detail := PullerSubscriptionDetail{
		SnapshotAt:      time.Now(),
		Subscription:    debugSubscriptionInfo(span, now),
		AdvanceInterval: span.advanceInterval,
		StaleLockTarget: span.staleLocksTargetTs.Load(),
		Regions:         []PullerRegionDebugInfo{},
		UncoveredRanges: []PullerUncoveredRangeDebugInfo{},
	}
	if lastAdvance := span.lastAdvanceTime.Load(); lastAdvance > 0 {
		lastAdvanceAt := time.UnixMilli(lastAdvance)
		detail.LastAdvanceAt = &lastAdvanceAt
	}

	regionLimit := options.RegionLimit
	if regionLimit <= 0 {
		regionLimit = defaultSlowSubscriptionLimit
	}
	mode := options.RegionMode
	if mode == "" {
		mode = "none"
	}
	candidateRegions := 0
	slowRegions := &pullerSlowRegionHeap{}
	regionStats := span.rangeLock.IterAll(func(regionID uint64, state *regionlock.LockedRangeState) {
		region := debugRegionInfo(regionID, state, now)
		if region.Initialized {
			detail.Ranges.InitializedRegions++
		} else {
			detail.Ranges.UninitializedRegions++
		}
		if mode == "none" || (mode == "uninitialized" && region.Initialized) {
			return
		}
		candidateRegions++
		if mode == "slow" {
			addSlowDebugRegion(slowRegions, region, regionLimit)
		} else if len(detail.Regions) < regionLimit {
			detail.Regions = append(detail.Regions, region)
		}
	})
	if mode == "slow" {
		detail.Regions = append(detail.Regions, (*slowRegions)...)
	}
	detail.RegionsTruncated = candidateRegions > len(detail.Regions)
	sort.Slice(detail.Regions, func(i, j int) bool {
		if mode == "slow" && detail.Regions[i].ResolvedTs != detail.Regions[j].ResolvedTs {
			return detail.Regions[i].ResolvedTs < detail.Regions[j].ResolvedTs
		}
		return detail.Regions[i].RegionID < detail.Regions[j].RegionID
	})

	selectedRegionIDs := make(map[uint64]struct{}, len(detail.Regions)+2)
	for _, region := range detail.Regions {
		selectedRegionIDs[region.RegionID] = struct{}{}
	}
	if regionStats.LockedRegionCount > 0 {
		selectedRegionIDs[regionStats.FastestRegion.RegionID] = struct{}{}
		selectedRegionIDs[regionStats.SlowestRegion.RegionID] = struct{}{}
	}
	tracked, initialScanning, streaming := s.debugTrackedSubscription(subID, selectedRegionIDs)
	for i := range detail.Regions {
		applyTrackedRegion(&detail.Regions[i], tracked)
	}
	detail.Pipeline.InitialScanning = initialScanning
	detail.Pipeline.StreamingRegions = streaming

	detail.Ranges.LockedRegions = regionStats.LockedRegionCount
	detail.Ranges.UncoveredRanges = len(regionStats.UnLockedRanges)
	if regionStats.LockedRegionCount > 0 {
		fastest := debugLockedRangeStatistic(regionStats.FastestRegion, now)
		slowest := debugLockedRangeStatistic(regionStats.SlowestRegion, now)
		applyTrackedRegion(&fastest, tracked)
		applyTrackedRegion(&slowest, tracked)
		detail.Ranges.FastestRegion = &fastest
		detail.Ranges.SlowestRegion = &slowest
	}
	for _, unlocked := range regionStats.UnLockedRanges {
		if len(detail.UncoveredRanges) >= regionLimit {
			detail.UncoveredRangesTruncated = true
			break
		}
		item := PullerUncoveredRangeDebugInfo{ResolvedTs: unlocked.ResolvedTs}
		if options.IncludeKeys {
			item.StartKey = hex.EncodeToString(unlocked.Span.StartKey)
			item.EndKey = hex.EncodeToString(unlocked.Span.EndKey)
		}
		detail.UncoveredRanges = append(detail.UncoveredRanges, item)
	}

	detail.Pipeline.RecoveringRanges = s.debugRecoveringRangeCount(subID)
	return detail, true
}

// GetPullerDebugStores returns store summaries without worker details.
func (s *subscriptionClient) GetPullerDebugStores() []PullerStoreDebugInfo {
	stores := s.debugStores(true)
	for i := range stores {
		stores[i].Workers = nil
	}
	return stores
}

// GetPullerDebugStore returns one store with per-worker queue information.
func (s *subscriptionClient) GetPullerDebugStore(address string) (PullerStoreDebugInfo, bool) {
	stores := s.debugStores(false)
	for _, store := range stores {
		if store.Address == address {
			return store, true
		}
	}
	return PullerStoreDebugInfo{}, false
}

func (s *subscriptionClient) debugStores(summaryOnly bool) []PullerStoreDebugInfo {
	if s.regionScheduler == nil {
		return []PullerStoreDebugInfo{}
	}
	stores := make([]PullerStoreDebugInfo, 0)
	s.regionScheduler.stores.Range(func(key, value any) bool {
		store := PullerStoreDebugInfo{Address: key.(string)}
		for _, worker := range value.(*regionRequestStore).workers {
			admission := worker.admission.stats()
			subscriptions, regions := worker.tracker.debugStats()
			workerInfo := PullerWorkerDebugInfo{
				WorkerID:             worker.workerID,
				PendingRequests:      admission.pending,
				InflightScans:        admission.inflight,
				TrackedRegions:       regions,
				TrackedSubscriptions: subscriptions,
				DeregisterPending:    worker.controlQueue.len(),
			}
			store.WorkerCount++
			store.PendingRequests += workerInfo.PendingRequests
			store.InflightScans += workerInfo.InflightScans
			store.TrackedRegions += workerInfo.TrackedRegions
			store.DeregisterPending += workerInfo.DeregisterPending
			if !summaryOnly {
				store.Workers = append(store.Workers, workerInfo)
			}
		}
		stores = append(stores, store)
		return true
	})
	sort.Slice(stores, func(i, j int) bool { return stores[i].Address < stores[j].Address })
	return stores
}

func (s *subscriptionClient) debugTrackedSubscription(
	subID SubscriptionID,
	selected map[uint64]struct{},
) (map[uint64]pullerTrackedRegion, int, int) {
	result := make(map[uint64]pullerTrackedRegion)
	if s.regionScheduler == nil {
		return result, 0, 0
	}
	initialScanning := 0
	streaming := 0
	s.regionScheduler.stores.Range(func(key, value any) bool {
		address := key.(string)
		for _, worker := range value.(*regionRequestStore).workers {
			workerRegions, workerInitial, workerStreaming := worker.tracker.debugSubscription(subID, selected)
			initialScanning += workerInitial
			streaming += workerStreaming
			for regionID, initialized := range workerRegions {
				result[regionID] = pullerTrackedRegion{
					storeAddress: address,
					workerID:     worker.workerID,
					initialized:  initialized,
				}
			}
		}
		return true
	})
	return result, initialScanning, streaming
}

func (s *subscriptionClient) debugFailureInfo() PullerFailureDebugInfo {
	if s.failureHandler == nil {
		return PullerFailureDebugInfo{}
	}
	info := PullerFailureDebugInfo{}
	s.failureHandler.cache.Lock()
	info.PendingErrors = len(s.failureHandler.cache.cache)
	info.DrainedSpans = len(s.failureHandler.cache.drainedSpans)
	s.failureHandler.cache.Unlock()

	s.failureHandler.recoveryMu.Lock()
	info.RecoveringRanges = len(s.failureHandler.recoveries)
	for _, state := range s.failureHandler.recoveries {
		info.MaxRecoveryAttempt = max(info.MaxRecoveryAttempt, state.attempt)
	}
	s.failureHandler.recoveryMu.Unlock()
	return info
}

func (s *subscriptionClient) debugRecoveringRangeCount(subID SubscriptionID) int {
	if s.failureHandler == nil {
		return 0
	}
	count := 0
	s.failureHandler.recoveryMu.Lock()
	for key := range s.failureHandler.recoveries {
		if key.subscriptionID == subID {
			count++
		}
	}
	s.failureHandler.recoveryMu.Unlock()
	return count
}

func (s *subscriptionClient) debugEventSinkInfo() PullerEventSinkDebugInfo {
	if s.eventSink == nil || s.eventSink.ds == nil {
		return PullerEventSinkDebugInfo{}
	}
	metrics := s.eventSink.ds.GetMetrics()
	return PullerEventSinkDebugInfo{
		EventChannelSize: metrics.EventChanSize,
		PendingQueue:     metrics.PendingQueueLen,
		Paths:            metrics.AddPath - metrics.RemovePath,
	}
}

func (s *subscriptionClient) debugMemoryInfo() PullerMemoryDebugInfo {
	if s.memoryQuota == nil {
		return PullerMemoryDebugInfo{}
	}
	quota := s.memoryQuota
	quota.scanMu.Lock()
	info := PullerMemoryDebugInfo{
		CapacityBytes:               quota.capacity,
		EventUsedBytes:              quota.used.Load(),
		ScanUsedBytes:               quota.scanUsed,
		HardLimitBytes:              quota.hardLimit,
		PauseLowPriorityLimitBytes:  quota.pauseLowPriorityLimit,
		ResumeLowPriorityLimitBytes: quota.resumeLowPriorityLimit,
		AdmissionLevel:              debugAdmissionLevel(quota.level),
		EventWaiters:                quota.eventNotifier.waiters.Load(),
		ScanWaiters:                 quota.scanWaiters.Load(),
	}
	quota.scanMu.Unlock()
	return info
}

func debugAdmissionLevel(level admissionLevel) string {
	if level == admissionPauseLowPriority {
		return "pause_low_priority"
	}
	return "normal"
}

func debugSubscriptionInfo(span *subscribedSpan, now time.Time) PullerSubscriptionDebugInfo {
	resolvedTs := span.resolvedTs.Load()
	resolvedTime, lag := debugTs(resolvedTs, now)
	info := PullerSubscriptionDebugInfo{
		SubscriptionID:      span.subID,
		KeyspaceID:          span.span.KeyspaceID,
		TableID:             span.span.TableID,
		StartTs:             span.startTs,
		ResolvedTs:          resolvedTs,
		ResolvedTsTime:      resolvedTime,
		ResolvedTsLagMillis: lag,
		Initialized:         span.initialized.Load(),
		Stopped:             span.stopped.Load(),
		EverCaughtUp:        span.priorityPolicy.everCaughtUp.Load(),
		FilterLoop:          span.filterLoop,
		LockedRegions:       span.rangeLock.Len(),
	}
	if updated := span.resolvedTsUpdated.Load(); updated > 0 {
		updatedAt := time.Unix(updated, 0)
		info.ResolvedTsUpdatedAt = &updatedAt
		info.ResolvedTsStaleForMs = max(int64(0), now.Sub(updatedAt).Milliseconds())
	}
	return info
}

func debugRegionInfo(
	regionID uint64,
	state *regionlock.LockedRangeState,
	now time.Time,
) PullerRegionDebugInfo {
	resolvedTs := state.ResolvedTs.Load()
	resolvedTime, lag := debugTs(resolvedTs, now)
	return PullerRegionDebugInfo{
		RegionID:        regionID,
		ResolvedTs:      resolvedTs,
		ResolvedTsTime:  resolvedTime,
		ResolvedTsLagMs: lag,
		Initialized:     state.Initialized.Load(),
		CreatedAt:       state.Created,
		AgeMillis:       max(int64(0), now.Sub(state.Created).Milliseconds()),
		Phase:           "scheduled_or_recovering",
	}
}

func debugLockedRangeStatistic(
	stat regionlock.LockedRangeStatistic,
	now time.Time,
) PullerRegionDebugInfo {
	resolvedTime, lag := debugTs(stat.ResolvedTs, now)
	return PullerRegionDebugInfo{
		RegionID:        stat.RegionID,
		ResolvedTs:      stat.ResolvedTs,
		ResolvedTsTime:  resolvedTime,
		ResolvedTsLagMs: lag,
		Initialized:     stat.Initialized,
		CreatedAt:       stat.Created,
		AgeMillis:       max(int64(0), now.Sub(stat.Created).Milliseconds()),
		Phase:           "unknown",
	}
}

func debugTs(ts uint64, now time.Time) (*time.Time, int64) {
	if ts == 0 {
		return nil, 0
	}
	physical := oracle.GetTimeFromTS(ts)
	return &physical, max(int64(0), now.Sub(physical).Milliseconds())
}

func applyTrackedRegion(
	region *PullerRegionDebugInfo,
	tracked map[uint64]pullerTrackedRegion,
) {
	owner, ok := tracked[region.RegionID]
	if !ok {
		return
	}
	region.StoreAddress = owner.storeAddress
	region.WorkerID = owner.workerID
	if owner.initialized {
		region.Phase = "streaming"
	} else {
		region.Phase = "initial_scan"
	}
}

func (t *regionTracker) debugStats() (subscriptions int, regions int) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	subscriptions = len(t.statesBySubscription)
	for _, states := range t.statesBySubscription {
		regions += len(states)
	}
	return subscriptions, regions
}

func (t *regionTracker) debugSubscription(
	subID SubscriptionID,
	selected map[uint64]struct{},
) (map[uint64]bool, int, int) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	result := make(map[uint64]bool, len(selected))
	initialScanning := 0
	streaming := 0
	for regionID, state := range t.statesBySubscription[subID] {
		initialized := state.isInitialized()
		if initialized {
			streaming++
		} else {
			initialScanning++
		}
		if _, ok := selected[regionID]; ok {
			result[regionID] = initialized
		}
	}
	return result, initialScanning, streaming
}

type pullerSlowRegionHeap []PullerRegionDebugInfo

func (h pullerSlowRegionHeap) Len() int { return len(h) }
func (h pullerSlowRegionHeap) Less(i, j int) bool {
	if h[i].ResolvedTs == h[j].ResolvedTs {
		return h[i].RegionID > h[j].RegionID
	}
	return h[i].ResolvedTs > h[j].ResolvedTs
}
func (h pullerSlowRegionHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *pullerSlowRegionHeap) Push(value any) {
	*h = append(*h, value.(PullerRegionDebugInfo))
}

func (h *pullerSlowRegionHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

func addSlowDebugRegion(
	regions *pullerSlowRegionHeap,
	region PullerRegionDebugInfo,
	limit int,
) {
	if regions.Len() < limit {
		heap.Push(regions, region)
		return
	}
	fastestSelected := (*regions)[0]
	if region.ResolvedTs > fastestSelected.ResolvedTs ||
		region.ResolvedTs == fastestSelected.ResolvedTs && region.RegionID >= fastestSelected.RegionID {
		return
	}
	heap.Pop(regions)
	heap.Push(regions, region)
}
