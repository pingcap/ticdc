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
	"container/heap"
	"sort"
	"time"

	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/tikv/client-go/v2/oracle"
)

const maxPullerDebugResultLimit = 20

// DebugInfoProvider exposes read-only snapshots of local puller progress.
// Implementations must not perform remote requests while collecting a snapshot.
type DebugInfoProvider interface {
	GetPullerDebugInfo(options PullerDebugOptions) PullerDebugInfo
	GetPullerDebugRegion(
		subID SubscriptionID,
		regionID uint64,
	) (PullerRegionDebugDetail, bool)
}

// PullerDebugOptions bounds the number of subscriptions and Regions returned.
type PullerDebugOptions struct {
	SubscriptionLimit int
	RegionLimit       int
}

// PullerDebugInfo reports the slowest active subscriptions on this node.
type PullerDebugInfo struct {
	SnapshotAt        time.Time                     `json:"snapshot_at"`
	SlowSubscriptions []PullerSubscriptionDebugInfo `json:"slow_subscriptions"`
}

// PullerSubscriptionDebugInfo reports one subscription and its slowest Regions.
type PullerSubscriptionDebugInfo struct {
	SubscriptionID       SubscriptionID          `json:"subscription_id,string"`
	KeyspaceID           uint32                  `json:"keyspace_id"`
	TableID              int64                   `json:"table_id,string"`
	ResolvedTs           uint64                  `json:"resolved_ts,string"`
	ResolvedTsLagMillis  int64                   `json:"resolved_ts_lag_ms"`
	Initialized          bool                    `json:"initialized"`
	LockedRegions        int                     `json:"locked_regions"`
	InitializedRegions   int                     `json:"initialized_regions"`
	UninitializedRegions int                     `json:"uninitialized_regions"`
	UncoveredRanges      int                     `json:"uncovered_ranges"`
	SlowRegions          []PullerRegionDebugInfo `json:"slow_regions"`
}

// PullerRegionDebugDetail identifies the subscription that owns one Region.
type PullerRegionDebugDetail struct {
	SnapshotAt     time.Time             `json:"snapshot_at"`
	SubscriptionID SubscriptionID        `json:"subscription_id,string"`
	KeyspaceID     uint32                `json:"keyspace_id"`
	TableID        int64                 `json:"table_id,string"`
	Region         PullerRegionDebugInfo `json:"region"`
}

// PullerRegionDebugInfo reports the current local state of one locked Region.
type PullerRegionDebugInfo struct {
	RegionID         uint64     `json:"region_id,string"`
	ResolvedTs       uint64     `json:"resolved_ts,string"`
	ResolvedTsLagMs  int64      `json:"resolved_ts_lag_ms"`
	Initialized      bool       `json:"initialized"`
	CreatedAt        time.Time  `json:"created_at"`
	AgeMillis        int64      `json:"age_ms"`
	StoreAddress     string     `json:"store_address,omitempty"`
	WorkerID         uint64     `json:"worker_id,string,omitempty"`
	Phase            string     `json:"phase"`
	RequestCreatedAt *time.Time `json:"request_created_at,omitempty"`
}

type pullerTrackedRegion struct {
	storeAddress     string
	workerID         uint64
	initialized      bool
	requestCreatedAt *time.Time
}

type pullerSlowSubscription struct {
	span       *subscribedSpan
	resolvedTs uint64
}

type pullerSlowSubscriptionHeap []pullerSlowSubscription

func (h pullerSlowSubscriptionHeap) Len() int { return len(h) }
func (h pullerSlowSubscriptionHeap) Less(i, j int) bool {
	if h[i].resolvedTs == h[j].resolvedTs {
		return h[i].span.subID > h[j].span.subID
	}
	return h[i].resolvedTs > h[j].resolvedTs
}
func (h pullerSlowSubscriptionHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *pullerSlowSubscriptionHeap) Push(value any) {
	*h = append(*h, value.(pullerSlowSubscription))
}
func (h *pullerSlowSubscriptionHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
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

// GetPullerDebugInfo returns the slowest active subscriptions and their slowest
// locked Regions. It walks Regions only for the selected subscriptions.
func (s *subscriptionClient) GetPullerDebugInfo(options PullerDebugOptions) PullerDebugInfo {
	now := s.debugNow()
	info := PullerDebugInfo{
		SnapshotAt:        now,
		SlowSubscriptions: []PullerSubscriptionDebugInfo{},
	}
	if s.spanRegistry == nil || options.SubscriptionLimit <= 0 || options.RegionLimit <= 0 {
		return info
	}
	subscriptionLimit := min(options.SubscriptionLimit, maxPullerDebugResultLimit)
	regionLimit := min(options.RegionLimit, maxPullerDebugResultLimit)

	slowest := &pullerSlowSubscriptionHeap{}
	s.spanRegistry.RLock()
	for _, span := range s.spanRegistry.spans {
		if span.stopped.Load() {
			continue
		}
		addSlowDebugSubscription(slowest, pullerSlowSubscription{
			span:       span,
			resolvedTs: span.resolvedTs.Load(),
		}, subscriptionLimit)
	}
	s.spanRegistry.RUnlock()
	sort.Slice(*slowest, func(i, j int) bool {
		if (*slowest)[i].resolvedTs == (*slowest)[j].resolvedTs {
			return (*slowest)[i].span.subID < (*slowest)[j].span.subID
		}
		return (*slowest)[i].resolvedTs < (*slowest)[j].resolvedTs
	})

	for _, selected := range *slowest {
		info.SlowSubscriptions = append(
			info.SlowSubscriptions,
			s.debugSubscription(selected.span, selected.resolvedTs, regionLimit, now),
		)
	}
	return info
}

// GetPullerDebugRegion returns one locked Region owned by a local subscription.
func (s *subscriptionClient) GetPullerDebugRegion(
	subID SubscriptionID,
	regionID uint64,
) (PullerRegionDebugDetail, bool) {
	now := s.debugNow()
	if s.spanRegistry == nil {
		return PullerRegionDebugDetail{}, false
	}
	span := s.spanRegistry.Get(subID)
	if span == nil {
		return PullerRegionDebugDetail{}, false
	}
	stat, found := span.rangeLock.GetRegionState(regionID)
	if !found {
		return PullerRegionDebugDetail{}, false
	}
	region := debugLockedRangeStatistic(stat, now)
	if tracked, ok := s.debugTrackedRegion(subID, regionID); ok {
		applyTrackedRegion(&region, tracked)
	}
	return PullerRegionDebugDetail{
		SnapshotAt:     now,
		SubscriptionID: subID,
		KeyspaceID:     span.span.KeyspaceID,
		TableID:        span.span.TableID,
		Region:         region,
	}, true
}

func (s *subscriptionClient) debugSubscription(
	span *subscribedSpan,
	resolvedTs uint64,
	regionLimit int,
	now time.Time,
) PullerSubscriptionDebugInfo {
	info := PullerSubscriptionDebugInfo{
		SubscriptionID:      span.subID,
		KeyspaceID:          span.span.KeyspaceID,
		TableID:             span.span.TableID,
		ResolvedTs:          resolvedTs,
		ResolvedTsLagMillis: debugTsLag(resolvedTs, now),
		Initialized:         span.initialized.Load(),
		SlowRegions:         []PullerRegionDebugInfo{},
	}
	slowRegions := &pullerSlowRegionHeap{}
	stats := span.rangeLock.IterAll(func(regionID uint64, state *regionlock.LockedRangeState) {
		region := debugRegionInfo(regionID, state, now)
		if region.Initialized {
			info.InitializedRegions++
		} else {
			info.UninitializedRegions++
		}
		addSlowDebugRegion(slowRegions, region, regionLimit)
	})
	info.LockedRegions = stats.LockedRegionCount
	info.UncoveredRanges = len(stats.UnLockedRanges)
	info.SlowRegions = append(info.SlowRegions, (*slowRegions)...)
	sort.Slice(info.SlowRegions, func(i, j int) bool {
		if info.SlowRegions[i].ResolvedTs == info.SlowRegions[j].ResolvedTs {
			return info.SlowRegions[i].RegionID < info.SlowRegions[j].RegionID
		}
		return info.SlowRegions[i].ResolvedTs < info.SlowRegions[j].ResolvedTs
	})
	for i := range info.SlowRegions {
		if tracked, ok := s.debugTrackedRegion(span.subID, info.SlowRegions[i].RegionID); ok {
			applyTrackedRegion(&info.SlowRegions[i], tracked)
		}
	}
	return info
}

func (s *subscriptionClient) debugNow() time.Time {
	if s.spanRegistry != nil && s.spanRegistry.pdClock != nil {
		return s.spanRegistry.pdClock.CurrentTime()
	}
	return time.Now()
}

func (s *subscriptionClient) debugTrackedRegion(
	subID SubscriptionID,
	regionID uint64,
) (pullerTrackedRegion, bool) {
	if s.regionScheduler == nil {
		return pullerTrackedRegion{}, false
	}
	var result pullerTrackedRegion
	found := false
	s.regionScheduler.stores.Range(func(key, value any) bool {
		address := key.(string)
		for _, worker := range value.(*regionRequestStore).workers {
			initialized, requestCreatedAt, ok := worker.tracker.debugRegion(subID, regionID)
			if ok {
				result = pullerTrackedRegion{
					storeAddress:     address,
					workerID:         worker.workerID,
					initialized:      initialized,
					requestCreatedAt: requestCreatedAt,
				}
				found = true
				return false
			}
		}
		return true
	})
	return result, found
}

func addSlowDebugSubscription(
	subscriptions *pullerSlowSubscriptionHeap,
	subscription pullerSlowSubscription,
	limit int,
) {
	if subscriptions.Len() < limit {
		heap.Push(subscriptions, subscription)
		return
	}
	fastestSelected := (*subscriptions)[0]
	if subscription.resolvedTs > fastestSelected.resolvedTs ||
		subscription.resolvedTs == fastestSelected.resolvedTs &&
			subscription.span.subID >= fastestSelected.span.subID {
		return
	}
	heap.Pop(subscriptions)
	heap.Push(subscriptions, subscription)
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
		region.ResolvedTs == fastestSelected.ResolvedTs &&
			region.RegionID >= fastestSelected.RegionID {
		return
	}
	heap.Pop(regions)
	heap.Push(regions, region)
}

func debugRegionInfo(
	regionID uint64,
	state *regionlock.LockedRangeState,
	now time.Time,
) PullerRegionDebugInfo {
	resolvedTs := state.ResolvedTs.Load()
	return PullerRegionDebugInfo{
		RegionID:        regionID,
		ResolvedTs:      resolvedTs,
		ResolvedTsLagMs: debugTsLag(resolvedTs, now),
		Initialized:     state.Initialized.Load(),
		CreatedAt:       state.Created,
		AgeMillis:       max(int64(0), now.Sub(state.Created).Milliseconds()),
		Phase:           "scheduling_or_recovering",
	}
}

func debugLockedRangeStatistic(
	stat regionlock.LockedRangeStatistic,
	now time.Time,
) PullerRegionDebugInfo {
	return PullerRegionDebugInfo{
		RegionID:        stat.RegionID,
		ResolvedTs:      stat.ResolvedTs,
		ResolvedTsLagMs: debugTsLag(stat.ResolvedTs, now),
		Initialized:     stat.Initialized,
		CreatedAt:       stat.Created,
		AgeMillis:       max(int64(0), now.Sub(stat.Created).Milliseconds()),
		Phase:           "scheduling_or_recovering",
	}
}

func debugTsLag(ts uint64, now time.Time) int64 {
	if ts == 0 {
		return 0
	}
	return max(int64(0), now.Sub(oracle.GetTimeFromTS(ts)).Milliseconds())
}

func applyTrackedRegion(region *PullerRegionDebugInfo, tracked pullerTrackedRegion) {
	region.StoreAddress = tracked.storeAddress
	region.WorkerID = tracked.workerID
	region.RequestCreatedAt = tracked.requestCreatedAt
	if tracked.initialized {
		region.Phase = "streaming"
	} else {
		region.Phase = "waiting_tikv_initial_scan"
	}
}

func (t *regionTracker) debugRegion(
	subID SubscriptionID,
	regionID uint64,
) (initialized bool, requestCreatedAt *time.Time, found bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	state, ok := t.statesBySubscription[subID][regionID]
	if !ok {
		return false, nil, false
	}
	if request := state.regionReq.Load(); request != nil {
		createdAt := request.createTime
		requestCreatedAt = &createdAt
	}
	return state.isInitialized(), requestCreatedAt, true
}
