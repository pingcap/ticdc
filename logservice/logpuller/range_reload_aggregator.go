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
	"sort"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"go.uber.org/zap"
)

type rangeReloadTaskSet struct {
	subscribedSpan *subscribedSpan
	filterLoop     bool
	priority       TaskType
	spans          []heartbeatpb.TableSpan
	flushScheduled bool
}

type rangeReloadAggregator struct {
	client         *subscriptionClient
	debounce       time.Duration
	flushThreshold int

	mu    sync.Mutex
	tasks map[SubscriptionID]*rangeReloadTaskSet
}

func newRangeReloadAggregator(client *subscriptionClient, debounce time.Duration, flushThreshold int) *rangeReloadAggregator {
	if flushThreshold <= 0 {
		flushThreshold = rangeReloadImmediateFlushMax
	}
	return &rangeReloadAggregator{
		client:         client,
		debounce:       debounce,
		flushThreshold: flushThreshold,
		tasks:          make(map[SubscriptionID]*rangeReloadTaskSet),
	}
}

func (a *rangeReloadAggregator) add(
	span heartbeatpb.TableSpan,
	subscribedSpan *subscribedSpan,
	filterLoop bool,
	priority TaskType,
) {
	if subscribedSpan == nil || common.IsEmptySpan(span) {
		return
	}
	var immediateFlush bool
	a.mu.Lock()
	set := a.tasks[subscribedSpan.subID]
	if set == nil {
		set = &rangeReloadTaskSet{
			subscribedSpan: subscribedSpan,
			filterLoop:     filterLoop,
			priority:       priority,
			spans:          make([]heartbeatpb.TableSpan, 0, 4),
		}
		a.tasks[subscribedSpan.subID] = set
	}
	if priority < set.priority {
		set.priority = priority
	}
	set.spans = append(set.spans, span)
	if len(set.spans) >= a.flushThreshold {
		immediateFlush = true
		set.flushScheduled = false
	} else if !set.flushScheduled {
		set.flushScheduled = true
		a.client.retryScheduler.schedule(a.debounce, func(_ context.Context) {
			a.flush(subscribedSpan.subID)
		})
	}
	a.mu.Unlock()

	if immediateFlush {
		a.flush(subscribedSpan.subID)
	}
}

func (a *rangeReloadAggregator) flush(subID SubscriptionID) {
	var (
		set         *rangeReloadTaskSet
		mergedSpans []heartbeatpb.TableSpan
	)
	a.mu.Lock()
	set = a.tasks[subID]
	if set == nil {
		a.mu.Unlock()
		return
	}
	delete(a.tasks, subID)
	set.flushScheduled = false
	mergedSpans = mergeTableSpans(set.spans)
	a.mu.Unlock()

	if set.subscribedSpan.stopped.Load() {
		return
	}
	log.Debug("subscription client flush aggregated reload spans",
		zap.Uint64("subscriptionID", uint64(subID)),
		zap.Int("inputSpanCount", len(set.spans)),
		zap.Int("mergedSpanCount", len(mergedSpans)))
	for _, span := range mergedSpans {
		a.client.scheduleRangeTaskAfter(rangeTask{
			span:           span,
			subscribedSpan: set.subscribedSpan,
			filterLoop:     set.filterLoop,
			priority:       set.priority,
		}, 0)
	}
}

func mergeTableSpans(spans []heartbeatpb.TableSpan) []heartbeatpb.TableSpan {
	if len(spans) <= 1 {
		return append([]heartbeatpb.TableSpan(nil), spans...)
	}
	copied := append([]heartbeatpb.TableSpan(nil), spans...)
	sort.Slice(copied, func(i, j int) bool {
		return common.StartCompare(copied[i].StartKey, copied[j].StartKey) < 0
	})

	merged := make([]heartbeatpb.TableSpan, 0, len(copied))
	for _, span := range copied {
		if common.IsEmptySpan(span) {
			continue
		}
		if len(merged) == 0 {
			merged = append(merged, span)
			continue
		}
		last := merged[len(merged)-1]
		if common.EndCompare(last.EndKey, span.StartKey) >= 0 {
			if common.EndCompare(span.EndKey, last.EndKey) > 0 {
				last.EndKey = span.EndKey
			}
			merged[len(merged)-1] = last
			continue
		}
		merged = append(merged, span)
	}
	return merged
}
