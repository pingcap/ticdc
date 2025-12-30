// Copyright 2025 PingCAP, Inc.
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
	"unsafe"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/utils/dynstream"
	"go.uber.org/zap"
)

type subscriptionEvent struct {
	subID SubscriptionID

	// only one of the following fields will be set
	kvEntries  []common.RawKVEntry
	resolvedTs uint64
}

func (e subscriptionEvent) size() int {
	size := int(unsafe.Sizeof(e))
	if len(e.kvEntries) > 0 {
		size += int(unsafe.Sizeof(e.kvEntries))
		for i := range e.kvEntries {
			size += int(unsafe.Sizeof(e.kvEntries[i]))
			size += len(e.kvEntries[i].Key)
			size += len(e.kvEntries[i].Value)
			size += len(e.kvEntries[i].OldValue)
		}
	}
	return size
}

type subscriptionEventHandler struct {
	wake func(SubscriptionID)
}

func (h *subscriptionEventHandler) Path(event subscriptionEvent) SubscriptionID {
	return event.subID
}

func (h *subscriptionEventHandler) Handle(span *subscribedSpan, events ...subscriptionEvent) bool {
	if len(span.pendingKVEntries) != 0 {
		log.Panic("pendingKVEntries is not empty",
			zap.Int("pendingKVEntriesLen", len(span.pendingKVEntries)),
			zap.Uint64("subscriptionID", uint64(span.subID)))
	}

	var maxResolvedTs uint64
	for i := range events {
		if len(events[i].kvEntries) != 0 {
			span.pendingKVEntries = append(span.pendingKVEntries, events[i].kvEntries...)
		} else if events[i].resolvedTs != 0 {
			if events[i].resolvedTs > maxResolvedTs {
				maxResolvedTs = events[i].resolvedTs
			}
		} else {
			log.Panic("unknown subscription event", zap.Any("event", events[i]))
		}
	}

	advanceResolvedTs := func() {
		if maxResolvedTs != 0 {
			span.advanceResolvedTs(maxResolvedTs)
		}
	}

	if len(span.pendingKVEntries) == 0 {
		advanceResolvedTs()
		return false
	}

	metricsEventCount.Add(float64(len(span.pendingKVEntries)))
	await := span.consumeKVEvents(span.pendingKVEntries, func() {
		span.clearPendingKVEntries()
		advanceResolvedTs()
		h.wake(span.subID)
	})
	if !await {
		span.clearPendingKVEntries()
		advanceResolvedTs()
	}
	return await
}

func (h *subscriptionEventHandler) GetSize(event subscriptionEvent) int { return event.size() }

func (h *subscriptionEventHandler) GetArea(path SubscriptionID, dest *subscribedSpan) int { return 0 }

func (h *subscriptionEventHandler) GetTimestamp(event subscriptionEvent) dynstream.Timestamp {
	if event.resolvedTs != 0 {
		return dynstream.Timestamp(event.resolvedTs)
	}
	if len(event.kvEntries) != 0 {
		return dynstream.Timestamp(event.kvEntries[0].CRTs)
	}
	return 0
}

func (h *subscriptionEventHandler) IsPaused(event subscriptionEvent) bool { return false }

func (h *subscriptionEventHandler) GetType(event subscriptionEvent) dynstream.EventType {
	if len(event.kvEntries) != 0 {
		return dynstream.EventType{DataGroup: 1, Property: dynstream.BatchableData}
	}
	if event.resolvedTs != 0 {
		return dynstream.EventType{DataGroup: 2, Property: dynstream.PeriodicSignal}
	}
	log.Panic("unknown subscription event type", zap.Any("event", event))
	return dynstream.DefaultEventType
}

func (h *subscriptionEventHandler) OnDrop(event subscriptionEvent) interface{} { return nil }

var _ dynstream.Handler[int, SubscriptionID, subscriptionEvent, *subscribedSpan] = (*subscriptionEventHandler)(nil)
