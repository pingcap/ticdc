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

type batchResolvedTsEntry struct {
	state      *regionFeedState
	resolvedTs uint64
}

// batchResolvedTsEvent groups multiple resolved ts updates of the same subscription.
type batchResolvedTsEvent struct {
	subscriptionID SubscriptionID
	entries        []batchResolvedTsEntry
}

func newBatchResolvedTsEvent(subID SubscriptionID, capacity int) *batchResolvedTsEvent {
	return &batchResolvedTsEvent{
		subscriptionID: subID,
		entries:        make([]batchResolvedTsEntry, 0, capacity),
	}
}

func (b *batchResolvedTsEvent) add(state *regionFeedState, resolvedTs uint64) {
	if state == nil {
		return
	}
	b.entries = append(b.entries, batchResolvedTsEntry{
		state:      state,
		resolvedTs: resolvedTs,
	})
}

func (b *batchResolvedTsEvent) len() int {
	return len(b.entries)
}

func (b *batchResolvedTsEvent) minResolvedTs() uint64 {
	if len(b.entries) == 0 {
		return 0
	}
	min := b.entries[0].resolvedTs
	for _, entry := range b.entries[1:] {
		if entry.resolvedTs < min {
			min = entry.resolvedTs
		}
	}
	return min
}
