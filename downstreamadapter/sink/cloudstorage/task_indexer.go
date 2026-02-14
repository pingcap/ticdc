// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cloudstorage

import (
	"sync"
	"sync/atomic"

	commonType "github.com/pingcap/ticdc/pkg/common"
)

type taskIndexer struct {
	inputShards  int
	outputShards int

	nextInput uint64

	mu                sync.RWMutex
	dispatcherToShard map[commonType.DispatcherID]int
}

// newTaskIndexer builds the routing policy used by storage sink task pipeline.
//
// Invariants:
// 1. Input index only affects encoder parallelism (round-robin).
// 2. Output index is stable per dispatcher to preserve per-dispatcher ordering.
func newTaskIndexer(inputShards, outputShards int) *taskIndexer {
	if inputShards <= 0 {
		inputShards = 1
	}
	if outputShards <= 0 {
		outputShards = 1
	}

	return &taskIndexer{
		inputShards:       inputShards,
		outputShards:      outputShards,
		dispatcherToShard: make(map[commonType.DispatcherID]int),
	}
}

func (r *taskIndexer) next(dispatcherID commonType.DispatcherID) (int, int) {
	return r.nextInputIndex(), r.routeOutputIndex(dispatcherID)
}

// nextInputIndex uses round-robin so hot dispatchers do not pin a single encoder shard.
func (r *taskIndexer) nextInputIndex() int {
	if r.inputShards <= 1 {
		return 0
	}
	next := atomic.AddUint64(&r.nextInput, 1)
	return int((next - 1) % uint64(r.inputShards))
}

func (r *taskIndexer) routeOutputIndex(dispatcherID commonType.DispatcherID) int {
	if r.outputShards <= 1 {
		return 0
	}

	r.mu.RLock()
	index, ok := r.dispatcherToShard[dispatcherID]
	r.mu.RUnlock()
	if ok {
		return index
	}

	// We compute hash once and then cache it.
	// Principle: stable routing + low per-task overhead.
	index = commonType.GID(dispatcherID).Hash(uint64(r.outputShards))

	r.mu.Lock()
	if cached, exists := r.dispatcherToShard[dispatcherID]; exists {
		r.mu.Unlock()
		return cached
	}
	r.dispatcherToShard[dispatcherID] = index
	r.mu.Unlock()
	return index
}

func (r *taskIndexer) cachedOutputCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.dispatcherToShard)
}
