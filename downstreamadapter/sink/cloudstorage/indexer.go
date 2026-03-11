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
	"sync/atomic"

	"github.com/pingcap/ticdc/pkg/common"
)

type indexer struct {
	inputShards int
	nextInput   uint64

	outputShards int
}

// newIndexer builds the routing policy used by storage sink task pipeline.
//
// Invariants:
// 1. Input index only affects encoder parallelism (round-robin).
// 2. Output index is stable per dispatcher to preserve per-dispatcher ordering.
func newIndexer(inputShards, outputShards int) *indexer {
	if inputShards <= 0 {
		inputShards = 1
	}
	if outputShards <= 0 {
		outputShards = 1
	}

	return &indexer{
		inputShards:  inputShards,
		outputShards: outputShards,
	}
}

func (r *indexer) next(dispatcherID common.DispatcherID) (int, int) {
	return r.nextInputIndex(), r.routeOutputIndex(dispatcherID)
}

// nextInputIndex uses round-robin so hot dispatchers do not pin a single encoder shard.
func (r *indexer) nextInputIndex() int {
	if r.inputShards <= 1 {
		return 0
	}
	// AddUint64 returns the incremented value, so subtract 1 to get a zero-based sequence.
	next := atomic.AddUint64(&r.nextInput, 1)
	return int((next - 1) % uint64(r.inputShards))
}

func (r *indexer) routeOutputIndex(dispatcherID common.DispatcherID) int {
	if r.outputShards <= 1 {
		return 0
	}
	return common.GID(dispatcherID).Hash(uint64(r.outputShards))
}
