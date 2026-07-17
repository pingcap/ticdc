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
	"context"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

// requestedStore owns the request workers connected to one TiKV store. The
// worker slice is complete before the store is published and is immutable
// afterwards, so task submission only needs an atomic round-robin counter.
type requestedStore struct {
	storeAddr  string
	workers    []*regionRequestWorker
	nextWorker atomic.Uint64
}

func newRequestedStore(
	client *subscriptionClient,
	storeAddr string,
	workerCount int,
	workerWindow int,
	maxWindowMultiplier int,
) *requestedStore {
	store := &requestedStore{
		storeAddr: storeAddr,
		workers:   make([]*regionRequestWorker, 0, workerCount),
	}
	for i := 0; i < workerCount; i++ {
		store.workers = append(store.workers, newRegionRequestWorker(
			client, store, workerWindow, maxWindowMultiplier))
	}
	return store
}

func (s *requestedStore) run(ctx context.Context, group *errgroup.Group) {
	for _, worker := range s.workers {
		group.Go(func() error { return worker.Run(ctx) })
	}
}

func (s *requestedStore) submit(task *regionPriorityTask) bool {
	if len(s.workers) == 0 {
		return false
	}
	index := (s.nextWorker.Add(1) - 1) % uint64(len(s.workers))
	return s.workers[index].admission.submit(task)
}

func (s *requestedStore) broadcastDeregister(subID SubscriptionID, filterLoop bool) {
	for _, worker := range s.workers {
		worker.controlQueue.push(deregisterRequest{subID: subID, filterLoop: filterLoop})
	}
}

func (s *requestedStore) close() {
	for _, worker := range s.workers {
		worker.admission.close()
	}
}

func (s *requestedStore) inflightCount() int {
	count := 0
	for _, worker := range s.workers {
		count += worker.admission.stats().inflight
	}
	return count
}
