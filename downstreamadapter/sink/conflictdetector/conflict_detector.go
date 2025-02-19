// Copyright 2022 PingCAP, Inc.
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

package conflictdetector

import (
	"sync"

	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/utils/chann"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// ConflictDetector implements a logic that dispatches transaction
// to different worker cache channels in a way that transactions
// modifying the same keys are never executed concurrently and
// have their original orders preserved. Transactions in different
// channels can be executed concurrently.
type ConflictDetector struct {
	// resolvedTxnCaches are used to cache resolved transactions.
	resolvedTxnCaches []txnCache

	// slots are used to find all unfinished transactions
	// conflicting with an incoming transactions.
	slots    *Slots
	numSlots uint64

	// nextCacheID is used to dispatch transactions round-robin.
	nextCacheID atomic.Int64

	closeCh chan struct{}

	notifiedNodes *chann.DrainableChann[func()]
	wg            sync.WaitGroup
}

// NewConflictDetector creates a new ConflictDetector.
func NewConflictDetector(
	numSlots uint64, opt TxnCacheOption,
) *ConflictDetector {
	ret := &ConflictDetector{
		resolvedTxnCaches: make([]txnCache, opt.Count),
		slots:             NewSlots(numSlots),
		numSlots:          numSlots,
		closeCh:           make(chan struct{}),
		notifiedNodes:     chann.NewAutoDrainChann[func()](),
	}
	for i := 0; i < opt.Count; i++ {
		ret.resolvedTxnCaches[i] = newTxnCache(opt)
	}

	ret.wg.Add(1)
	go func() {
		defer ret.wg.Done()
		ret.runBackgroundTasks()
	}()

	return ret
}

func (d *ConflictDetector) runBackgroundTasks() {
	defer func() {
		d.notifiedNodes.CloseAndDrain()
	}()
	for {
		select {
		case <-d.closeCh:
			return
		case notifyCallback := <-d.notifiedNodes.Out():
			if notifyCallback != nil {
				notifyCallback()
			}
		}
	}
}

// Add pushes a transaction to the ConflictDetector.
//
// NOTE: if multiple threads access this concurrently,
// ConflictKeys must be sorted by the slot index.
func (d *ConflictDetector) Add(event *commonEvent.DMLEvent) error {
	hashes, err := ConflictKeys(event)
	if err != nil {
		return err
	}
	node := d.slots.AllocNode(hashes)

	event.AddPostFlushFunc(func() {
		d.slots.Remove(node)
	})

	node.TrySendToTxnCache = func(cacheID int64) bool {
		// Try sending this txn to related cache as soon as all dependencies are resolved.
		return d.sendToCache(event, cacheID)
	}
	node.RandCacheID = func() int64 { return d.nextCacheID.Add(1) % int64(len(d.resolvedTxnCaches)) }
	node.OnNotified = func(callback func()) { d.notifiedNodes.In() <- callback }
	d.slots.Add(node)

	return nil
}

// Close closes the ConflictDetector.
func (d *ConflictDetector) Close() {
	close(d.closeCh)
}

// sendToCache should not call txn.Callback if it returns an error.
func (d *ConflictDetector) sendToCache(event *commonEvent.DMLEvent, id int64) bool {
	if id < 0 {
		log.Panic("must assign with a valid cacheID", zap.Int64("cacheID", id))
	}

	cache := d.resolvedTxnCaches[id]
	ok := cache.add(event)
	return ok
}

// GetOutChByCacheID returns the output channel by cacheID.
// Note txns in single cache should be executed sequentially.
func (d *ConflictDetector) GetOutChByCacheID(id int64) <-chan *commonEvent.DMLEvent {
	if id < 0 {
		log.Panic("must assign with a valid cacheID", zap.Int64("cacheID", id))
	}
	return d.resolvedTxnCaches[id].out()
}
