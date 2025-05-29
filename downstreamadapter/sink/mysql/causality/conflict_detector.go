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

package causality

import (
	"context"

	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
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
	slots *Slots

	// nextCacheID is used to dispatch transactions round-robin.
	nextCacheID atomic.Int64

	notifiedNodes *chann.DrainableChann[func()]

	closed atomic.Bool
}

// New creates a new ConflictDetector.
func New(
	numSlots uint64, opt TxnCacheOption,
) *ConflictDetector {
	ret := &ConflictDetector{
		resolvedTxnCaches: make([]txnCache, opt.Count),
		slots:             NewSlots(numSlots),
		notifiedNodes:     chann.NewAutoDrainChann[func()](),
	}
	for i := 0; i < opt.Count; i++ {
		ret.resolvedTxnCaches[i] = newTxnCache(opt)
	}
	log.Info("conflict detector initialized", zap.Int("cacheCount", opt.Count),
		zap.Int("cacheSize", opt.Size), zap.String("BlockStrategy", string(opt.BlockStrategy)))
	return ret
}

func (d *ConflictDetector) Run(ctx context.Context) error {
	defer d.Close()
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
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
func (d *ConflictDetector) Add(event *commonEvent.DMLEvent) {
	if d.closed.Load() {
		return
	}
	hashes := ConflictKeys(event)
	node := d.slots.AllocNode(hashes)

	event.AddPostFlushFunc(func() {
		d.slots.Remove(node)
	})

	node.TrySendToTxnCache = func(cacheID int64) bool {
		// Try sending this txn to related cache as soon as all dependencies are resolved.
		return d.sendToCache(event, cacheID)
	}
	node.RandCacheID = func() int64 {
		return d.nextCacheID.Add(1) % int64(len(d.resolvedTxnCaches))
	}
	node.OnNotified = func(callback func()) {
		d.notifiedNodes.In() <- callback
	}
	d.slots.Add(node)
}

// sendToCache should not call txn.Callback if it returns an error.
func (d *ConflictDetector) sendToCache(event *commonEvent.DMLEvent, id int64) bool {
	if d.closed.Load() {
		return false
	}
	cache := d.resolvedTxnCaches[id]
	ok := cache.add(event)
	return ok
}

// GetOutChByCacheID returns the output channel by cacheID.
// Note txns in single cache should be executed sequentially.
func (d *ConflictDetector) GetOutChByCacheID(id int) *chann.UnlimitedChannel[*commonEvent.DMLEvent, any] {
	return d.resolvedTxnCaches[id].out()
}

func (d *ConflictDetector) Close() {
	if d.closed.Load() {
		return
	}
	d.notifiedNodes.CloseAndDrain()
	for _, cache := range d.resolvedTxnCaches { // the unlimited channel should be closed, otherwise dmlWriter will be blocked
		cache.out().Close()
	}
	d.closed.Store(true)
}
