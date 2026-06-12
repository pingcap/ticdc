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
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/prometheus/client_golang/prometheus"
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

	notifiedNodes        *chann.UnlimitedChannel[func(), any]
	notifyGuardWaitGroup util.GuardedWaitGroup
	notifyClosed         atomic.Bool

	changefeedID                 common.ChangeFeedID
	metricConflictDetectDuration prometheus.Observer

	admissionMu sync.Mutex
	activeFence *Node
}

// New creates a new ConflictDetector.
func New(
	numSlots uint64, opt TxnCacheOption, changefeedID common.ChangeFeedID,
) *ConflictDetector {
	ret := &ConflictDetector{
		resolvedTxnCaches:            make([]txnCache, opt.Count),
		slots:                        NewSlots(numSlots),
		notifiedNodes:                chann.NewUnlimitedChannelDefault[func()](),
		metricConflictDetectDuration: metrics.ConflictDetectDuration.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name()),

		changefeedID: changefeedID,
	}
	for i := 0; i < opt.Count; i++ {
		ret.resolvedTxnCaches[i] = newTxnCache(opt)
	}
	log.Info("conflict detector initialized", zap.Int("cacheCount", opt.Count),
		zap.Int("cacheSize", opt.Size), zap.String("BlockStrategy", string(opt.BlockStrategy)))
	return ret
}

func (d *ConflictDetector) Run(ctx context.Context) error {
	defer func() {
		metrics.ConflictDetectDuration.DeleteLabelValues(d.changefeedID.Keyspace(), d.changefeedID.Name())
		d.closeCache()
	}()

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		default:
			if notifyCallback, ok := d.notifiedNodes.Get(); ok {
				notifyCallback()
			} else {
				log.Info("notifiedNodes is closed, return")
				return nil
			}
		}
	}
}

// Add pushes a transaction to the ConflictDetector.
//
// NOTE: if multiple threads access this concurrently,
// ConflictKeys must be sorted by the slot index.
func (d *ConflictDetector) Add(event *commonEvent.DMLEvent) {
	d.admissionMu.Lock()
	defer d.admissionMu.Unlock()

	start := time.Now()
	hashes := ConflictKeys(event)
	node := d.slots.AllocNode(hashes)

	event.AddPostFlushFunc(func() {
		d.slots.Remove(node)
	})

	node.TrySendToTxnCache = func(cacheID int64) bool {
		// Try sending this txn to related cache as soon as all dependencies are resolved.
		ok := d.sendToCache(event, cacheID)
		if ok {
			d.metricConflictDetectDuration.Observe(time.Since(start).Seconds())
		}
		return ok
	}
	node.RandCacheID = func() int64 {
		return d.nextCacheID.Add(1) % int64(len(d.resolvedTxnCaches))
	}
	node.OnNotified = d.onNodeNotified
	extraDependencies := d.activeFenceDependency()
	d.slots.AddWithDependencies(node, extraDependencies)
}

// sendToCache should not call txn.Callback if it returns an error.
func (d *ConflictDetector) sendToCache(event *commonEvent.DMLEvent, id int64) bool {
	cache := d.resolvedTxnCaches[id]
	ok := cache.add(NewDMLItem(event))
	return ok
}

// BroadcastBarrier installs a removal-only fence after all DMLs admitted so far
// and broadcasts one barrier token to every writer queue after that fence resolves.
func (d *ConflictDetector) BroadcastBarrier(barrier Barrier) error {
	if d.notifyClosed.Load() {
		return errors.ErrMySQLTxnError.GenWithStackByArgs("broadcast barrier on closed conflict detector")
	}

	d.admissionMu.Lock()

	dependencyNodes := make(map[int64]*Node)
	for _, node := range d.slots.SnapshotTailNodes() {
		dependencyNodes[node.nodeID()] = node
	}
	for id, node := range d.activeFenceDependency() {
		dependencyNodes[id] = node
	}

	fence := &Node{
		id:                   genNextNodeID(),
		assignedTo:           unassigned,
		resolveByRemovalOnly: true,
	}
	fence.TrySendToTxnCache = func(cacheID) bool {
		item := NewBarrierItem(barrier)
		for _, cache := range d.resolvedTxnCaches {
			if !cache.forceAdd(item) {
				err := errors.ErrMySQLTxnError.GenWithStackByArgs("broadcast barrier to closed DML writer queue")
				go barrier.Fail(err)
				return true
			}
		}
		return true
	}
	fence.RandCacheID = func() cacheID { return 0 }
	fence.OnNotified = d.onNodeNotified

	d.activeFence = fence
	fence.dependOn(dependencyNodes)
	d.admissionMu.Unlock()

	barrier.OnDone(func() {
		d.admissionMu.Lock()
		if d.activeFence == fence {
			d.activeFence = nil
		}
		d.admissionMu.Unlock()
		fence.remove()
	})
	return nil
}

func (d *ConflictDetector) activeFenceDependency() map[int64]*Node {
	if d.activeFence == nil {
		return nil
	}
	return map[int64]*Node{d.activeFence.nodeID(): d.activeFence}
}

func (d *ConflictDetector) onNodeNotified(callback func()) {
	if !d.notifyGuardWaitGroup.AddIf(func() bool { return !d.notifyClosed.Load() }) {
		return
	}
	defer d.notifyGuardWaitGroup.Done()

	d.notifiedNodes.Push(callback)
}

// GetOutChByCacheID returns the output channel by cacheID.
// Note txns in single cache should be executed sequentially.
func (d *ConflictDetector) GetOutChByCacheID(id int) *chann.UnlimitedChannel[WriterItem, any] {
	return d.resolvedTxnCaches[id].out()
}

func (d *ConflictDetector) closeCache() {
	// the unlimited channel should be closed when quit wait group, otherwise dmlWriter will be blocked
	for _, cache := range d.resolvedTxnCaches {
		cache.out().Close()
	}
}

func (d *ConflictDetector) CloseNotifiedNodes() {
	if d.notifyClosed.CompareAndSwap(false, true) {
		d.notifyGuardWaitGroup.Wait()
		d.notifiedNodes.Close()
	}
}

func (d *ConflictDetector) Close() {
	d.CloseNotifiedNodes()
}
