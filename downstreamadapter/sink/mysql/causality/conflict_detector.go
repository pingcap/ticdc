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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
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

	notifiedNodes *chann.UnlimitedChannel[func(), any]

	changefeedID                 common.ChangeFeedID
	metricConflictDetectDuration prometheus.Observer
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
	node.OnNotified = func(callback func()) {
		// TODO:find a better way to handle the panic
		defer func() {
			if r := recover(); r != nil {
				log.Warn("failed to send notification, channel might be closed", zap.Any("error", r))
			}
		}()
		d.notifiedNodes.Push(callback)
	}
	d.slots.Add(node)
}

// sendToCache should not call txn.Callback if it returns an error.
func (d *ConflictDetector) sendToCache(event *commonEvent.DMLEvent, id int64) bool {
	cache := d.resolvedTxnCaches[id]
	ok := cache.add(event)
	return ok
}

// GetOutChByCacheID returns the output channel by cacheID.
// Note txns in single cache should be executed sequentially.
func (d *ConflictDetector) GetOutChByCacheID(id int) *chann.UnlimitedChannel[*commonEvent.DMLEvent, any] {
	return d.resolvedTxnCaches[id].out()
}

func (d *ConflictDetector) closeCache() {
	// the unlimited channel should be closed when quit wait group, otherwise dmlWriter will be blocked
	for _, cache := range d.resolvedTxnCaches {
		cache.out().Close()
	}
}

func (d *ConflictDetector) CloseNotifiedNodes() {
	d.notifiedNodes.Close()
}

func (d *ConflictDetector) Close() {
	d.CloseNotifiedNodes()
}
