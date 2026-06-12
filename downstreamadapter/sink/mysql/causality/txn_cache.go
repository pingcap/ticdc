// Copyright 2024 PingCAP, Inc.
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
	"sync/atomic"

	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/utils/chann"
)

// Barrier is an in-band DML writer control marker. It is acknowledged by each
// writer only after all earlier DMLs in that writer queue have flushed.
type Barrier interface {
	Ack(writerID int)
	Fail(err error)
	OnDone(func())
}

// WriterItem is the value carried by MySQL DML writer queues.
type WriterItem struct {
	DML     *commonEvent.DMLEvent
	Barrier Barrier
}

// NewDMLItem creates a writer queue item for a DML event.
func NewDMLItem(event *commonEvent.DMLEvent) WriterItem {
	return WriterItem{DML: event}
}

// NewBarrierItem creates a writer queue item for a barrier token.
func NewBarrierItem(barrier Barrier) WriterItem {
	return WriterItem{Barrier: barrier}
}

const (
	// BlockStrategyWaitAvailable means the cache will block until there is an available slot.
	BlockStrategyWaitAvailable BlockStrategy = "waitAvailable"
	// BlockStrategyWaitEmpty means the cache will block until all cached txns are consumed.
	BlockStrategyWaitEmpty = "waitEmpty"
	// TODO: maybe we can implement a strategy that can automatically adapt to different scenarios
)

// BlockStrategy is the strategy to handle the situation when the cache is full.
type BlockStrategy string

// TxnCacheOption is the option for creating a cache for resolved txns.
type TxnCacheOption struct {
	// Count controls the number of caches, txns in different caches could be executed concurrently.
	Count int
	// Size controls the max number of txns a cache can hold.
	Size int
	// BlockStrategy controls the strategy when the cache is full.
	BlockStrategy BlockStrategy
}

// In current implementation, the conflict detector will push txn to the txnCache.
type txnCache interface {
	// add adds a event to the Cache.
	add(item WriterItem) bool
	// forceAdd appends a control item even when the cache is blocked, but fails
	// if the underlying channel has already been closed.
	forceAdd(item WriterItem) bool
	// out returns a unlimited channel to receive events which are ready to be executed.
	out() *chann.UnlimitedChannel[WriterItem, any]
}

func newTxnCache(opt TxnCacheOption) txnCache {
	if opt.Size <= 0 {
		log.Panic("WorkerOption.CacheSize should be greater than 0, please report a bug")
	}

	switch opt.BlockStrategy {
	case BlockStrategyWaitAvailable:
		return &boundedTxnCache{ch: chann.NewUnlimitedChannel[WriterItem, any](nil, nil), upperSize: opt.Size}
	case BlockStrategyWaitEmpty:
		return &boundedTxnCacheWithBlock{ch: chann.NewUnlimitedChannel[WriterItem, any](nil, nil), upperSize: opt.Size}
	default:
		return nil
	}
}

// boundedTxnCache is a cache which has a limit on the number of txns it can hold.
//
//nolint:unused
type boundedTxnCache struct {
	ch        *chann.UnlimitedChannel[WriterItem, any]
	upperSize int
}

//nolint:unused
func (w *boundedTxnCache) add(item WriterItem) bool {
	if w.ch.Len() > w.upperSize {
		return false
	}
	return w.ch.PushIfNotClosed(item)
}

//nolint:unused
func (w *boundedTxnCache) forceAdd(item WriterItem) bool {
	return w.ch.PushIfNotClosed(item)
}

//nolint:unused
func (w *boundedTxnCache) out() *chann.UnlimitedChannel[WriterItem, any] {
	return w.ch
}

// boundedTxnCacheWithBlock is a special boundedWorker. Once the cache
// is full, it will block until all cached txns are consumed.
type boundedTxnCacheWithBlock struct {
	ch *chann.UnlimitedChannel[WriterItem, any]
	//nolint:unused
	isBlocked atomic.Bool
	upperSize int
}

//nolint:unused
func (w *boundedTxnCacheWithBlock) add(item WriterItem) bool {
	if w.isBlocked.Load() && w.ch.Len() <= 0 {
		w.isBlocked.Store(false)
	}

	if !w.isBlocked.Load() {
		if w.ch.Len() > w.upperSize {
			w.isBlocked.CompareAndSwap(false, true)
			return false
		}
		return w.ch.PushIfNotClosed(item)
	}
	return false
}

//nolint:unused
func (w *boundedTxnCacheWithBlock) forceAdd(item WriterItem) bool {
	return w.ch.PushIfNotClosed(item)
}

//nolint:unused
func (w *boundedTxnCacheWithBlock) out() *chann.UnlimitedChannel[WriterItem, any] {
	return w.ch
}
