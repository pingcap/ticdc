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

package eventstore

import (
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller"
	"github.com/pingcap/ticdc/logservice/logservicepb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	CounterKv       = metrics.EventStoreReceivedEventCount.WithLabelValues("kv")
	CounterResolved = metrics.EventStoreReceivedEventCount.WithLabelValues("resolved")
)

var (
	metricEventStoreFirstReadDurationHistogram = metrics.EventStoreReadDurationHistogram.WithLabelValues("first")
	metricEventStoreNextReadDurationHistogram  = metrics.EventStoreReadDurationHistogram.WithLabelValues("next")
	metricEventStoreCloseReadDurationHistogram = metrics.EventStoreReadDurationHistogram.WithLabelValues("close")
)

type ResolvedTsNotifier func(watermark uint64, latestCommitTs uint64)

type EventStore interface {
	Name() string

	Run(ctx context.Context) error

	Close(ctx context.Context) error

	RegisterDispatcher(
		dispatcherID common.DispatcherID,
		span *heartbeatpb.TableSpan,
		startTS uint64,
		notifier ResolvedTsNotifier,
		onlyReuse bool,
		bdrMode bool,
	) (bool, error)

	UnregisterDispatcher(dispatcherID common.DispatcherID) error

	// TODO: Implement this after checkpointTs is correctly reported by the downstream dispatcher.
	UpdateDispatcherCheckpointTs(dispatcherID common.DispatcherID, checkpointTs uint64) error

	GetDispatcherDMLEventState(dispatcherID common.DispatcherID) (bool, DMLEventState)

	// return an iterator which scan the data in ts range (dataRange.StartTs, dataRange.EndTs]
	GetIterator(dispatcherID common.DispatcherID, dataRange common.DataRange) (EventIterator, error)
}

type DMLEventState struct {
	// ResolvedTs       uint64
	// The max commit ts of dml event in the store.
	MaxEventCommitTs uint64
}

type EventIterator interface {
	Next() (*common.RawKVEntry, bool, error)

	// Close closes the iterator.
	// It returns the number of events that are read from the iterator.
	Close() (eventCnt int64, err error)
}

type dispatcherStat struct {
	dispatcherID common.DispatcherID

	tableSpan *heartbeatpb.TableSpan
	// the max ts of events which is not needed by this dispatcher
	checkpointTs uint64

	subID logpuller.SubscriptionID
}

type subscriptionStat struct {
	subID logpuller.SubscriptionID

	tableID int64

	// dispatchers depend on this subscription
	dispatchers struct {
		sync.Mutex
		notifiers map[common.DispatcherID]ResolvedTsNotifier
	}

	dbIndex int

	eventCh *chann.UnlimitedChannel[eventWithCallback, uint64]
	// data <= checkpointTs can be deleted
	checkpointTs atomic.Uint64
	// the resolveTs persisted in the store
	resolvedTs atomic.Uint64
	// the max commit ts of dml event in the store
	maxEventCommitTs atomic.Uint64
}

type eventWithCallback struct {
	subID    logpuller.SubscriptionID
	tableID  int64
	kvs      []common.RawKVEntry
	callback func()
}

func eventWithCallbackSizer(e eventWithCallback) int {
	size := 0
	for _, e := range e.kvs {
		size += int(e.KeyLen + e.ValueLen + e.OldValueLen)
	}
	return size
}

type eventStore struct {
	pdClock   pdutil.Clock
	subClient *logpuller.SubscriptionClient

	dbs            []*pebble.DB
	chs            []*chann.UnlimitedChannel[eventWithCallback, uint64]
	writeTaskPools []*writeTaskPool

	gcManager *gcManager

	messageCenter messaging.MessageCenter

	coordinatorInfo struct {
		sync.RWMutex
		id node.ID
	}

	// To manage background goroutines.
	wg sync.WaitGroup

	dispatcherMeta struct {
		sync.RWMutex
		dispatcherStats   map[common.DispatcherID]*dispatcherStat
		subscriptionStats map[logpuller.SubscriptionID]*subscriptionStat
		// table id -> dispatcher ids
		// use table id as the key is to share data between spans not completely the same in the future.
		tableToDispatchers map[int64]map[common.DispatcherID]bool
	}
}

const (
	dataDir             = "event_store"
	dbCount             = 4
	writeWorkerNumPerDB = 4
)

func New(
	ctx context.Context,
	root string,
	subClient *logpuller.SubscriptionClient,
	pdClock pdutil.Clock,
) EventStore {
	dbPath := fmt.Sprintf("%s/%s", root, dataDir)

	// FIXME: avoid remove
	err := os.RemoveAll(dbPath)
	if err != nil {
		log.Panic("fail to remove path")
	}

	store := &eventStore{
		pdClock:   pdClock,
		subClient: subClient,

		dbs:            createPebbleDBs(dbPath, dbCount),
		chs:            make([]*chann.UnlimitedChannel[eventWithCallback, uint64], 0, dbCount),
		writeTaskPools: make([]*writeTaskPool, 0, dbCount),

		gcManager: newGCManager(),
	}

	// create a write task pool per db instance
	for i := 0; i < dbCount; i++ {
		store.chs = append(store.chs, chann.NewUnlimitedChannel[eventWithCallback, uint64](nil, eventWithCallbackSizer))
		store.writeTaskPools = append(store.writeTaskPools, newWriteTaskPool(store, store.dbs[i], store.chs[i], writeWorkerNumPerDB))
	}
	store.dispatcherMeta.dispatcherStats = make(map[common.DispatcherID]*dispatcherStat)
	store.dispatcherMeta.subscriptionStats = make(map[logpuller.SubscriptionID]*subscriptionStat)
	store.dispatcherMeta.tableToDispatchers = make(map[int64]map[common.DispatcherID]bool)

	// recv and handle messages
	messageCenter := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	store.messageCenter = messageCenter
	messageCenter.RegisterHandler(messaging.EventStoreTopic, store.handleMessage)

	return store
}

type writeTaskPool struct {
	store     *eventStore
	db        *pebble.DB
	dataCh    *chann.UnlimitedChannel[eventWithCallback, uint64]
	workerNum int
}

func newWriteTaskPool(store *eventStore, db *pebble.DB, ch *chann.UnlimitedChannel[eventWithCallback, uint64], workerNum int) *writeTaskPool {
	return &writeTaskPool{
		store:     store,
		db:        db,
		dataCh:    ch,
		workerNum: workerNum,
	}
}

func (p *writeTaskPool) run(ctx context.Context) {
	p.store.wg.Add(p.workerNum)
	for i := 0; i < p.workerNum; i++ {
		go func() {
			defer p.store.wg.Done()
			buffer := make([]eventWithCallback, 0, 128)
			for {
				select {
				case <-ctx.Done():
					return
				default:
					events, ok := p.dataCh.GetMultipleNoGroup(buffer)
					if !ok {
						return
					}
					p.store.writeEvents(p.db, events)
					for i := range events {
						events[i].callback()
					}
					buffer = buffer[:0]
				}
			}
		}()
	}
}

func (e *eventStore) setCoordinatorInfo(id node.ID) {
	e.coordinatorInfo.Lock()
	defer e.coordinatorInfo.Unlock()
	e.coordinatorInfo.id = id
}

func (e *eventStore) getCoordinatorInfo() node.ID {
	e.coordinatorInfo.RLock()
	defer e.coordinatorInfo.RUnlock()
	return e.coordinatorInfo.id
}

func (e *eventStore) Name() string {
	return appcontext.EventStore
}

func (e *eventStore) Run(ctx context.Context) error {
	log.Info("event store start to run")
	defer func() {
		log.Info("event store exited")
	}()
	eg, ctx := errgroup.WithContext(ctx)

	for _, p := range e.writeTaskPools {
		p := p
		eg.Go(func() error {
			p.run(ctx)
			return nil
		})
	}

	// TODO: manage gcManager exit
	eg.Go(func() error {
		return e.gcManager.run(ctx, e.deleteEvents)
	})

	eg.Go(func() error {
		return e.updateMetrics(ctx)
	})

	eg.Go(func() error {
		return e.uploadStatePeriodically(ctx)
	})

	return eg.Wait()
}

func (e *eventStore) Close(ctx context.Context) error {
	log.Info("event store start to close")
	defer log.Info("event store closed")

	log.Info("closing pebble db")
	for _, db := range e.dbs {
		if err := db.Close(); err != nil {
			log.Error("failed to close pebble db", zap.Error(err))
		}
	}
	log.Info("pebble db closed")

	return nil
}

func (e *eventStore) RegisterDispatcher(
	dispatcherID common.DispatcherID,
	tableSpan *heartbeatpb.TableSpan,
	startTs uint64,
	notifier ResolvedTsNotifier,
	onlyReuse bool,
	bdrMode bool,
) (bool, error) {
	log.Info("register dispatcher",
		zap.Stringer("dispatcherID", dispatcherID),
		zap.String("span", common.FormatTableSpan(tableSpan)),
		zap.Uint64("startTs", startTs))

	start := time.Now()
	defer func() {
		log.Info("register dispatcher done",
			zap.Stringer("dispatcherID", dispatcherID),
			zap.String("span", common.FormatTableSpan(tableSpan)),
			zap.Uint64("startTs", startTs),
			zap.Duration("duration", time.Since(start)))
	}()

	stat := &dispatcherStat{
		dispatcherID: dispatcherID,
		tableSpan:    tableSpan,
		checkpointTs: startTs,
	}

	e.dispatcherMeta.Lock()
	if candidateIDs, ok := e.dispatcherMeta.tableToDispatchers[tableSpan.TableID]; ok {
		for candidateID := range candidateIDs {
			candidateDispatcher, ok := e.dispatcherMeta.dispatcherStats[candidateID]
			if !ok {
				log.Panic("should not happen")
			}
			if candidateDispatcher.tableSpan.Equal(tableSpan) {
				subscriptionStat, ok := e.dispatcherMeta.subscriptionStats[candidateDispatcher.subID]
				if !ok {
					log.Panic("should not happen")
				}
				// check whether startTs is in the range [checkpointTs, resolvedTs]
				// for `[checkpointTs`: because we want data > startTs, so data <= checkpointTs == startTs deleted is ok.
				// for `resolvedTs]`: startTs == resolvedTs is a special case that no resolved ts has been recieved, so it is ok.
				if subscriptionStat.checkpointTs.Load() <= startTs && startTs <= subscriptionStat.resolvedTs.Load() {
					stat.subID = candidateDispatcher.subID
					e.dispatcherMeta.dispatcherStats[dispatcherID] = stat
					// add dispatcher to existing subscription and return
					subscriptionStat.dispatchers.Lock()
					subscriptionStat.dispatchers.notifiers[dispatcherID] = notifier
					subscriptionStat.dispatchers.Unlock()
					candidateIDs[dispatcherID] = true
					e.dispatcherMeta.Unlock()
					log.Info("reuse existing subscription",
						zap.Stringer("dispatcherID", dispatcherID),
						zap.Uint64("subID", uint64(stat.subID)),
						zap.Uint64("checkpointTs", subscriptionStat.checkpointTs.Load()),
						zap.Uint64("startTs", startTs))
					return true, nil
				}
			}
		}
	}
	e.dispatcherMeta.Unlock()

	if onlyReuse {
		return false, nil
	}

	// cannot share data from existing subscription, create a new subscription

	// TODO: hash span is only needed when we need to reuse data after restart
	// (if we finally decide not to reuse data after restart, use round robin instead)
	// But if we need to share data for sub span, we need hash table id instead.
	chIndex := common.HashTableSpan(tableSpan, len(e.chs))
	stat.subID = e.subClient.AllocSubscriptionID()
	subStat := &subscriptionStat{
		subID:   stat.subID,
		tableID: tableSpan.TableID,
		dbIndex: chIndex,
		eventCh: e.chs[chIndex],
	}

	e.dispatcherMeta.Lock()
	e.dispatcherMeta.dispatcherStats[dispatcherID] = stat
	subStat.dispatchers.notifiers = make(map[common.DispatcherID]ResolvedTsNotifier)
	subStat.dispatchers.notifiers[dispatcherID] = notifier
	subStat.checkpointTs.Store(startTs)
	subStat.resolvedTs.Store(startTs)
	subStat.maxEventCommitTs.Store(startTs)
	e.dispatcherMeta.subscriptionStats[stat.subID] = subStat

	dispatchersForSameTable, ok := e.dispatcherMeta.tableToDispatchers[tableSpan.TableID]
	if !ok {
		e.dispatcherMeta.tableToDispatchers[tableSpan.TableID] = map[common.DispatcherID]bool{dispatcherID: true}
	} else {
		dispatchersForSameTable[dispatcherID] = true
	}
	e.dispatcherMeta.Unlock()

	consumeKVEvents := func(kvs []common.RawKVEntry, finishCallback func()) bool {
		maxCommitTs := uint64(0)
		// Must find the max commit ts in the kvs, since the kvs is not sorted yet.
		for _, kv := range kvs {
			if kv.CRTs > maxCommitTs {
				maxCommitTs = kv.CRTs
			}
		}
		util.CompareAndMonotonicIncrease(&subStat.maxEventCommitTs, maxCommitTs)
		subStat.eventCh.Push(eventWithCallback{
			subID:    subStat.subID,
			tableID:  subStat.tableID,
			kvs:      kvs,
			callback: finishCallback,
		})
		return true
	}
	advanceResolvedTs := func(ts uint64) {
		// filter out identical resolved ts
		currentResolvedTs := subStat.resolvedTs.Load()
		if ts <= currentResolvedTs {
			return
		}
		// just do CompareAndSwap once, if failed, it means another goroutine has updated resolvedTs
		if subStat.resolvedTs.CompareAndSwap(currentResolvedTs, ts) {
			subStat.dispatchers.Lock()
			defer subStat.dispatchers.Unlock()
			for _, notifier := range subStat.dispatchers.notifiers {
				notifier(ts, subStat.maxEventCommitTs.Load())
			}
			CounterResolved.Inc()
		}
	}
	// Note: don't hold any lock when call Subscribe
	e.subClient.Subscribe(stat.subID, *tableSpan, startTs, consumeKVEvents, advanceResolvedTs, 600, bdrMode)
	metrics.EventStoreSubscriptionGauge.Inc()
	return true, nil
}

func (e *eventStore) UnregisterDispatcher(dispatcherID common.DispatcherID) error {
	log.Info("unregister dispatcher", zap.Stringer("dispatcherID", dispatcherID))
	defer func() {
		log.Info("unregister dispatcher done", zap.Stringer("dispatcherID", dispatcherID))
	}()
	e.dispatcherMeta.Lock()
	defer e.dispatcherMeta.Unlock()
	stat, ok := e.dispatcherMeta.dispatcherStats[dispatcherID]
	if !ok {
		return nil
	}
	subID := stat.subID
	tableID := stat.tableSpan.TableID
	delete(e.dispatcherMeta.dispatcherStats, dispatcherID)

	// delete the dispatcher from subscription
	subscriptionStat, ok := e.dispatcherMeta.subscriptionStats[subID]
	if !ok {
		log.Panic("should not happen")
	}
	subscriptionStat.dispatchers.Lock()
	delete(subscriptionStat.dispatchers.notifiers, dispatcherID)
	if len(subscriptionStat.dispatchers.notifiers) == 0 {
		delete(e.dispatcherMeta.subscriptionStats, subID)
		// TODO: do we need unlock before puller.Unsubscribe?
		e.subClient.Unsubscribe(subID)
		log.Info("clean data for subscription",
			zap.Int("dbIndex", subscriptionStat.dbIndex),
			zap.Uint64("subID", uint64(subID)),
			zap.Int64("tableID", subscriptionStat.tableID))
		if err := e.deleteEvents(subscriptionStat.dbIndex, uint64(subID), subscriptionStat.tableID, 0, math.MaxUint64); err != nil {
			log.Warn("fail to delete events", zap.Error(err))
		}
		metrics.EventStoreSubscriptionGauge.Dec()
	}
	subscriptionStat.dispatchers.Unlock()

	// delete the dispatcher from table subscriptions
	dispatchersForSameTable, ok := e.dispatcherMeta.tableToDispatchers[tableID]
	if !ok {
		log.Panic("should not happen")
	}
	delete(dispatchersForSameTable, dispatcherID)
	if len(dispatchersForSameTable) == 0 {
		delete(e.dispatcherMeta.tableToDispatchers, tableID)
	}

	return nil
}

func (e *eventStore) UpdateDispatcherCheckpointTs(
	dispatcherID common.DispatcherID,
	checkpointTs uint64,
) error {
	e.dispatcherMeta.RLock()
	defer e.dispatcherMeta.RUnlock()
	if stat, ok := e.dispatcherMeta.dispatcherStats[dispatcherID]; ok {
		stat.checkpointTs = checkpointTs
		subscriptionStat := e.dispatcherMeta.subscriptionStats[stat.subID]
		// calculate the new checkpoint ts of the subscription
		newCheckpointTs := uint64(0)
		for dispatcherID := range subscriptionStat.dispatchers.notifiers {
			dispatcherStat := e.dispatcherMeta.dispatcherStats[dispatcherID]
			if newCheckpointTs == 0 || dispatcherStat.checkpointTs < newCheckpointTs {
				newCheckpointTs = dispatcherStat.checkpointTs
			}
		}
		if newCheckpointTs == 0 {
			return nil
		}
		if newCheckpointTs < subscriptionStat.checkpointTs.Load() {
			log.Panic("should not happen",
				zap.Uint64("newCheckpointTs", newCheckpointTs),
				zap.Uint64("oldCheckpointTs", subscriptionStat.checkpointTs.Load()))
		}

		if subscriptionStat.checkpointTs.Load() < newCheckpointTs {
			e.gcManager.addGCItem(
				subscriptionStat.dbIndex,
				uint64(subscriptionStat.subID),
				stat.tableSpan.TableID,
				subscriptionStat.checkpointTs.Load(),
				newCheckpointTs,
			)
			subscriptionStat.checkpointTs.CompareAndSwap(subscriptionStat.checkpointTs.Load(), newCheckpointTs)
			if log.GetLevel() <= zap.DebugLevel {
				log.Debug("update checkpoint ts",
					zap.Any("dispatcherID", dispatcherID),
					zap.Uint64("subID", uint64(stat.subID)),
					zap.Uint64("newCheckpointTs", newCheckpointTs),
					zap.Uint64("oldCheckpointTs", subscriptionStat.checkpointTs.Load()))
			}
		}
	}
	return nil
}

func (e *eventStore) GetDispatcherDMLEventState(dispatcherID common.DispatcherID) (bool, DMLEventState) {
	e.dispatcherMeta.RLock()
	defer e.dispatcherMeta.RUnlock()
	stat, ok := e.dispatcherMeta.dispatcherStats[dispatcherID]
	if !ok {
		log.Warn("fail to find dispatcher", zap.Stringer("dispatcherID", dispatcherID))
		return false, DMLEventState{
			// ResolvedTs:       subscriptionStat.resolvedTs,
			MaxEventCommitTs: math.MaxUint64,
		}
	}
	subscriptionStat := e.dispatcherMeta.subscriptionStats[stat.subID]
	return true, DMLEventState{
		// ResolvedTs:       subscriptionStat.resolvedTs,
		MaxEventCommitTs: subscriptionStat.maxEventCommitTs.Load(),
	}
}

func (e *eventStore) GetIterator(dispatcherID common.DispatcherID, dataRange common.DataRange) (EventIterator, error) {
	e.dispatcherMeta.RLock()
	stat, ok := e.dispatcherMeta.dispatcherStats[dispatcherID]
	if !ok {
		log.Warn("fail to find dispatcher", zap.Stringer("dispatcherID", dispatcherID))
		e.dispatcherMeta.RUnlock()
		return nil, nil
	}
	subscriptionStat := e.dispatcherMeta.subscriptionStats[stat.subID]
	if dataRange.StartTs < subscriptionStat.checkpointTs.Load() {
		log.Panic("should not happen",
			zap.Stringer("dispatcherID", dispatcherID),
			zap.Uint64("checkpointTs", subscriptionStat.checkpointTs.Load()),
			zap.Uint64("startTs", dataRange.StartTs))
	}
	db := e.dbs[subscriptionStat.dbIndex]
	e.dispatcherMeta.RUnlock()

	// convert range before pass it to pebble: (startTs, endTs] is equal to [startTs + 1, endTs + 1)
	start := EncodeKeyPrefix(uint64(subscriptionStat.subID), stat.tableSpan.TableID, dataRange.StartTs+1)
	end := EncodeKeyPrefix(uint64(subscriptionStat.subID), stat.tableSpan.TableID, dataRange.EndTs+1)
	// TODO: optimize read performance
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	if err != nil {
		return nil, err
	}
	startTime := time.Now()
	iter.First()
	metricEventStoreFirstReadDurationHistogram.Observe(time.Since(startTime).Seconds())
	metrics.EventStoreScanRequestsCount.Inc()

	return &eventStoreIter{
		tableID:      stat.tableSpan.TableID,
		innerIter:    iter,
		prevStartTs:  0,
		prevCommitTs: 0,
		startTs:      dataRange.StartTs,
		endTs:        dataRange.EndTs,
		rowCount:     0,
	}, nil
}

func (e *eventStore) updateMetrics(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			e.updateMetricsOnce()
		}
	}
}

func (e *eventStore) updateMetricsOnce() {
	for i, db := range e.dbs {
		stats := db.Metrics()
		id := strconv.Itoa(i + 1)
		metrics.EventStoreOnDiskDataSizeGauge.WithLabelValues(id).Set(float64(stats.DiskSpaceUsage()))
		memorySize := stats.MemTable.Size
		if stats.BlockCache.Size > 0 {
			memorySize += uint64(stats.BlockCache.Size)
		}
		metrics.EventStoreInMemoryDataSizeGauge.WithLabelValues(id).Set(float64(memorySize))
	}

	pdTime := e.pdClock.CurrentTime()
	pdPhyTs := oracle.GetPhysical(pdTime)
	minResolvedTs := uint64(0)
	e.dispatcherMeta.RLock()
	for _, subscriptionStat := range e.dispatcherMeta.subscriptionStats {
		// resolved ts lag
		resolvedTs := subscriptionStat.resolvedTs.Load()
		resolvedPhyTs := oracle.ExtractPhysical(resolvedTs)
		resolvedLag := float64(pdPhyTs-resolvedPhyTs) / 1e3
		metrics.EventStoreDispatcherResolvedTsLagHist.Observe(float64(resolvedLag))
		if minResolvedTs == 0 || resolvedTs < minResolvedTs {
			minResolvedTs = resolvedTs
		}
		// checkpoint ts lag
		checkpointTs := subscriptionStat.checkpointTs.Load()
		watermarkPhyTs := oracle.ExtractPhysical(checkpointTs)
		watermarkLag := float64(pdPhyTs-watermarkPhyTs) / 1e3
		metrics.EventStoreDispatcherWatermarkLagHist.Observe(float64(watermarkLag))
	}
	e.dispatcherMeta.RUnlock()
	if minResolvedTs == 0 {
		metrics.EventStoreResolvedTsLagGauge.Set(0)
		return
	}
	minResolvedPhyTs := oracle.ExtractPhysical(minResolvedTs)
	eventStoreResolvedTsLag := float64(pdPhyTs-minResolvedPhyTs) / 1e3
	metrics.EventStoreResolvedTsLagGauge.Set(eventStoreResolvedTsLag)
}

func (e *eventStore) writeEvents(db *pebble.DB, events []eventWithCallback) error {
	metrics.EventStoreWriteRequestsCount.Inc()
	batch := db.NewBatch()
	kvCount := 0
	for _, event := range events {
		kvCount += len(event.kvs)
		for _, kv := range event.kvs {
			key := EncodeKey(uint64(event.subID), event.tableID, &kv)
			value := kv.Encode()
			if err := batch.Set(key, value, pebble.NoSync); err != nil {
				log.Panic("failed to update pebble batch", zap.Error(err))
			}
		}
	}
	CounterKv.Add(float64(kvCount))
	metrics.EventStoreWriteBatchEventsCountHist.Observe(float64(kvCount))
	metrics.EventStoreWriteBatchSizeHist.Observe(float64(batch.Len()))
	metrics.EventStoreWriteBytes.Add(float64(batch.Len()))
	start := time.Now()
	err := batch.Commit(pebble.NoSync)
	metrics.EventStoreWriteDurationHistogram.Observe(float64(time.Since(start).Milliseconds()) / 1000)
	return err
}

func (e *eventStore) deleteEvents(dbIndex int, uniqueKeyID uint64, tableID int64, startTs uint64, endTs uint64) error {
	db := e.dbs[dbIndex]
	start := EncodeKeyPrefix(uniqueKeyID, tableID, startTs)
	end := EncodeKeyPrefix(uniqueKeyID, tableID, endTs)

	return db.DeleteRange(start, end, pebble.NoSync)
}

type eventStoreIter struct {
	tableID      common.TableID
	innerIter    *pebble.Iterator
	prevStartTs  uint64
	prevCommitTs uint64

	// for debug
	startTs  uint64
	endTs    uint64
	rowCount int64
}

func (iter *eventStoreIter) Next() (*common.RawKVEntry, bool, error) {
	if iter.innerIter == nil {
		log.Panic("iter is nil")
	}
	if !iter.innerIter.Valid() {
		return nil, false, nil
	}

	value := iter.innerIter.Value()
	// rawKV need reference the byte slice, so we need copy it here
	copiedValue := make([]byte, len(value))
	copy(copiedValue, value)
	rawKV := &common.RawKVEntry{}
	err := rawKV.Decode(copiedValue)
	if err != nil {
		log.Panic("fail to decode raw kv entry", zap.Error(err))
	}
	metrics.EventStoreScanBytes.Add(float64(len(copiedValue)))
	isNewTxn := false
	if iter.prevCommitTs == 0 || (rawKV.StartTs != iter.prevStartTs || rawKV.CRTs != iter.prevCommitTs) {
		isNewTxn = true
	}
	iter.prevCommitTs = rawKV.CRTs
	iter.prevStartTs = rawKV.StartTs
	iter.rowCount++
	startTime := time.Now()
	iter.innerIter.Next()
	metricEventStoreNextReadDurationHistogram.Observe(float64(time.Since(startTime).Seconds()))
	return rawKV, isNewTxn, nil
}

func (iter *eventStoreIter) Close() (int64, error) {
	if iter.innerIter == nil {
		log.Info("event store close nil iter",
			zap.Uint64("tableID", uint64(iter.tableID)),
			zap.Uint64("startTs", iter.startTs),
			zap.Uint64("endTs", iter.endTs),
			zap.Int64("rowCount", iter.rowCount))
		return 0, nil
	}
	startTime := time.Now()
	err := iter.innerIter.Close()
	iter.innerIter = nil
	metricEventStoreCloseReadDurationHistogram.Observe(float64(time.Since(startTime).Seconds()))
	return iter.rowCount, err
}

func (e *eventStore) handleMessage(_ context.Context, targetMessage *messaging.TargetMessage) error {
	for _, msg := range targetMessage.Message {
		switch msg.(type) {
		case *common.LogCoordinatorBroadcastRequest:
			e.setCoordinatorInfo(targetMessage.From)
		default:
			log.Panic("invalid message type", zap.Any("msg", msg))
		}
	}
	return nil
}

func (e *eventStore) uploadStatePeriodically(ctx context.Context) error {
	tick := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			e.dispatcherMeta.RLock()
			state := &logservicepb.EventStoreState{
				Subscriptions: make(map[int64]*logservicepb.SubscriptionStates),
			}
			for tableID, dispatcherIDs := range e.dispatcherMeta.tableToDispatchers {
				subStates := make([]*logservicepb.SubscriptionState, 0, len(dispatcherIDs))
				subIDs := make(map[logpuller.SubscriptionID]bool)
				for dispatcherID := range dispatcherIDs {
					dispatcherStat := e.dispatcherMeta.dispatcherStats[dispatcherID]
					subID := dispatcherStat.subID
					subStat := e.dispatcherMeta.subscriptionStats[subID]
					if _, ok := subIDs[subID]; ok {
						continue
					}
					subStates = append(subStates, &logservicepb.SubscriptionState{
						SubID:        uint64(subID),
						Span:         dispatcherStat.tableSpan,
						CheckpointTs: subStat.checkpointTs.Load(),
						ResolvedTs:   subStat.resolvedTs.Load(),
					})
					subIDs[subID] = true
				}
				sort.Slice(subStates, func(i, j int) bool {
					return subStates[i].SubID < subStates[j].SubID
				})
				state.Subscriptions[tableID] = &logservicepb.SubscriptionStates{
					Subscriptions: subStates,
				}
			}

			message := messaging.NewSingleTargetMessage(e.getCoordinatorInfo(), messaging.LogCoordinatorTopic, state)
			e.dispatcherMeta.RUnlock()
			// just ignore messagees fail to send
			if err := e.messageCenter.SendEvent(message); err != nil {
				log.Debug("send broadcast message to node failed", zap.Error(err))
			}
		}
	}
}
