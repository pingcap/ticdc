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
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"os"
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
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
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
	common.SubModule

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

	// GetIterator return an iterator which scan the data in ts range (dataRange.StartTs, dataRange.EndTs]
	GetIterator(dispatcherID common.DispatcherID, dataRange common.DataRange) (EventIterator, error)
}

type DMLEventState struct {
	// ResolvedTs       uint64
	// The max commit ts of dml event in the store.
	MaxEventCommitTs uint64
}

type EventIterator interface {
	// Next returns the next event in the iterator and whether this event is from a new txn.
	Next() (*common.RawKVEntry, bool, error)

	// Close closes the iterator.
	// It returns the number of events that are read from the iterator.
	Close() (eventCnt int64, err error)
}

type dispatcherStat struct {
	dispatcherID common.DispatcherID
	// data span of this dispatcher
	tableSpan *heartbeatpb.TableSpan
	// the max ts of events which is not needed by this dispatcher
	checkpointTs uint64
	// the subscription which this dipatcher depends on
	subStat *subscriptionStat
}

type subscriptionStat struct {
	subID logpuller.SubscriptionID
	// data span of the subscription, it can support dispatchers with smaller span
	tableSpan *heartbeatpb.TableSpan
	// dispatchers depend on this subscription
	dispatchers struct {
		sync.Mutex
		notifiers map[common.DispatcherID]ResolvedTsNotifier
	}
	// the index of the db which stores the data of the subscription
	// used to clean obselete data of the subscription
	dbIndex int
	// used to receive data of the subscription from upstream
	eventCh *chann.UnlimitedChannel[eventWithCallback, uint64]
	// data <= checkpointTs can be deleted
	checkpointTs atomic.Uint64
	// the resolveTs persisted in the store
	resolvedTs atomic.Uint64
	// the max commit ts of dml event in the store
	maxEventCommitTs atomic.Uint64
	// the time when the subscription is not used by any dispatchers
	// 0 means the subscription is not idle
	idleTime atomic.Int64
}

type subscriptionStats map[logpuller.SubscriptionID]*subscriptionStat

type eventWithCallback struct {
	subID   logpuller.SubscriptionID
	tableID int64
	kvs     []common.RawKVEntry
	// kv with commitTs <= currentResolvedTs will be filtered out
	currentResolvedTs uint64
	callback          func()
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
	subClient logpuller.SubscriptionClient

	dbs            []*pebble.DB
	chs            []*chann.UnlimitedChannel[eventWithCallback, uint64]
	writeTaskPools []*writeTaskPool

	gcManager *gcManager

	messageCenter messaging.MessageCenter

	coordinatorInfo struct {
		sync.Mutex
		id node.ID
	}
	// The channel is used to gather the subscription info
	// which need to be uploaded to log coordinator periodically.
	subscriptionChangeCh *chann.DrainableChann[SubscriptionChange]

	// To manage background goroutines.
	wg sync.WaitGroup

	dispatcherMeta struct {
		sync.RWMutex
		// dispatcher id -> dispatcher stat
		dispatcherStats map[common.DispatcherID]*dispatcherStat
		// table id -> subscription stats
		tableStats map[int64]subscriptionStats
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
	subClient logpuller.SubscriptionClient,
) EventStore {
	dbPath := fmt.Sprintf("%s/%s", root, dataDir)

	// FIXME: avoid remove
	err := os.RemoveAll(dbPath)
	if err != nil {
		log.Panic("fail to remove path")
	}

	store := &eventStore{
		pdClock:   appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock),
		subClient: subClient,

		dbs:            createPebbleDBs(dbPath, dbCount),
		chs:            make([]*chann.UnlimitedChannel[eventWithCallback, uint64], 0, dbCount),
		writeTaskPools: make([]*writeTaskPool, 0, dbCount),

		gcManager: newGCManager(),

		subscriptionChangeCh: chann.NewAutoDrainChann[SubscriptionChange](),
	}

	// create a write task pool per db instance
	for i := 0; i < dbCount; i++ {
		store.chs = append(store.chs, chann.NewUnlimitedChannel[eventWithCallback, uint64](nil, eventWithCallbackSizer))
		store.writeTaskPools = append(store.writeTaskPools, newWriteTaskPool(store, store.dbs[i], store.chs[i], writeWorkerNumPerDB))
	}
	store.dispatcherMeta.dispatcherStats = make(map[common.DispatcherID]*dispatcherStat)
	store.dispatcherMeta.tableStats = make(map[int64]subscriptionStats)

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
					if err := p.store.writeEvents(p.db, events); err != nil {
						log.Panic("write events failed")
					}
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
	e.coordinatorInfo.Lock()
	defer e.coordinatorInfo.Unlock()
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

	// recv and handle messages
	messageCenter := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	e.messageCenter = messageCenter
	messageCenter.RegisterHandler(messaging.EventStoreTopic, e.handleMessage)

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

	// Delay the remove of subscription which has no dispatchers depend on it to a separate goroutine.
	// Because the subscription may be used by later dispatchers.(e.g. after dispatcher scheduling)
	eg.Go(func() error {
		return e.cleanObsoleteSubscriptions(ctx)
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
	dispatcherSpan *heartbeatpb.TableSpan,
	startTs uint64,
	notifier ResolvedTsNotifier,
	onlyReuse bool,
	bdrMode bool,
) (bool, error) {
	log.Info("register dispatcher",
		zap.Stringer("dispatcherID", dispatcherID),
		zap.String("span", common.FormatTableSpan(dispatcherSpan)),
		zap.Uint64("startTs", startTs))

	start := time.Now()
	defer func() {
		log.Info("register dispatcher done",
			zap.Stringer("dispatcherID", dispatcherID),
			zap.String("span", common.FormatTableSpan(dispatcherSpan)),
			zap.Uint64("startTs", startTs),
			zap.Duration("duration", time.Since(start)))
	}()

	stat := &dispatcherStat{
		dispatcherID: dispatcherID,
		tableSpan:    dispatcherSpan,
		checkpointTs: startTs,
	}

	e.dispatcherMeta.Lock()
	if subStats, ok := e.dispatcherMeta.tableStats[dispatcherSpan.TableID]; ok {
		for _, subStat := range subStats {
			// dispatcher span is not contained in subscription span, skip it
			if bytes.Compare(subStat.tableSpan.StartKey, dispatcherSpan.StartKey) > 0 ||
				bytes.Compare(subStat.tableSpan.EndKey, dispatcherSpan.EndKey) < 0 {
				continue
			}
			// when `onlyReuse` is false, must find a subscription with the same span, otherwise we create a new one
			if !onlyReuse && !subStat.tableSpan.Equal(dispatcherSpan) {
				continue
			}
			// check whether startTs is in the range [checkpointTs, resolvedTs]
			// why `[checkpointTs`:
			//   1) ts <= checkpointTs may be deleted
			//   2) dispatcher need data which ts > startTs
			//   so startTs == checkpointTs is ok.
			// why `resolvedTs]`:
			//   1) actually startTs > resolvedTs is also ok if the difference is small
			//   2) we use the condition startTs <= resolvedTs for simplicity
			if subStat.checkpointTs.Load() <= startTs && startTs <= subStat.resolvedTs.Load() {
				stat.subStat = subStat
				e.dispatcherMeta.dispatcherStats[dispatcherID] = stat
				subStat.idleTime.Store(0)
				// add dispatcher to existing subscription and return
				subStat.dispatchers.Lock()
				subStat.dispatchers.notifiers[dispatcherID] = notifier
				subStat.dispatchers.Unlock()
				e.dispatcherMeta.Unlock()
				log.Info("reuse existing subscription",
					zap.Stringer("dispatcherID", dispatcherID),
					zap.String("dispatcherSpan", common.FormatTableSpan(dispatcherSpan)),
					zap.Uint64("startTs", startTs),
					zap.Uint64("subID", uint64(subStat.subID)),
					zap.String("subSpan", common.FormatTableSpan(subStat.tableSpan)),
					zap.Uint64("checkpointTs", subStat.checkpointTs.Load()))
				return true, nil
			}
		}
	}
	e.dispatcherMeta.Unlock()

	if onlyReuse {
		return false, nil
	}

	// cannot share data from existing subscription, create a new subscription

	chIndex := common.HashTableSpan(dispatcherSpan, len(e.chs))
	subStat := &subscriptionStat{
		subID:     e.subClient.AllocSubscriptionID(),
		tableSpan: dispatcherSpan,
		dbIndex:   chIndex,
		eventCh:   e.chs[chIndex],
	}
	subStat.dispatchers.notifiers = make(map[common.DispatcherID]ResolvedTsNotifier)
	subStat.dispatchers.notifiers[dispatcherID] = notifier
	subStat.checkpointTs.Store(startTs)
	subStat.resolvedTs.Store(startTs)
	subStat.maxEventCommitTs.Store(startTs)
	stat.subStat = subStat

	e.dispatcherMeta.Lock()
	e.dispatcherMeta.dispatcherStats[dispatcherID] = stat
	if len(e.dispatcherMeta.tableStats[dispatcherSpan.TableID]) == 0 {
		e.dispatcherMeta.tableStats[dispatcherSpan.TableID] = make(subscriptionStats)
	}
	e.dispatcherMeta.tableStats[dispatcherSpan.TableID][subStat.subID] = subStat
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
			subID:             subStat.subID,
			tableID:           subStat.tableSpan.TableID,
			kvs:               kvs,
			currentResolvedTs: subStat.resolvedTs.Load(),
			callback:          finishCallback,
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

	serverConfig := config.GetGlobalServerConfig()
	resolvedTsAdvanceInterval := int64(serverConfig.KVClient.AdvanceIntervalInMs)
	// Note: don't hold any lock when call Subscribe
	e.subClient.Subscribe(stat.subStat.subID, *dispatcherSpan, startTs, consumeKVEvents, advanceResolvedTs, resolvedTsAdvanceInterval, bdrMode)
	log.Info("new subscription created",
		zap.Uint64("subID", uint64(stat.subStat.subID)),
		zap.String("subSpan", common.FormatTableSpan(stat.subStat.tableSpan)))
	e.subscriptionChangeCh.In() <- SubscriptionChange{
		ChangeType:   SubscriptionChangeTypeAdd,
		SubID:        uint64(stat.subStat.subID),
		Span:         dispatcherSpan,
		CheckpointTs: startTs,
		ResolvedTs:   startTs,
	}
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
	subStat := stat.subStat
	delete(e.dispatcherMeta.dispatcherStats, dispatcherID)

	subStat.dispatchers.Lock()
	delete(subStat.dispatchers.notifiers, dispatcherID)
	if len(subStat.dispatchers.notifiers) == 0 {
		subStat.idleTime.Store(time.Now().UnixMilli())
	}
	subStat.dispatchers.Unlock()
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
		subStat := stat.subStat
		// calculate the new checkpoint ts of the subscription
		newCheckpointTs := uint64(0)
		for dispatcherID := range subStat.dispatchers.notifiers {
			dispatcherStat := e.dispatcherMeta.dispatcherStats[dispatcherID]
			if newCheckpointTs == 0 || dispatcherStat.checkpointTs < newCheckpointTs {
				newCheckpointTs = dispatcherStat.checkpointTs
			}
		}
		if newCheckpointTs == 0 {
			return nil
		}
		if newCheckpointTs < subStat.checkpointTs.Load() {
			log.Panic("should not happen",
				zap.Uint64("newCheckpointTs", newCheckpointTs),
				zap.Uint64("oldCheckpointTs", subStat.checkpointTs.Load()))
		}

		if subStat.checkpointTs.Load() < newCheckpointTs {
			e.gcManager.addGCItem(
				subStat.dbIndex,
				uint64(subStat.subID),
				stat.tableSpan.TableID,
				subStat.checkpointTs.Load(),
				newCheckpointTs,
			)
			e.subscriptionChangeCh.In() <- SubscriptionChange{
				ChangeType:   SubscriptionChangeTypeUpdate,
				SubID:        uint64(stat.subStat.subID),
				Span:         stat.tableSpan,
				CheckpointTs: newCheckpointTs,
				ResolvedTs:   subStat.resolvedTs.Load(),
			}
			subStat.checkpointTs.CompareAndSwap(subStat.checkpointTs.Load(), newCheckpointTs)
			if log.GetLevel() <= zap.DebugLevel {
				log.Debug("update checkpoint ts",
					zap.Any("dispatcherID", dispatcherID),
					zap.Uint64("subID", uint64(subStat.subID)),
					zap.Uint64("newCheckpointTs", newCheckpointTs),
					zap.Uint64("oldCheckpointTs", subStat.checkpointTs.Load()))
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
			MaxEventCommitTs: math.MaxUint64,
		}
	}
	return true, DMLEventState{
		MaxEventCommitTs: stat.subStat.maxEventCommitTs.Load(),
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
	subscriptionStat := stat.subStat
	checkpoint := subscriptionStat.checkpointTs.Load()
	if dataRange.StartTs < checkpoint {
		log.Panic("dataRange startTs is smaller than subscriptionStat checkpointTs, it should not happen",
			zap.Stringer("dispatcherID", dispatcherID),
			zap.Uint64("startTs", dataRange.StartTs),
			zap.Uint64("checkpointTs", checkpoint))
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
		return nil, errors.Trace(err)
	}
	startTime := time.Now()
	// todo: what happens if iter.First() returns false?
	_ = iter.First()
	metricEventStoreFirstReadDurationHistogram.Observe(time.Since(startTime).Seconds())
	metrics.EventStoreScanRequestsCount.Inc()

	needCheckSpan := true
	if stat.tableSpan.Equal(subscriptionStat.tableSpan) {
		needCheckSpan = false
	}

	return &eventStoreIter{
		tableSpan:     stat.tableSpan,
		needCheckSpan: needCheckSpan,
		innerIter:     iter,
		prevStartTs:   0,
		prevCommitTs:  0,
		startTs:       dataRange.StartTs,
		endTs:         dataRange.EndTs,
		rowCount:      0,
	}, nil
}

func (e *eventStore) cleanObsoleteSubscriptions(ctx context.Context) error {
	ticker := time.NewTicker(1 * time.Minute)
	ttlInMs := int64(5 * 60 * 1000) // 5min
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			now := time.Now().UnixMilli()
			e.dispatcherMeta.Lock()
			for tableID, subStats := range e.dispatcherMeta.tableStats {
				for subID, subStat := range subStats {
					idleTime := subStat.idleTime.Load()
					if idleTime == 0 {
						continue
					}
					if now-idleTime > ttlInMs {
						log.Info("clean obsolete subscription",
							zap.Uint64("subID", uint64(subID)),
							zap.Int("dbIndex", subStat.dbIndex),
							zap.Int64("tableID", subStat.tableSpan.TableID))
						e.subClient.Unsubscribe(subID)
						if err := e.deleteEvents(subStat.dbIndex, uint64(subID), subStat.tableSpan.TableID, 0, math.MaxUint64); err != nil {
							log.Warn("fail to delete events", zap.Error(err))
						}
						delete(subStats, subID)
						e.subscriptionChangeCh.In() <- SubscriptionChange{
							ChangeType: SubscriptionChangeTypeRemove,
							SubID:      uint64(subStat.subID),
							Span:       subStat.tableSpan,
						}
						metrics.EventStoreSubscriptionGauge.Dec()
						// If subStats becomes empty, remove it from tableStats
						if len(subStats) == 0 {
							delete(e.dispatcherMeta.tableStats, tableID)
						}
					}
				}
			}
			e.dispatcherMeta.Unlock()
		}
	}
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
	for _, subStats := range e.dispatcherMeta.tableStats {
		for _, subStat := range subStats {
			// resolved ts lag
			resolvedTs := subStat.resolvedTs.Load()
			resolvedPhyTs := oracle.ExtractPhysical(resolvedTs)
			resolvedLag := float64(pdPhyTs-resolvedPhyTs) / 1e3
			metrics.EventStoreDispatcherResolvedTsLagHist.Observe(float64(resolvedLag))
			if minResolvedTs == 0 || resolvedTs < minResolvedTs {
				minResolvedTs = resolvedTs
			}
			// checkpoint ts lag
			checkpointTs := subStat.checkpointTs.Load()
			watermarkPhyTs := oracle.ExtractPhysical(checkpointTs)
			watermarkLag := float64(pdPhyTs-watermarkPhyTs) / 1e3
			metrics.EventStoreDispatcherWatermarkLagHist.Observe(float64(watermarkLag))
		}
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
			if kv.CRTs <= event.currentResolvedTs {
				log.Warn("event store received kv with commitTs less than resolvedTs",
					zap.Uint64("commitTs", kv.CRTs),
					zap.Uint64("resolvedTs", event.currentResolvedTs),
					zap.Uint64("subID", uint64(event.subID)),
					zap.Int64("tableID", event.tableID))
				continue
			}
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
	tableSpan *heartbeatpb.TableSpan
	// true when need check whether data from `innerIter` is in `tableSpan`
	// (e.g. subscription span is not the same as dispatcher span)
	needCheckSpan bool
	innerIter     *pebble.Iterator
	prevStartTs   uint64
	prevCommitTs  uint64

	// for debug
	startTs  uint64
	endTs    uint64
	rowCount int64
}

func (iter *eventStoreIter) Next() (*common.RawKVEntry, bool, error) {
	rawKV := &common.RawKVEntry{}
	for {
		if !iter.innerIter.Valid() {
			return nil, false, nil
		}
		value := iter.innerIter.Value()
		err := rawKV.Decode(value)
		if err != nil {
			log.Panic("fail to decode raw kv entry", zap.Error(err))
		}
		metrics.EventStoreScanBytes.Add(float64(len(value)))
		if !iter.needCheckSpan {
			break
		}
		comparableKey := common.ToComparableKey(rawKV.Key)
		if bytes.Compare(comparableKey, iter.tableSpan.StartKey) >= 0 &&
			bytes.Compare(comparableKey, iter.tableSpan.EndKey) <= 0 {
			break
		}
		log.Debug("event store iter skip kv not in table span",
			zap.String("tableSpan", common.FormatTableSpan(iter.tableSpan)),
			zap.String("key", hex.EncodeToString(rawKV.Key)),
			zap.Uint64("startTs", rawKV.StartTs),
			zap.Uint64("commitTs", rawKV.CRTs))
		iter.innerIter.Next()
	}
	isNewTxn := false
	if iter.prevCommitTs == 0 || (rawKV.StartTs != iter.prevStartTs || rawKV.CRTs != iter.prevCommitTs) {
		isNewTxn = true
	}
	iter.prevCommitTs = rawKV.CRTs
	iter.prevStartTs = rawKV.StartTs
	iter.rowCount++
	startTime := time.Now()
	iter.innerIter.Next()
	metricEventStoreNextReadDurationHistogram.Observe(time.Since(startTime).Seconds())
	return rawKV, isNewTxn, nil
}

func (iter *eventStoreIter) Close() (int64, error) {
	if iter.innerIter == nil {
		log.Info("event store close nil iter",
			zap.Uint64("tableID", uint64(iter.tableSpan.TableID)),
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

type SubscriptionChangeType int

const (
	SubscriptionChangeTypeAdd    SubscriptionChangeType = 1
	SubscriptionChangeTypeRemove SubscriptionChangeType = 2
	SubscriptionChangeTypeUpdate SubscriptionChangeType = 3
)

type SubscriptionChange struct {
	ChangeType   SubscriptionChangeType
	SubID        uint64
	Span         *heartbeatpb.TableSpan
	CheckpointTs uint64 // only valid for SubscriptionChangeTypeAdd/SubscriptionChangeTypeUpdate
	ResolvedTs   uint64 // only valid for SubscriptionChangeTypeAdd/SubscriptionChangeTypeUpdate
}

func (e *eventStore) uploadStatePeriodically(ctx context.Context) error {
	tick := time.NewTicker(10 * time.Second)
	state := &logservicepb.EventStoreState{
		TableStates: make(map[int64]*logservicepb.TableState),
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case change := <-e.subscriptionChangeCh.Out():
			switch change.ChangeType {
			case SubscriptionChangeTypeAdd:
				log.Info("add subscription for upload state", zap.Uint64("subID", change.SubID))
				if tableState, ok := state.TableStates[change.Span.TableID]; ok {
					tableState.Subscriptions = append(tableState.Subscriptions, &logservicepb.SubscriptionState{
						SubID:        change.SubID,
						Span:         change.Span,
						CheckpointTs: change.CheckpointTs,
						ResolvedTs:   change.ResolvedTs,
					})
				} else {
					state.TableStates[change.Span.TableID] = &logservicepb.TableState{
						Subscriptions: []*logservicepb.SubscriptionState{
							{
								SubID:        change.SubID,
								Span:         change.Span,
								CheckpointTs: change.CheckpointTs,
								ResolvedTs:   change.ResolvedTs,
							},
						},
					}
				}
			case SubscriptionChangeTypeRemove:
				log.Info("remove subscription from upload state", zap.Uint64("subID", change.SubID))
				tableState, ok := state.TableStates[change.Span.TableID]
				if !ok {
					log.Panic("cannot find table state", zap.Int64("tableID", change.Span.TableID))
				}
				targetIndex := -1
				for i := 0; i < len(tableState.Subscriptions); i++ {
					if tableState.Subscriptions[i].SubID == change.SubID {
						targetIndex = i
						break
					}
				}
				if targetIndex == -1 {
					log.Panic("cannot find subscription state", zap.Uint64("subID", change.SubID))
				}
				tableState.Subscriptions = append(tableState.Subscriptions[:targetIndex], tableState.Subscriptions[targetIndex+1:]...)
			case SubscriptionChangeTypeUpdate:
				tableState, ok := state.TableStates[change.Span.TableID]
				if !ok {
					log.Warn("cannot find table state", zap.Int64("tableID", change.Span.TableID))
					continue
				}
				targetIndex := -1
				for i := 0; i < len(tableState.Subscriptions); i++ {
					if tableState.Subscriptions[i].SubID == change.SubID {
						targetIndex = i
						break
					}
				}
				if targetIndex == -1 {
					log.Warn("cannot find subscription state", zap.Uint64("subID", change.SubID))
					continue
				}
				if change.CheckpointTs < tableState.Subscriptions[targetIndex].CheckpointTs ||
					change.ResolvedTs < tableState.Subscriptions[targetIndex].ResolvedTs {
					log.Panic("should not happen",
						zap.Uint64("subID", change.SubID),
						zap.Uint64("oldCheckpointTs", tableState.Subscriptions[targetIndex].CheckpointTs),
						zap.Uint64("oldResolvedTs", tableState.Subscriptions[targetIndex].ResolvedTs),
						zap.Uint64("newCheckpointTs", change.CheckpointTs),
						zap.Uint64("newResolvedTs", change.ResolvedTs))
				}
				tableState.Subscriptions[targetIndex].CheckpointTs = change.CheckpointTs
				tableState.Subscriptions[targetIndex].ResolvedTs = change.ResolvedTs
			default:
				log.Panic("invalid subscription change type", zap.Int("changeType", int(change.ChangeType)))
			}
		case <-tick.C:
			coordinatorID := e.getCoordinatorInfo()
			if coordinatorID == "" {
				continue
			}
			// When the log coordinator resides on the same node, it will receive the same object reference.
			// To prevent data races, we need to create a clone of the state.
			message := messaging.NewSingleTargetMessage(coordinatorID, messaging.LogCoordinatorTopic, state.Copy())
			// just ignore messagees fail to send
			if err := e.messageCenter.SendEvent(message); err != nil {
				log.Warn("send broadcast message to coordinator failed", zap.Error(err))
			}
		}
	}
}
