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
	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller"
	"github.com/pingcap/ticdc/logservice/logservicepb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
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

// Subscriber represents the dispatcher which depends on the subscription.
type Subscriber struct {
	notifyFunc ResolvedTsNotifier
	isStopped  bool
}

type EventStore interface {
	common.SubModule

	// Note: changefeedID is just a tag for dispatcher, avoid abuse it
	RegisterDispatcher(
		changefeedID common.ChangeFeedID,
		dispatcherID common.DispatcherID,
		span *heartbeatpb.TableSpan,
		startTS uint64,
		notifier ResolvedTsNotifier,
		onlyReuse bool,
		bdrMode bool,
	) bool

	UnregisterDispatcher(changefeedID common.ChangeFeedID, dispatcherID common.DispatcherID)

	UpdateDispatcherCheckpointTs(dispatcherID common.DispatcherID, checkpointTs uint64)

	// GetIterator return an iterator which scan the data in ts range (dataRange.CommitTsStart, dataRange.CommitTsEnd]
	GetIterator(dispatcherID common.DispatcherID, dataRange common.DataRange) EventIterator

	GetLogCoordinatorNodeID() node.ID
}

type DMLEventState struct {
	// ResolvedTs       uint64
	// The max commit ts of dml event in the store.
	MaxEventCommitTs uint64
}

type EventIterator interface {
	// Next returns the next event in the iterator and whether this event is from a new txn.
	Next() (*common.RawKVEntry, bool)

	// Close closes the iterator.
	// It returns the number of events that are read from the iterator.
	Close() (eventCnt int64, err error)
}

type dispatcherStat struct {
	dispatcherID common.DispatcherID
	// data span of this dispatcher
	tableSpan *heartbeatpb.TableSpan

	resolvedTs atomic.Uint64
	// the max ts of events which is not needed by this dispatcher
	checkpointTs uint64
	// the difference between `subStat`, `pendingSubStat` and `removingSubStat`:
	//   1) if there is no existing subscriptions which can be reused,
	//      or there is a existing subscription with exact span match,
	//      the dispatcher will just have a `subStat`.
	//   2) if a dispatcher can only find an existing subscription which has a larger containing span,
	//      it will reuse this existing subscription as `subStat`
	//      and create a new subscription with exact span, and set it as `pendingSubStat`.
	//   3) when `pendingStat` is ready, we try to switch `pendingStat` to `subStat`,
	//      and set the old `subStat` as `removingSubStat`.
	//      the reason why `removingSubStat` is need is that if we remove old `subStat` directly,
	//      there is a gap between the time when old `subStat` is removed
	//      and when the old `subStat` send a new resolved ts to the dispatcher.
	//      the new resolved ts may trigger a new scan task,
	// 		and if the new `subStat` doesn't advance to new resolved ts,
	//      the scan task cannot work as expected which break our design.
	//      so the correct way to remove the old `subStat` is
	//      a) set old `subStat` as `removingSubStat`
	//      b) stop receive resolve ts event from `removingSubStat`
	//      c) during later scan requests, when the new `subStat` can cover the scan range,
	// 		   remove the `removingSubStat`. if not, we can still use `removingSubStat` temporarily.
	//         (note: at most one later scan requests
	//          maybe triggered by the resolved ts event from old `subStat`)
	pendingSubStat  *subscriptionStat
	subStat         *subscriptionStat
	removingSubStat *subscriptionStat
}

type subscribersWithIdleTime struct {
	subscribers map[common.DispatcherID]*Subscriber
	idleTime    int64
}

type subscriptionStat struct {
	subID logpuller.SubscriptionID
	// data span of the subscription, it can support dispatchers with smaller span
	tableSpan   *heartbeatpb.TableSpan
	subscribers atomic.Pointer[subscribersWithIdleTime]
	// markedDeleteTime is the time when the subscription is marked for deletion.
	markedDeleteTime atomic.Int64
	// the index of the db which stores the data of the subscription
	// used to clean obselete data of the subscription
	dbIndex int
	// used to receive data of the subscription from upstream
	eventCh *chann.UnlimitedChannel[eventWithCallback, uint64]
	// data <= checkpointTs can be deleted
	checkpointTs atomic.Uint64
	// whether the subStat has received any resolved ts
	initialized atomic.Bool
	// the time when the subscription receives latest resolved ts
	lastAdvanceTime atomic.Int64
	// the time when the subscription receives latest dml event
	// it is used to calculate the idle time of the subscription
	// 0 means the subscription has not received any dml event
	lastReceiveDMLTime atomic.Int64
	// the resolveTs persisted in the store
	resolvedTs atomic.Uint64
	// the max commit ts of dml event in the store
	maxEventCommitTs atomic.Uint64
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
	for _, kv := range e.kvs {
		size += int(kv.KeyLen + kv.ValueLen + kv.OldValueLen)
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
		value atomic.Value
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

	decoderPool *sync.Pool

	// closed is used to indicate the event store is closed.
	closed atomic.Bool

	// compressionThreshold is the size in bytes above which a value will be compressed.
	compressionThreshold int
}

const (
	dataDir             = "event_store"
	dbCount             = 4
	writeWorkerNumPerDB = 2
)

func New(
	root string,
	subClient logpuller.SubscriptionClient,
) EventStore {
	dbPath := fmt.Sprintf("%s/%s", root, dataDir)

	// FIXME: avoid remove
	err := os.RemoveAll(dbPath)
	if err != nil {
		log.Panic("fail to remove path", zap.String("path", dbPath), zap.Error(err))
	}

	store := &eventStore{
		pdClock:   appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock),
		subClient: subClient,

		dbs:            createPebbleDBs(dbPath, dbCount),
		chs:            make([]*chann.UnlimitedChannel[eventWithCallback, uint64], 0, dbCount),
		writeTaskPools: make([]*writeTaskPool, 0, dbCount),

		gcManager: newGCManager(),

		subscriptionChangeCh: chann.NewAutoDrainChann[SubscriptionChange](),
		messageCenter:        appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter),

		decoderPool: &sync.Pool{
			New: func() any {
				decoder, err := zstd.NewReader(nil)
				if err != nil {
					log.Panic("failed to create zstd decoder", zap.Error(err))
				}
				return decoder
			},
		},
		compressionThreshold: config.GetGlobalServerConfig().Debug.EventStore.CompressionThreshold,
	}

	// create a write task pool per db instance
	for i := 0; i < dbCount; i++ {
		store.chs = append(store.chs, chann.NewUnlimitedChannel[eventWithCallback, uint64](nil, eventWithCallbackSizer))
		store.writeTaskPools = append(store.writeTaskPools, newWriteTaskPool(store, store.dbs[i], i, store.chs[i], writeWorkerNumPerDB))
	}
	store.dispatcherMeta.dispatcherStats = make(map[common.DispatcherID]*dispatcherStat)
	store.dispatcherMeta.tableStats = make(map[int64]subscriptionStats)

	store.messageCenter.RegisterHandler(messaging.EventStoreTopic, store.handleMessage)
	return store
}

type writeTaskPool struct {
	store     *eventStore
	db        *pebble.DB
	dbIndex   int
	dataCh    *chann.UnlimitedChannel[eventWithCallback, uint64]
	workerNum int
}

func newWriteTaskPool(store *eventStore, db *pebble.DB, index int, ch *chann.UnlimitedChannel[eventWithCallback, uint64], workerNum int) *writeTaskPool {
	return &writeTaskPool{
		store:     store,
		db:        db,
		dbIndex:   index,
		dataCh:    ch,
		workerNum: workerNum,
	}
}

func (p *writeTaskPool) run(ctx context.Context) {
	p.store.wg.Add(p.workerNum)
	for i := 0; i < p.workerNum; i++ {
		go func(workerID int) {
			defer p.store.wg.Done()
			encoder, err := zstd.NewWriter(nil)
			if err != nil {
				log.Panic("failed to create zstd encoder", zap.Error(err))
			}
			defer encoder.Close()
			buffer := make([]eventWithCallback, 0, 128)

			ioWriteDuration := metrics.EventStoreWriteWorkerIODuration.WithLabelValues(strconv.Itoa(p.dbIndex), strconv.Itoa(workerID))
			totalDuration := metrics.EventStoreWriteWorkerTotalDuration.WithLabelValues(strconv.Itoa(p.dbIndex), strconv.Itoa(workerID))
			totalStart := time.Now()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					events, ok := p.dataCh.GetMultipleNoGroup(buffer)
					if !ok {
						return
					}
					start := time.Now()
					if err = p.store.writeEvents(p.db, events, encoder); err != nil {
						log.Panic("write events failed", zap.Error(err))
					}
					for idx := range events {
						events[idx].callback()
					}
					ioWriteDuration.Observe(time.Since(start).Seconds())
					totalDuration.Observe(time.Since(totalStart).Seconds())
					totalStart = time.Now()
					buffer = buffer[:0]
				}
			}
		}(i)
	}
}

func (e *eventStore) setCoordinatorInfo(id node.ID) {
	e.coordinatorInfo.value.Store(id)
}

func (e *eventStore) getCoordinatorInfo() node.ID {
	if v := e.coordinatorInfo.value.Load(); v != nil {
		return v.(node.ID)
	}
	return ""
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
		return e.runMetricsCollector(ctx)
	})

	eg.Go(func() error {
		return e.uploadStatePeriodically(ctx)
	})

	return eg.Wait()
}

func (e *eventStore) Close(_ context.Context) error {
	log.Info("event store start to close")
	defer log.Info("event store closed")

	e.closed.Store(true)

	for _, db := range e.dbs {
		if err := db.Close(); err != nil {
			log.Error("failed to close pebble db", zap.Error(err))
		}
	}

	return nil
}

func (e *eventStore) RegisterDispatcher(
	changefeedID common.ChangeFeedID,
	dispatcherID common.DispatcherID,
	dispatcherSpan *heartbeatpb.TableSpan,
	startTs uint64,
	notifier ResolvedTsNotifier,
	onlyReuse bool,
	bdrMode bool,
) bool {
	if e.closed.Load() {
		return false
	}

	lag := time.Since(oracle.GetTimeFromTS(startTs))
	metrics.EventStoreRegisterDispatcherStartTsLagHist.Observe(lag.Seconds())
	if lag >= 10*time.Second {
		log.Warn("register dispatcher with large startTs lag",
			zap.Stringer("dispatcherID", dispatcherID),
			zap.String("span", common.FormatTableSpan(dispatcherSpan)),
			zap.Uint64("startTs", startTs),
			zap.Duration("lag", lag))
	} else {
		log.Info("register dispatcher",
			zap.Stringer("dispatcherID", dispatcherID),
			zap.String("span", common.FormatTableSpan(dispatcherSpan)),
			zap.Uint64("startTs", startTs))
	}

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
	stat.resolvedTs.Store(startTs)

	wrappedNotifier := func(resolvedTs uint64, latestCommitTs uint64) {
		util.CompareAndMonotonicIncrease(&stat.resolvedTs, resolvedTs)
		notifier(resolvedTs, latestCommitTs)
	}

	e.dispatcherMeta.Lock()
	var bestMatch *subscriptionStat
	if subStats, ok := e.dispatcherMeta.tableStats[dispatcherSpan.TableID]; ok {
		for _, subStat := range subStats {
			// Check if this subStat's span contains the dispatcherSpan
			if bytes.Compare(subStat.tableSpan.StartKey, dispatcherSpan.StartKey) <= 0 &&
				bytes.Compare(subStat.tableSpan.EndKey, dispatcherSpan.EndKey) >= 0 {

				// Check whether the subStat ts range contains startTs
				if subStat.checkpointTs.Load() > startTs || startTs > subStat.resolvedTs.Load() {
					continue
				}

				// If we find an exact match, use it immediately
				if subStat.tableSpan.Equal(dispatcherSpan) {
					stat.subStat = subStat
					e.dispatcherMeta.dispatcherStats[dispatcherID] = stat
					e.addSubscriberToSubStat(subStat, dispatcherID, &Subscriber{notifyFunc: wrappedNotifier})
					e.dispatcherMeta.Unlock()
					log.Info("reuse existing subscription with exact span match",
						zap.Stringer("dispatcherID", dispatcherID),
						zap.String("dispatcherSpan", common.FormatTableSpan(dispatcherSpan)),
						zap.Uint64("startTs", startTs),
						zap.Uint64("subscriptionID", uint64(subStat.subID)),
						zap.String("subSpan", common.FormatTableSpan(subStat.tableSpan)),
						zap.Uint64("checkpointTs", subStat.checkpointTs.Load()))
					return true
				}

				// Track the smallest containing span that meets ts requirements
				// Note: this is still not bestMatch
				// for example, if we have a dispatcher with span [b, c),
				// it is hard to determine whether [a, d) or [b, h) is bestMatch without some statistics.
				if bestMatch == nil ||
					(bytes.Compare(subStat.tableSpan.StartKey, bestMatch.tableSpan.StartKey) >= 0 &&
						bytes.Compare(subStat.tableSpan.EndKey, bestMatch.tableSpan.EndKey) <= 0) {
					bestMatch = subStat
				}
			}
		}
	}
	if bestMatch != nil {
		stat.subStat = bestMatch
		e.dispatcherMeta.dispatcherStats[dispatcherID] = stat
		e.addSubscriberToSubStat(bestMatch, dispatcherID, &Subscriber{notifyFunc: wrappedNotifier})
		e.dispatcherMeta.Unlock()
		log.Info("reuse existing subscription with smallest containing span",
			zap.Stringer("dispatcherID", dispatcherID),
			zap.String("dispatcherSpan", common.FormatTableSpan(dispatcherSpan)),
			zap.Uint64("startTs", startTs),
			zap.Uint64("subscriptionID", uint64(bestMatch.subID)),
			zap.String("subSpan", common.FormatTableSpan(bestMatch.tableSpan)),
			zap.Uint64("resolvedTs", bestMatch.resolvedTs.Load()),
			zap.Uint64("checkpointTs", bestMatch.checkpointTs.Load()),
			zap.Bool("exactMatch", bestMatch.tableSpan.Equal(dispatcherSpan)))
		// when onlyReuse is true, we don't need a exact span match
		if onlyReuse {
			return true
		}
	} else {
		e.dispatcherMeta.Unlock()
		// when onlyReuse is true, we never create new subscription
		if onlyReuse {
			return false
		}
	}

	// cannot find an existing subscription with the same span, create a new subscription
	chIndex := common.HashTableSpan(dispatcherSpan, len(e.chs))
	subStat := &subscriptionStat{
		subID:     e.subClient.AllocSubscriptionID(),
		tableSpan: dispatcherSpan,
		dbIndex:   chIndex,
		eventCh:   e.chs[chIndex],
	}
	subStat.subscribers.Store(&subscribersWithIdleTime{
		subscribers: map[common.DispatcherID]*Subscriber{dispatcherID: {notifyFunc: wrappedNotifier}},
		idleTime:    0,
	})
	subStat.checkpointTs.Store(startTs)
	subStat.resolvedTs.Store(startTs)
	subStat.maxEventCommitTs.Store(0)
	if stat.subStat == nil {
		stat.subStat = subStat
	} else {
		stat.pendingSubStat = subStat
	}

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
		subStat.lastReceiveDMLTime.Store(time.Now().UnixMilli())
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
		now := time.Now().UnixMilli()
		subStat.lastAdvanceTime.Store(now)
		subStat.initialized.Store(true)
		// just do CompareAndSwap once, if failed, it means another goroutine has updated resolvedTs
		if subStat.resolvedTs.CompareAndSwap(currentResolvedTs, ts) {
			subscribersData := subStat.subscribers.Load()
			if subscribersData == nil {
				return
			}
			for _, subscriber := range subscribersData.subscribers {
				if !subscriber.isStopped {
					subscriber.notifyFunc(ts, subStat.maxEventCommitTs.Load())
				}
			}
			CounterResolved.Inc()
			metrics.EventStoreNotifyDispatcherDurationHist.Observe(float64(time.Since(start).Seconds()))
		}
	}

	serverConfig := config.GetGlobalServerConfig()
	resolvedTsAdvanceInterval := int64(serverConfig.KVClient.AdvanceIntervalInMs)
	// Note: don't hold any lock when call Subscribe
	e.subClient.Subscribe(subStat.subID, *dispatcherSpan, startTs, consumeKVEvents, advanceResolvedTs, resolvedTsAdvanceInterval, bdrMode)
	log.Info("new subscription created",
		zap.Stringer("dispatcherID", dispatcherID),
		zap.Uint64("startTs", startTs),
		zap.Uint64("subscriptionID", uint64(subStat.subID)),
		zap.String("subSpan", common.FormatTableSpan(subStat.tableSpan)))
	e.subscriptionChangeCh.In() <- SubscriptionChange{
		ChangeType:   SubscriptionChangeTypeAdd,
		SubID:        uint64(subStat.subID),
		Span:         dispatcherSpan,
		CheckpointTs: startTs,
		ResolvedTs:   startTs,
	}
	metrics.EventStoreSubscriptionGauge.Inc()
	return true
}

func (e *eventStore) UnregisterDispatcher(changefeedID common.ChangeFeedID, dispatcherID common.DispatcherID) {
	if e.closed.Load() {
		return
	}

	log.Info("unregister dispatcher", zap.Stringer("changefeedID", changefeedID), zap.Stringer("dispatcherID", dispatcherID))
	defer func() {
		log.Info("unregister dispatcher done", zap.Stringer("changefeedID", changefeedID), zap.Stringer("dispatcherID", dispatcherID))
	}()
	e.dispatcherMeta.Lock()
	if stat, ok := e.dispatcherMeta.dispatcherStats[dispatcherID]; ok {
		e.detachFromSubStat(dispatcherID, stat.subStat)
		e.detachFromSubStat(dispatcherID, stat.pendingSubStat)
		e.detachFromSubStat(dispatcherID, stat.removingSubStat)
		delete(e.dispatcherMeta.dispatcherStats, dispatcherID)
	}
	e.dispatcherMeta.Unlock()
}

func (e *eventStore) UpdateDispatcherCheckpointTs(
	dispatcherID common.DispatcherID,
	checkpointTs uint64,
) {
	if e.closed.Load() {
		return
	}

	e.dispatcherMeta.RLock()
	defer e.dispatcherMeta.RUnlock()

	dispatcherStat, ok := e.dispatcherMeta.dispatcherStats[dispatcherID]
	if !ok {
		return
	}
	dispatcherStat.checkpointTs = checkpointTs

	updateSubStatCheckpoint := func(subStat *subscriptionStat) {
		if subStat == nil {
			return
		}
		// calculate the new checkpoint ts of the subscription
		var newCheckpointTs uint64

		subscribersData := subStat.subscribers.Load()
		if subscribersData == nil {
			return
		}
		for id := range subscribersData.subscribers {
			dispatcherStat, ok := e.dispatcherMeta.dispatcherStats[id]
			if !ok {
				log.Warn("fail to find dispatcher", zap.Stringer("dispatcherID", id))
				continue
			}

			if newCheckpointTs == 0 || dispatcherStat.checkpointTs < newCheckpointTs {
				newCheckpointTs = dispatcherStat.checkpointTs
			}
		}

		resolvedTs := subStat.resolvedTs.Load()
		// newCheckpointTs maybe larger than subStat's resolvedTs,
		// because dispatcher may depend on multiple subStats.
		if newCheckpointTs > resolvedTs {
			newCheckpointTs = resolvedTs
		}
		if newCheckpointTs == 0 {
			return
		}
		oldCheckpointTs := subStat.checkpointTs.Load()
		if newCheckpointTs == oldCheckpointTs {
			return
		}
		if newCheckpointTs < oldCheckpointTs {
			log.Panic("should not happen",
				zap.Uint64("newCheckpointTs", newCheckpointTs),
				zap.Uint64("oldCheckpointTs", oldCheckpointTs))
		}
		// If there is no dml event after old checkpoint ts, then there is no data to be deleted.
		// So we can skip adding gc item.
		lastReceiveDMLTime := subStat.lastReceiveDMLTime.Load()
		if lastReceiveDMLTime > 0 {
			oldCheckpointPhysicalTime := oracle.GetTimeFromTS(oldCheckpointTs)
			if lastReceiveDMLTime >= oldCheckpointPhysicalTime.UnixMilli() {
				e.gcManager.addGCItem(
					subStat.dbIndex,
					uint64(subStat.subID),
					subStat.tableSpan.TableID,
					oldCheckpointTs,
					newCheckpointTs,
				)
			}
		}
		e.subscriptionChangeCh.In() <- SubscriptionChange{
			ChangeType:   SubscriptionChangeTypeUpdate,
			SubID:        uint64(subStat.subID),
			Span:         subStat.tableSpan,
			CheckpointTs: newCheckpointTs,
			ResolvedTs:   subStat.resolvedTs.Load(),
		}
		subStat.checkpointTs.Store(newCheckpointTs)
		if log.GetLevel() <= zap.DebugLevel {
			log.Debug("update checkpoint ts",
				zap.Any("dispatcherID", dispatcherID),
				zap.Uint64("subscriptionID", uint64(subStat.subID)),
				zap.Uint64("newCheckpointTs", newCheckpointTs),
				zap.Uint64("oldCheckpointTs", oldCheckpointTs))
		}
	}
	updateSubStatCheckpoint(dispatcherStat.subStat)
	updateSubStatCheckpoint(dispatcherStat.pendingSubStat)
	updateSubStatCheckpoint(dispatcherStat.removingSubStat)
}

func (e *eventStore) GetIterator(dispatcherID common.DispatcherID, dataRange common.DataRange) EventIterator {
	if e.closed.Load() {
		return nil
	}

	e.dispatcherMeta.RLock()
	stat, ok := e.dispatcherMeta.dispatcherStats[dispatcherID]
	if !ok {
		log.Warn("fail to find dispatcher", zap.Stringer("dispatcherID", dispatcherID))
		e.dispatcherMeta.RUnlock()
		return nil
	}

	tryGetDB := func(subStat *subscriptionStat, force bool) *pebble.DB {
		if subStat == nil {
			if force {
				log.Panic("subStat is nil, should not happen",
					zap.Stringer("dispatcherID", dispatcherID),
					zap.Int64("tableID", dataRange.Span.GetTableID()),
					zap.Uint64("commitTsStart", dataRange.CommitTsStart),
					zap.Uint64("commitTsEnd", dataRange.CommitTsEnd),
					zap.Uint64("lastScannedTxnStartTs", dataRange.LastScannedTxnStartTs))
			}
			return nil
		}
		checkpointTs := subStat.checkpointTs.Load()
		if dataRange.CommitTsStart < checkpointTs {
			log.Panic("dataRange startTs is smaller than subscriptionStat checkpointTs, it should not happen",
				zap.Stringer("dispatcherID", dispatcherID),
				zap.Int64("tableID", dataRange.Span.GetTableID()),
				zap.Uint64("commitTsStart", dataRange.CommitTsStart),
				zap.Uint64("commitTsEnd", dataRange.CommitTsEnd),
				zap.Uint64("lastScannedTxnStartTs", dataRange.LastScannedTxnStartTs),
				zap.Uint64("subStatCheckpointTs", checkpointTs),
				zap.Uint64("subStatResolvedTs", subStat.resolvedTs.Load()))
		}
		if dataRange.CommitTsEnd > subStat.resolvedTs.Load() {
			if force {
				log.Panic("dataRange endTs is larger than subscriptionStat resolvedTs, it should not happen",
					zap.Stringer("dispatcherID", dispatcherID),
					zap.Int64("tableID", dataRange.Span.GetTableID()),
					zap.Uint64("commitTsStart", dataRange.CommitTsStart),
					zap.Uint64("commitTsEnd", dataRange.CommitTsEnd),
					zap.Uint64("lastScannedTxnStartTs", dataRange.LastScannedTxnStartTs),
					zap.Uint64("subStatCheckpointTs", checkpointTs),
					zap.Uint64("subStatResolvedTs", subStat.resolvedTs.Load()))
			}
			return nil
		}
		return e.dbs[subStat.dbIndex]
	}

	// get from pendingSubStat first,
	// because its span is more close to dispatcher span if it's not nil
	var pendingSubStatReady bool
	var cleanRemovingSubStat bool
	db := tryGetDB(stat.pendingSubStat, false)
	subStat := stat.pendingSubStat
	if db != nil {
		pendingSubStatReady = true
	} else {
		db = tryGetDB(stat.subStat, stat.removingSubStat == nil)
		subStat = stat.subStat
		if db != nil {
			if stat.removingSubStat != nil {
				cleanRemovingSubStat = true
			}
		} else if stat.removingSubStat != nil {
			db = tryGetDB(stat.removingSubStat, true)
			subStat = stat.removingSubStat
		}
	}
	e.dispatcherMeta.RUnlock()

	if pendingSubStatReady {
		e.dispatcherMeta.Lock()
		// check stat again because we release the lock for a short period
		stat, ok := e.dispatcherMeta.dispatcherStats[dispatcherID]
		if ok {
			// GetIterator for the same dispatcher won't be called concurrently.
			// So if the dispatcher is not unregistered during the unlock period,
			// we can safely update stat.pendingSubStat as stat.subStat.
			e.stopReceiveEventFromSubStat(dispatcherID, stat.subStat)
			stat.removingSubStat = stat.subStat
			stat.subStat = stat.pendingSubStat
			stat.pendingSubStat = nil
		}
		e.dispatcherMeta.Unlock()
	}

	if cleanRemovingSubStat {
		e.dispatcherMeta.Lock()
		stat, ok := e.dispatcherMeta.dispatcherStats[dispatcherID]
		if ok && stat.removingSubStat != nil {
			e.detachFromSubStat(dispatcherID, stat.removingSubStat)
			stat.removingSubStat = nil
		}
		e.dispatcherMeta.Unlock()
	}

	// convert range before pass it to pebble: (startTs, endTs] is equal to [startTs + 1, endTs + 1)
	var start []byte
	if dataRange.LastScannedTxnStartTs != 0 {
		start = EncodeKeyPrefix(uint64(subStat.subID), stat.tableSpan.TableID, dataRange.CommitTsStart, dataRange.LastScannedTxnStartTs+1)
	} else {
		start = EncodeKeyPrefix(uint64(subStat.subID), stat.tableSpan.TableID, dataRange.CommitTsStart+1)
	}
	end := EncodeKeyPrefix(uint64(subStat.subID), stat.tableSpan.TableID, dataRange.CommitTsEnd+1)
	// TODO: optimize read performance
	// it's impossible return error here
	iter, _ := db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	decoder := e.decoderPool.Get().(*zstd.Decoder)
	startTime := time.Now()
	// todo: what happens if iter.First() returns false?
	_ = iter.First()
	metricEventStoreFirstReadDurationHistogram.Observe(time.Since(startTime).Seconds())
	metrics.EventStoreScanRequestsCount.Inc()

	needCheckSpan := true
	if stat.tableSpan.Equal(subStat.tableSpan) {
		needCheckSpan = false
	}

	return &eventStoreIter{
		tableSpan:     stat.tableSpan,
		needCheckSpan: needCheckSpan,
		innerIter:     iter,
		prevStartTs:   0,
		prevCommitTs:  0,
		startTs:       dataRange.CommitTsStart,
		endTs:         dataRange.CommitTsEnd,
		rowCount:      0,
		decoder:       decoder,
		decoderPool:   e.decoderPool,
	}
}

func (e *eventStore) GetLogCoordinatorNodeID() node.ID {
	return e.getCoordinatorInfo()
}

func (e *eventStore) detachFromSubStat(dispatcherID common.DispatcherID, subStat *subscriptionStat) {
	if subStat == nil {
		return
	}
	oldData := subStat.subscribers.Load()
	if oldData == nil || oldData.subscribers == nil {
		return
	}
	if _, ok := oldData.subscribers[dispatcherID]; !ok {
		return // Not found, nothing to do.
	}
	newMap := make(map[common.DispatcherID]*Subscriber, len(oldData.subscribers)-1)
	for id, sub := range oldData.subscribers {
		if id != dispatcherID {
			newMap[id] = sub
		}
	}
	idleTime := int64(0)
	if len(newMap) == 0 {
		idleTime = time.Now().UnixMilli()
	}
	newData := &subscribersWithIdleTime{subscribers: newMap, idleTime: idleTime}
	// It is safe to call Store without checking oldData here,
	// as all modifications to subStat are guarded by the dispatcherMeta lock.
	subStat.subscribers.Store(newData)
}

func (e *eventStore) stopReceiveEventFromSubStat(dispatcherID common.DispatcherID, subStat *subscriptionStat) {
	if subStat == nil {
		return
	}
	oldData := subStat.subscribers.Load()
	if oldData == nil || oldData.subscribers == nil {
		return
	}
	oldSub, ok := oldData.subscribers[dispatcherID]
	if !ok {
		return // Not found, nothing to do.
	}
	if oldSub.isStopped {
		return // Already stopped.
	}

	newMap := make(map[common.DispatcherID]*Subscriber, len(oldData.subscribers))
	for id, sub := range oldData.subscribers {
		newMap[id] = sub
	}

	newSub := &Subscriber{notifyFunc: oldSub.notifyFunc, isStopped: true}
	newMap[dispatcherID] = newSub

	newData := &subscribersWithIdleTime{
		subscribers: newMap,
		idleTime:    oldData.idleTime,
	}
	// It is safe to call Store without checking oldData here,
	// as all modifications to subStat are guarded by the dispatcherMeta lock.
	subStat.subscribers.Store(newData)
}

func (e *eventStore) addSubscriberToSubStat(subStat *subscriptionStat, dispatcherID common.DispatcherID, subscriber *Subscriber) {
	oldData := subStat.subscribers.Load()
	var oldMap map[common.DispatcherID]*Subscriber
	if oldData != nil {
		oldMap = oldData.subscribers
	}

	newMap := make(map[common.DispatcherID]*Subscriber, len(oldMap)+1)
	for id, sub := range oldMap {
		newMap[id] = sub
	}
	newMap[dispatcherID] = subscriber

	newData := &subscribersWithIdleTime{
		subscribers: newMap,
		idleTime:    0, // Not idle anymore.
	}
	// It is safe to call Store without checking oldData here,
	// as all modifications to subStat are guarded by the dispatcherMeta lock.
	subStat.subscribers.Store(newData)
}

func (e *eventStore) cleanObsoleteSubscriptions(ctx context.Context) error {
	ticker := time.NewTicker(1 * time.Minute)
	ttlInMsForMarkDeletion := int64(60 * 1000) // 1min
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			now := time.Now().UnixMilli()
			e.dispatcherMeta.Lock()
			for tableID, subStats := range e.dispatcherMeta.tableStats {
				for subID, subStat := range subStats {
					subData := subStat.subscribers.Load()
					if subData != nil && len(subData.subscribers) == 0 && subData.idleTime > 0 && now-subData.idleTime > ttlInMsForMarkDeletion {
						log.Info("clean obsolete subscription",
							zap.Uint64("subscriptionID", uint64(subID)),
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

func (e *eventStore) runMetricsCollector(ctx context.Context) error {
	storeMetricsTicker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-storeMetricsTicker.C:
			e.collectAndReportStoreMetrics()
		}
	}
}

func (e *eventStore) collectAndReportStoreMetrics() {
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

	pdCurrentTime := e.pdClock.CurrentTime()
	pdPhysicalTime := oracle.GetPhysical(pdCurrentTime)
	globalMinResolvedTs := uint64(0)
	uninitializedStatCount := 0
	e.dispatcherMeta.RLock()
	for _, subStats := range e.dispatcherMeta.tableStats {
		for _, subStat := range subStats {
			// resolved ts lag
			subResolvedTs := subStat.resolvedTs.Load()
			subResolvedPhysicalTime := oracle.ExtractPhysical(subResolvedTs)
			subResolvedTsLagInSec := float64(pdPhysicalTime-subResolvedPhysicalTime) / 1e3
			const largeResolvedTsLagInSecs = 30
			if subStat.initialized.Load() {
				if subResolvedTsLagInSec >= largeResolvedTsLagInSecs {
					lastReceiveDMLTimeRepr := "never"
					if lastReceiveDMLTime := subStat.lastReceiveDMLTime.Load(); lastReceiveDMLTime > 0 {
						lastReceiveDMLTimeRepr = time.UnixMilli(lastReceiveDMLTime).String()
					}
					log.Warn("resolved ts lag is too large for initialized subscription",
						zap.Uint64("subID", uint64(subStat.subID)),
						zap.Int64("tableID", subStat.tableSpan.TableID),
						zap.Uint64("resolvedTs", subResolvedTs),
						zap.Float64("resolvedLag(s)", subResolvedTsLagInSec),
						zap.Stringer("lastAdvanceTime", time.UnixMilli(subStat.lastAdvanceTime.Load())),
						zap.String("lastReceiveDMLTime", lastReceiveDMLTimeRepr),
						zap.String("tableSpan", common.FormatTableSpan(subStat.tableSpan)),
						zap.Uint64("checkpointTs", subStat.checkpointTs.Load()),
						zap.Uint64("maxEventCommitTs", subStat.maxEventCommitTs.Load()))
				}
			} else {
				uninitializedStatCount++
			}
			metrics.EventStoreSubscriptionResolvedTsLagHist.Observe(subResolvedTsLagInSec)
			if globalMinResolvedTs == 0 || subResolvedTs < globalMinResolvedTs {
				globalMinResolvedTs = subResolvedTs
			}
			// checkpoint ts lag
			subCheckpointTs := subStat.checkpointTs.Load()
			subCheckpointPhysicalTime := oracle.ExtractPhysical(subCheckpointTs)
			subCheckpointTsLagInSec := float64(pdPhysicalTime-subCheckpointPhysicalTime) / 1e3
			metrics.EventStoreSubscriptionDataGCLagHist.Observe(subCheckpointTsLagInSec)
		}
	}
	e.dispatcherMeta.RUnlock()
	if uninitializedStatCount > 0 {
		log.Info("found uninitialized subscriptions", zap.Int("count", uninitializedStatCount))
	}
	if globalMinResolvedTs == 0 {
		metrics.EventStoreResolvedTsLagGauge.Set(0)
		return
	}
	globalMinResolvedPhysicalTime := oracle.ExtractPhysical(globalMinResolvedTs)
	eventStoreResolvedTsLagInSec := float64(pdPhysicalTime-globalMinResolvedPhysicalTime) / 1e3
	metrics.EventStoreResolvedTsLagGauge.Set(eventStoreResolvedTsLagInSec)
}

func (e *eventStore) writeEvents(db *pebble.DB, events []eventWithCallback, encoder *zstd.Encoder) error {
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
					zap.Uint64("subscriptionID", uint64(event.subID)),
					zap.Int64("tableID", event.tableID))
				continue
			}

			compressionType := CompressionNone
			value := kv.Encode()
			if len(value) > e.compressionThreshold {
				value = encoder.EncodeAll(value, nil)
				compressionType = CompressionZSTD
				metrics.EventStoreCompressedRowsCount.Inc()
			}

			key := EncodeKey(uint64(event.subID), event.tableID, &kv, compressionType)
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

	decoder     *zstd.Decoder
	decoderPool *sync.Pool
}

func (iter *eventStoreIter) Next() (*common.RawKVEntry, bool) {
	rawKV := &common.RawKVEntry{}
	for {
		if !iter.innerIter.Valid() {
			return nil, false
		}
		key := iter.innerIter.Key()
		value := iter.innerIter.Value()

		_, compressionType := DecodeKeyMetas(key)
		var decodedValue []byte
		if compressionType == CompressionZSTD {
			var err error
			decodedValue, err = iter.decoder.DecodeAll(value, nil)
			if err != nil {
				log.Panic("failed to decompress value", zap.Error(err))
			}
		} else {
			decodedValue = value
		}

		err := rawKV.Decode(decodedValue)
		if err != nil {
			log.Panic("fail to decode raw kv entry", zap.Error(err))
		}
		metrics.EventStoreScanBytes.WithLabelValues("scanned").Add(float64(len(value)))
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
		metrics.EventStoreScanBytes.WithLabelValues("skipped").Add(float64(len(value)))
		iter.innerIter.Next()
	}
	isNewTxn := false
	// 2 PC transactions have different startTs and commitTs.
	// async-commit transactions have different startTs and may have the same commitTs.
	// at the moment, use commit-ts determine whether it is a new transaction, even though multiple
	// different transactions may be grouped together, to satisfy the resolved-ts semantics.
	if iter.prevCommitTs == 0 || (rawKV.StartTs != iter.prevStartTs || rawKV.CRTs != iter.prevCommitTs) {
		isNewTxn = true
	}
	iter.prevCommitTs = rawKV.CRTs
	iter.prevStartTs = rawKV.StartTs
	iter.rowCount++
	startTime := time.Now()
	iter.innerIter.Next()
	metricEventStoreNextReadDurationHistogram.Observe(time.Since(startTime).Seconds())
	return rawKV, isNewTxn
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
	iter.decoderPool.Put(iter.decoder)
	iter.innerIter = nil
	metricEventStoreCloseReadDurationHistogram.Observe(time.Since(startTime).Seconds())
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
				log.Info("add subscription for upload state", zap.Uint64("subscriptionID", change.SubID))
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
				log.Info("remove subscription from upload state", zap.Uint64("subscriptionID", change.SubID))
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
					log.Panic("cannot find subscription state", zap.Uint64("subscriptionID", change.SubID))
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
					log.Warn("cannot find subscription state", zap.Uint64("subscriptionID", change.SubID))
					continue
				}
				if change.CheckpointTs < tableState.Subscriptions[targetIndex].CheckpointTs ||
					change.ResolvedTs < tableState.Subscriptions[targetIndex].ResolvedTs {
					log.Panic("should not happen",
						zap.Uint64("subscriptionID", change.SubID),
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
			// just ignore messages fail to send
			if err := e.messageCenter.SendEvent(message); err != nil {
				log.Warn("send broadcast message to coordinator failed", zap.Error(err))
			}
		}
	}
}
