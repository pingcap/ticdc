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

package txnsink

import (
	"context"
	"encoding/binary"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/mysql/causality"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// TxnStore represents the in-memory store for DML events
// Structure: map[commitTs]map[startTs][]DMLEvent
type TxnStore struct {
	store map[uint64]map[uint64][]*commonEvent.DMLEvent
	mu    sync.RWMutex
}

// NewTxnStore creates a new TxnStore instance
func NewTxnStore() *TxnStore {
	return &TxnStore{
		store: make(map[uint64]map[uint64][]*commonEvent.DMLEvent),
	}
}

// AddEvent adds a DML event to the store
func (ts *TxnStore) AddEvent(event *commonEvent.DMLEvent) {
	log.Info("txnSink: add event",
		zap.Uint64("commitTs", event.CommitTs),
		zap.Uint64("startTs", event.StartTs),
		zap.Int64("tableID", event.GetTableID()),
		zap.Int32("rowCount", event.Len()))
	ts.mu.Lock()
	defer ts.mu.Unlock()

	commitTs := event.CommitTs
	startTs := event.StartTs

	if ts.store[commitTs] == nil {
		ts.store[commitTs] = make(map[uint64][]*commonEvent.DMLEvent)
	}
	ts.store[commitTs][startTs] = append(ts.store[commitTs][startTs], event)
}

// GetEventsByCheckpointTs retrieves all events with commitTs <= checkpointTs
// Returns txnGroups sorted by commitTs in ascending order
func (ts *TxnStore) GetEventsByCheckpointTs(checkpointTs uint64) []*TxnGroup {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	var groups []*TxnGroup
	for commitTs, startTsMap := range ts.store {
		if commitTs <= checkpointTs {
			for startTs, events := range startTsMap {
				groups = append(groups, &TxnGroup{
					CommitTs: commitTs,
					StartTs:  startTs,
					Events:   events,
				})
			}
		}
	}

	// Sort groups by commitTs in ascending order
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].CommitTs < groups[j].CommitTs
	})

	return groups
}

// RemoveEventsByCheckpointTs removes all events with commitTs <= checkpointTs
func (ts *TxnStore) RemoveEventsByCheckpointTs(checkpointTs uint64) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	for commitTs := range ts.store {
		if commitTs <= checkpointTs {
			delete(ts.store, commitTs)
		}
	}
}

// TxnGroup represents a complete transaction
type TxnGroup struct {
	CommitTs uint64
	StartTs  uint64
	Events   []*commonEvent.DMLEvent

	// PostFlushFuncs are functions to be executed after the transaction is flushed
	PostFlushFuncs []func()
}

// GetTxnKey returns a unique key for the transaction
func (tg *TxnGroup) GetTxnKey() string {
	return strconv.FormatUint(tg.CommitTs, 10) + "_" + strconv.FormatUint(tg.StartTs, 10)
}

// ExtractKeys extracts all affected keys from the transaction
func (tg *TxnGroup) ExtractKeys() map[string]struct{} {
	keys := make(map[string]struct{})
	for _, event := range tg.Events {
		for _, rowKey := range event.RowKeys {
			keys[string(rowKey)] = struct{}{}
		}
	}
	return keys
}

// AddPostFlushFunc adds a function to be executed after the transaction is flushed
func (tg *TxnGroup) AddPostFlushFunc(f func()) {
	tg.PostFlushFuncs = append(tg.PostFlushFuncs, f)
}

// PostFlush executes all post-flush functions
func (tg *TxnGroup) PostFlush() {
	for _, f := range tg.PostFlushFuncs {
		f()
	}
}

// TxnSQL represents the SQL statements for a transaction
type TxnSQL struct {
	TxnGroup *TxnGroup
	SQL      string
	Args     []interface{}
	Keys     map[string]struct{}
}

// BlockStrategy is the strategy to handle the situation when the cache is full.
type BlockStrategy string

const (
	// BlockStrategyWaitAvailable means the cache will block until there is an available slot.
	BlockStrategyWaitAvailable BlockStrategy = "waitAvailable"
	// BlockStrategyWaitEmpty means the cache will block until all cached txns are consumed.
	BlockStrategyWaitEmpty = "waitEmpty"
)

// TxnCacheOption is the option for creating a cache for resolved txns.
type TxnCacheOption struct {
	// Count controls the number of caches, txns in different caches could be executed concurrently.
	Count int
	// Size controls the max number of txns a cache can hold.
	Size int
	// BlockStrategy controls the strategy when the cache is full.
	BlockStrategy BlockStrategy
}

// txnCache interface for TxnGroup
type txnCache interface {
	// addTxnGroup adds a txn group to the Cache.
	addTxnGroup(txnGroup *TxnGroup) bool
	// out returns a unlimited channel to receive txn groups which are ready to be executed.
	out() *chann.UnlimitedChannel[*TxnGroup, any]
}

// boundedTxnCache is a cache which has a limit on the number of txn groups it can hold.
type boundedTxnCache struct {
	ch        *chann.UnlimitedChannel[*TxnGroup, any]
	upperSize int
}

func (w *boundedTxnCache) addTxnGroup(txnGroup *TxnGroup) bool {
	if w.ch.Len() > w.upperSize {
		return false
	}
	w.ch.Push(txnGroup)
	return true
}

func (w *boundedTxnCache) out() *chann.UnlimitedChannel[*TxnGroup, any] {
	return w.ch
}

// boundedTxnCacheWithBlock is a special bounded cache. Once the cache
// is full, it will block until all cached txn groups are consumed.
type boundedTxnCacheWithBlock struct {
	ch        *chann.UnlimitedChannel[*TxnGroup, any]
	isBlocked atomic.Bool
	upperSize int
}

func (w *boundedTxnCacheWithBlock) addTxnGroup(txnGroup *TxnGroup) bool {
	if w.isBlocked.Load() && w.ch.Len() <= 0 {
		w.isBlocked.Store(false)
	}

	if !w.isBlocked.Load() {
		if w.ch.Len() > w.upperSize {
			w.isBlocked.CompareAndSwap(false, true)
			return false
		}
		w.ch.Push(txnGroup)
		return true
	}
	return false
}

func (w *boundedTxnCacheWithBlock) out() *chann.UnlimitedChannel[*TxnGroup, any] {
	return w.ch
}

func newTxnCache(opt TxnCacheOption) txnCache {
	if opt.Size <= 0 {
		log.Panic("TxnCacheOption.Size should be greater than 0, please report a bug")
	}

	switch opt.BlockStrategy {
	case BlockStrategyWaitAvailable:
		return &boundedTxnCache{ch: chann.NewUnlimitedChannel[*TxnGroup, any](nil, nil), upperSize: opt.Size}
	case BlockStrategyWaitEmpty:
		return &boundedTxnCacheWithBlock{ch: chann.NewUnlimitedChannel[*TxnGroup, any](nil, nil), upperSize: opt.Size}
	default:
		return nil
	}
}

// ConflictKeysForTxnGroup generates conflict keys for a transaction group
func ConflictKeysForTxnGroup(txnGroup *TxnGroup) []uint64 {
	if len(txnGroup.Events) == 0 {
		return nil
	}

	hashRes := make(map[uint64]struct{})
	hasher := fnv.New32a()

	// Iterate through all events in the transaction group
	for _, event := range txnGroup.Events {
		// Iterate through all rows in the event
		event.Rewind()
		for {
			rowChange, ok := event.GetNextRow()
			if !ok {
				break
			}

			// Generate keys for each row
			keys := genRowKeysForTxnGroup(rowChange, event.TableInfo, event.DispatcherID)
			for _, key := range keys {
				if n, err := hasher.Write(key); n != len(key) || err != nil {
					log.Panic("transaction key hash fail")
				}
				hashRes[uint64(hasher.Sum32())] = struct{}{}
				hasher.Reset()
			}
		}
		event.Rewind()
	}

	keys := make([]uint64, 0, len(hashRes))
	for key := range hashRes {
		keys = append(keys, key)
	}
	return keys
}

// genRowKeysForTxnGroup generates row keys for a row change in transaction group
func genRowKeysForTxnGroup(rowChange commonEvent.RowChange, tableInfo *common.TableInfo, dispatcherID common.DispatcherID) [][]byte {
	var keys [][]byte

	if !rowChange.Row.IsEmpty() {
		for iIdx, idxColID := range tableInfo.GetIndexColumns() {
			key := genKeyListForTxnGroup(&rowChange.Row, iIdx, idxColID, dispatcherID, tableInfo)
			if len(key) == 0 {
				continue
			}
			keys = append(keys, key)
		}
	}
	if !rowChange.PreRow.IsEmpty() {
		for iIdx, idxColID := range tableInfo.GetIndexColumns() {
			key := genKeyListForTxnGroup(&rowChange.PreRow, iIdx, idxColID, dispatcherID, tableInfo)
			if len(key) == 0 {
				continue
			}
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		// use dispatcherID as key if no key generated (no PK/UK),
		// no concurrence for rows in the same dispatcher.
		log.Debug("Use dispatcherID as the key", zap.Any("dispatcherID", dispatcherID))
		tableKey := make([]byte, 8)
		binary.BigEndian.PutUint64(tableKey, uint64(dispatcherID.GetLow()))
		keys = [][]byte{tableKey}
	}
	return keys
}

// genKeyListForTxnGroup generates a key list for a row in transaction group
func genKeyListForTxnGroup(
	row *chunk.Row, iIdx int, idxColID []int64, dispatcherID common.DispatcherID, tableInfo *common.TableInfo,
) []byte {
	var key []byte
	for _, colID := range idxColID {
		info, ok := tableInfo.GetColumnInfo(colID)
		// If the index contain generated column, we can't use this key to detect conflict with other DML,
		if !ok || info == nil || info.IsGenerated() {
			return nil
		}
		offset, ok := tableInfo.GetRowColumnsOffset()[colID]
		if !ok {
			log.Warn("can't find column offset", zap.Int64("colID", colID), zap.String("colName", info.Name.O))
			return nil
		}
		value := common.ExtractColVal(row, info, offset)
		// if a column value is null, we can ignore this index
		if value == nil {
			return nil
		}

		val := common.ColumnValueString(value)
		if columnNeeds2LowerCase(info.GetType(), info.GetCollate()) {
			val = strings.ToLower(val)
		}

		key = append(key, []byte(val)...)
		key = append(key, 0)
	}
	if len(key) == 0 {
		return nil
	}
	tableKey := make([]byte, 16)
	binary.BigEndian.PutUint64(tableKey[:8], uint64(iIdx))
	binary.BigEndian.PutUint64(tableKey[8:], dispatcherID.GetLow())
	key = append(key, tableKey...)
	return key
}

// columnNeeds2LowerCase checks if a column needs to be converted to lowercase
func columnNeeds2LowerCase(mysqlType byte, collation string) bool {
	switch mysqlType {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob:
		return collationNeeds2LowerCase(collation)
	}
	return false
}

// collationNeeds2LowerCase checks if a collation needs to be converted to lowercase
func collationNeeds2LowerCase(collation string) bool {
	return strings.HasSuffix(collation, "_ci")
}

// ConflictDetector manages transaction conflicts for parallel processing
type ConflictDetector struct {
	// resolvedTxnCaches are used to cache resolved transactions.
	resolvedTxnCaches []txnCache

	// slots are used to find all unfinished transactions
	// conflicting with an incoming transactions.
	slots *causality.Slots

	// nextCacheID is used to dispatch transactions round-robin.
	nextCacheID atomic.Int64

	notifiedNodes *chann.DrainableChann[func()]

	changefeedID                 common.ChangeFeedID
	metricConflictDetectDuration prometheus.Observer
}

// NewConflictDetector creates a new ConflictDetector instance
func NewConflictDetector(changefeedID common.ChangeFeedID, maxConcurrentTxns int) *ConflictDetector {
	opt := TxnCacheOption{
		Count:         maxConcurrentTxns, // Default worker count
		Size:          1024,
		BlockStrategy: BlockStrategyWaitEmpty,
	}

	ret := &ConflictDetector{
		resolvedTxnCaches:            make([]txnCache, opt.Count),
		slots:                        causality.NewSlots(16 * 1024), // Default slot count
		notifiedNodes:                chann.NewAutoDrainChann[func()](),
		metricConflictDetectDuration: metrics.ConflictDetectDuration.WithLabelValues(changefeedID.Namespace(), changefeedID.Name()),
		changefeedID:                 changefeedID,
	}
	for i := 0; i < opt.Count; i++ {
		ret.resolvedTxnCaches[i] = newTxnCache(opt)
	}
	log.Info("txn conflict detector initialized", zap.Int("cacheCount", opt.Count),
		zap.Int("cacheSize", opt.Size), zap.String("BlockStrategy", string(opt.BlockStrategy)))
	return ret
}

// AddTxnGroup adds a transaction group to the conflict detector
func (cd *ConflictDetector) AddTxnGroup(txnGroup *TxnGroup) {
	start := time.Now()
	hashes := ConflictKeysForTxnGroup(txnGroup)
	node := cd.slots.AllocNode(hashes)

	txnGroup.AddPostFlushFunc(func() {
		cd.slots.Remove(node)
	})

	node.TrySendToTxnCache = func(cacheID int64) bool {
		// Try sending this txn group to related cache as soon as all dependencies are resolved.
		ok := cd.sendTxnGroupToCache(txnGroup, cacheID)
		if ok {
			cd.metricConflictDetectDuration.Observe(time.Since(start).Seconds())
		}
		return ok
	}
	node.RandCacheID = func() int64 {
		return cd.nextCacheID.Add(1) % int64(len(cd.resolvedTxnCaches))
	}
	node.OnNotified = func(callback func()) {
		// TODO:find a better way to handle the panic
		defer func() {
			if r := recover(); r != nil {
				log.Warn("failed to send notification, channel might be closed", zap.Any("error", r))
			}
		}()
		cd.notifiedNodes.In() <- callback
	}
	cd.slots.Add(node)
}

// GetOutChByCacheID returns the output channel by cacheID
func (cd *ConflictDetector) GetOutChByCacheID(id int) *chann.UnlimitedChannel[*TxnGroup, any] {
	return cd.resolvedTxnCaches[id].out()
}

// Run starts the conflict detector
func (cd *ConflictDetector) Run(ctx context.Context) error {
	defer func() {
		metrics.ConflictDetectDuration.DeleteLabelValues(cd.changefeedID.Namespace(), cd.changefeedID.Name())
		cd.closeCache()
	}()

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case notifyCallback := <-cd.notifiedNodes.Out():
			if notifyCallback != nil {
				notifyCallback()
			}
		}
	}
}

// Close closes the conflict detector
func (cd *ConflictDetector) Close() {
	cd.notifiedNodes.CloseAndDrain()
}

// sendTxnGroupToCache should not call txn.Callback if it returns an error.
func (cd *ConflictDetector) sendTxnGroupToCache(txnGroup *TxnGroup, id int64) bool {
	cache := cd.resolvedTxnCaches[id]
	ok := cache.addTxnGroup(txnGroup)
	return ok
}

func (cd *ConflictDetector) closeCache() {
	// the unlimited channel should be closed when quit wait group, otherwise txnWorker will be blocked
	for _, cache := range cd.resolvedTxnCaches {
		cache.out().Close()
	}
}

// TxnSinkConfig represents the configuration for txnSink
type TxnSinkConfig struct {
	MaxConcurrentTxns int
	BatchSize         int
	FlushInterval     int // milliseconds
	MaxSQLBatchSize   int // maximum size of SQL batch in bytes
}
