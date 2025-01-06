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

package schemastore

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// The parent folder to store schema data
const dataDir = "schema_store"

// persistentStorage stores the following kinds of data on disk:
//  1. table info and database info from upstream snapshot
//  2. incremental ddl jobs
//  3. metadata which describes the valid data range on disk
type persistentStorage struct {
	pdCli pd.Client

	kvStorage kv.Storage

	db *pebble.DB

	mu sync.RWMutex

	// the current gcTs on disk
	gcTs uint64

	upperBound UpperBoundMeta

	upperBoundChanged bool

	tableMap map[int64]*BasicTableInfo

	partitionMap map[int64]BasicPartitionInfo

	// schemaID -> database info
	// it contains all databases and deleted databases
	// will only be removed when its delete version is smaller than gc ts
	databaseMap map[int64]*BasicDatabaseInfo

	// table id -> a sorted list of finished ts for the table's ddl events
	tablesDDLHistory map[int64][]uint64

	// it has two use cases:
	// 1. store the ddl events need to send to a table dispatcher
	//    Note: some ddl events in the history may never be send,
	//          for example the create table ddl, truncate table ddl(usually the first event)
	// 2. build table info store for a table
	tableTriggerDDLHistory []uint64

	// tableID -> versioned store
	// it just contains tables which is used by dispatchers
	tableInfoStoreMap map[int64]*versionedTableInfoStore

	// tableID -> total registered count
	tableRegisteredCount map[int64]int
}

func exists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	log.Fatal("check path failed", zap.Error(err))
	return true
}

func openDB(dbPath string) *pebble.DB {
	opts := &pebble.Options{
		DisableWAL:   true,
		MemTableSize: 8 << 20,
	}
	opts.Levels = make([]pebble.LevelOptions, 7)
	for i := 0; i < len(opts.Levels); i++ {
		l := &opts.Levels[i]
		l.BlockSize = 64 << 10       // 64 KB
		l.IndexBlockSize = 256 << 10 // 256 KB
		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		l.TargetFileSize = 8 << 20 // 8 MB
		l.Compression = pebble.SnappyCompression
		l.EnsureDefaults()
	}
	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		log.Fatal("open db failed", zap.Error(err))
	}
	return db
}

func newPersistentStorage(
	ctx context.Context,
	root string,
	pdCli pd.Client,
	storage kv.Storage,
) *persistentStorage {
	gcSafePoint, err := pdCli.UpdateServiceGCSafePoint(ctx, "cdc-new-store", 0, 0)
	if err != nil {
		log.Panic("get ts failed", zap.Error(err))
	}

	dbPath := fmt.Sprintf("%s/%s", root, dataDir)
	// FIXME: currently we don't try to reuse data at restart, when we need, just remove the following line
	if err := os.RemoveAll(dbPath); err != nil {
		log.Panic("fail to remove path")
	}

	dataStorage := &persistentStorage{
		pdCli:                  pdCli,
		kvStorage:              storage,
		tableMap:               make(map[int64]*BasicTableInfo),
		partitionMap:           make(map[int64]BasicPartitionInfo),
		databaseMap:            make(map[int64]*BasicDatabaseInfo),
		tablesDDLHistory:       make(map[int64][]uint64),
		tableTriggerDDLHistory: make([]uint64, 0),
		tableInfoStoreMap:      make(map[int64]*versionedTableInfoStore),
		tableRegisteredCount:   make(map[int64]int),
	}

	isDataReusable := false
	if exists(dbPath) {
		isDataReusable = true
		db := openDB(dbPath)
		// check whether the data on disk is reusable
		gcTs, err := readGcTs(db)
		if err != nil {
			isDataReusable = false
		}
		if gcSafePoint < gcTs {
			log.Panic("gc safe point should never go back")
		}
		upperBound, err := readUpperBoundMeta(db)
		if err != nil {
			isDataReusable = false
		}
		if gcSafePoint >= upperBound.ResolvedTs {
			isDataReusable = false
		}

		if isDataReusable {
			dataStorage.db = db
			dataStorage.gcTs = gcTs
			dataStorage.upperBound = upperBound
			dataStorage.initializeFromDisk()
		} else {
			db.Close()
		}
	}
	if !isDataReusable {
		dataStorage.initializeFromKVStorage(dbPath, storage, gcSafePoint)
	}

	go func() {
		dataStorage.gc(ctx)
	}()

	go func() {
		dataStorage.persistUpperBoundPeriodically(ctx)
	}()

	return dataStorage
}

func (p *persistentStorage) initializeFromKVStorage(dbPath string, storage kv.Storage, gcTs uint64) {
	now := time.Now()
	if err := os.RemoveAll(dbPath); err != nil {
		log.Fatal("fail to remove path in initializeFromKVStorage")
	}
	p.db = openDB(dbPath)

	log.Info("schema store initialize from kv storage begin",
		zap.Uint64("snapTs", gcTs))

	var err error
	if p.databaseMap, p.tableMap, err = writeSchemaSnapshotAndMeta(p.db, storage, gcTs, true); err != nil {
		// TODO: retry
		log.Fatal("fail to initialize from kv snapshot")
	}
	p.gcTs = gcTs
	p.upperBound = UpperBoundMeta{
		FinishedDDLTs: 0,
		ResolvedTs:    gcTs,
	}
	writeUpperBoundMeta(p.db, p.upperBound)
	log.Info("schema store initialize from kv storage done",
		zap.Int("databaseMapLen", len(p.databaseMap)),
		zap.Int("tableMapLen", len(p.tableMap)),
		zap.Any("duration(s)", time.Since(now).Seconds()))
}

func (p *persistentStorage) initializeFromDisk() {
	cleanObsoleteData(p.db, 0, p.gcTs)

	storageSnap := p.db.NewSnapshot()
	defer storageSnap.Close()

	var err error
	if p.databaseMap, err = loadDatabasesInKVSnap(storageSnap, p.gcTs); err != nil {
		log.Fatal("load database info from disk failed")
	}

	if p.tableMap, p.partitionMap, err = loadTablesInKVSnap(storageSnap, p.gcTs, p.databaseMap); err != nil {
		log.Fatal("load tables in kv snapshot failed")
	}

	if p.tablesDDLHistory, p.tableTriggerDDLHistory, err = loadAndApplyDDLHistory(
		storageSnap,
		p.gcTs,
		p.upperBound.FinishedDDLTs,
		p.databaseMap,
		p.tableMap,
		p.partitionMap); err != nil {
		log.Fatal("fail to initialize from disk")
	}
}

func (p *persistentStorage) close() error {
	return p.db.Close()
}

// getAllPhysicalTables returns all physical tables in the snapshot
// caller must ensure current resolve ts is larger than snapTs
func (p *persistentStorage) getAllPhysicalTables(snapTs uint64, tableFilter filter.Filter) ([]commonEvent.Table, error) {
	storageSnap := p.db.NewSnapshot()
	defer storageSnap.Close()

	p.mu.Lock()
	if snapTs < p.gcTs {
		return nil, fmt.Errorf("snapTs %d is smaller than gcTs %d", snapTs, p.gcTs)
	}
	gcTs := p.gcTs
	p.mu.Unlock()

	start := time.Now()
	defer func() {
		log.Info("getAllPhysicalTables finish",
			zap.Uint64("snapTs", snapTs),
			zap.Any("duration(s)", time.Since(start).Seconds()))
	}()
	return loadAllPhysicalTablesAtTs(storageSnap, gcTs, snapTs, tableFilter)
}

// only return when table info is initialized
func (p *persistentStorage) registerTable(tableID int64, startTs uint64) error {
	p.mu.Lock()
	if startTs < p.gcTs {
		p.mu.Unlock()
		return fmt.Errorf("startTs %d is smaller than gcTs %d", startTs, p.gcTs)
	}
	p.tableRegisteredCount[tableID] += 1
	store, ok := p.tableInfoStoreMap[tableID]
	if !ok {
		store = newEmptyVersionedTableInfoStore(tableID)
		p.tableInfoStoreMap[tableID] = store
	}
	p.mu.Unlock()

	if !ok {
		return p.buildVersionedTableInfoStore(store)
	}

	store.waitTableInfoInitialized()

	// Note: no need to check startTs < gcTs here again because if it is true, getTableInfo will failed later.

	return nil
}

func (p *persistentStorage) unregisterTable(tableID int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tableRegisteredCount[tableID] -= 1
	if p.tableRegisteredCount[tableID] <= 0 {
		if _, ok := p.tableInfoStoreMap[tableID]; !ok {
			return fmt.Errorf("table %d not found", tableID)
		}
		delete(p.tableInfoStoreMap, tableID)
		log.Info("unregister table",
			zap.Int64("tableID", tableID))
	}
	return nil
}

func (p *persistentStorage) getTableInfo(tableID int64, ts uint64) (*common.TableInfo, error) {
	p.mu.RLock()
	store, ok := p.tableInfoStoreMap[tableID]
	if !ok {
		p.mu.RUnlock()
		return nil, fmt.Errorf("table %d not found", tableID)
	}
	p.mu.RUnlock()
	return store.getTableInfo(ts)
}

func (p *persistentStorage) forceGetTableInfo(tableID int64, ts uint64) (*common.TableInfo, error) {
	p.mu.RLock()
	// if there is already a store, it must contain all table info on disk, so we can use it directly
	if store, ok := p.tableInfoStoreMap[tableID]; ok {
		p.mu.RUnlock()
		return store.getTableInfo(ts)
	}
	p.mu.RUnlock()
	// build a temp store to get table info
	store := newEmptyVersionedTableInfoStore(tableID)
	p.buildVersionedTableInfoStore(store)
	return store.getTableInfo(ts)
}

// TODO: this may consider some shouldn't be send ddl, like create table, does it matter?
func (p *persistentStorage) getMaxEventCommitTs(tableID int64, ts uint64) uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.tablesDDLHistory[tableID]) == 0 {
		return 0
	}
	index := sort.Search(len(p.tablesDDLHistory[tableID]), func(i int) bool {
		return p.tablesDDLHistory[tableID][i] > ts
	})
	if index == 0 {
		return 0
	}
	return p.tablesDDLHistory[tableID][index-1]
}

// TODO: not all ddl in p.tablesDDLHistory should be sent to the dispatcher, verify dispatcher will set the right range
func (p *persistentStorage) fetchTableDDLEvents(tableID int64, tableFilter filter.Filter, start, end uint64) ([]commonEvent.DDLEvent, error) {
	// TODO: check a dispatcher won't fetch the ddl events that create it(create table/rename table)
	p.mu.RLock()
	// fast check
	history := p.tablesDDLHistory[tableID]
	if len(history) == 0 || start >= history[len(history)-1] {
		p.mu.RUnlock()
		return nil, nil
	}
	index := sort.Search(len(history), func(i int) bool {
		return history[i] > start
	})
	if index == len(history) {
		log.Panic("should not happen")
	}
	// copy all target ts to a new slice
	allTargetTs := make([]uint64, 0)
	for i := index; i < len(history); i++ {
		if history[i] <= end {
			allTargetTs = append(allTargetTs, history[i])
		}
	}
	p.mu.RUnlock()

	storageSnap := p.db.NewSnapshot()
	defer storageSnap.Close()

	p.mu.RLock()
	if start < p.gcTs {
		p.mu.RUnlock()
		return nil, fmt.Errorf("startTs %d is smaller than gcTs %d", start, p.gcTs)
	}
	p.mu.RUnlock()

	// TODO: if the first event is a create table ddl, return error?
	events := make([]commonEvent.DDLEvent, 0, len(allTargetTs))
	for _, ts := range allTargetTs {
		rawEvent := readPersistedDDLEvent(storageSnap, ts)
		// TODO: if ExtraSchemaName and other fields are empty, does it cause any problem?
		if tableFilter != nil &&
			tableFilter.ShouldDiscardDDL(model.ActionType(rawEvent.Type), rawEvent.CurrentSchemaName, rawEvent.CurrentTableName) &&
			tableFilter.ShouldDiscardDDL(model.ActionType(rawEvent.Type), rawEvent.PrevSchemaName, rawEvent.PrevTableName) {
			continue
		}
		events = append(events, buildDDLEvent(&rawEvent, tableFilter))
	}
	// log.Info("fetchTableDDLEvents",
	// 	zap.Int64("tableID", tableID),
	// 	zap.Uint64("start", start),
	// 	zap.Uint64("end", end),
	// 	zap.Any("history", history),
	// 	zap.Any("allTargetTs", allTargetTs))

	return events, nil
}

func (p *persistentStorage) fetchTableTriggerDDLEvents(tableFilter filter.Filter, start uint64, limit int) ([]commonEvent.DDLEvent, error) {
	// fast check
	p.mu.RLock()
	if len(p.tableTriggerDDLHistory) == 0 || start >= p.tableTriggerDDLHistory[len(p.tableTriggerDDLHistory)-1] {
		p.mu.RUnlock()
		return nil, nil
	}
	p.mu.RUnlock()

	events := make([]commonEvent.DDLEvent, 0)
	nextStartTs := start
	for {
		allTargetTs := make([]uint64, 0, limit)
		p.mu.RLock()
		// log.Debug("fetchTableTriggerDDLEvents in persistentStorage",
		// 	zap.Any("start", start),
		// 	zap.Int("limit", limit),
		// 	zap.Any("tableTriggerDDLHistory", p.tableTriggerDDLHistory))
		index := sort.Search(len(p.tableTriggerDDLHistory), func(i int) bool {
			return p.tableTriggerDDLHistory[i] > nextStartTs
		})
		// no more events to read
		if index == len(p.tableTriggerDDLHistory) {
			p.mu.RUnlock()
			return events, nil
		}
		for i := index; i < len(p.tableTriggerDDLHistory); i++ {
			allTargetTs = append(allTargetTs, p.tableTriggerDDLHistory[i])
			if len(allTargetTs) >= limit-len(events) {
				break
			}
		}
		p.mu.RUnlock()

		if len(allTargetTs) == 0 {
			return events, nil
		}

		// ensure the order: get target ts -> get storage snap -> check gc ts
		storageSnap := p.db.NewSnapshot()
		p.mu.RLock()
		if allTargetTs[0] < p.gcTs {
			p.mu.RUnlock()
			return nil, fmt.Errorf("startTs %d is smaller than gcTs %d", allTargetTs[0], p.gcTs)
		}
		p.mu.RUnlock()
		for _, ts := range allTargetTs {
			rawEvent := readPersistedDDLEvent(storageSnap, ts)
			if tableFilter != nil {
				if rawEvent.Type == byte(model.ActionCreateTables) {
					allFiltered := true
					for _, tableInfo := range rawEvent.MultipleTableInfos {
						if !tableFilter.ShouldDiscardDDL(model.ActionType(rawEvent.Type), rawEvent.CurrentSchemaName, tableInfo.Name.O) {
							allFiltered = false
							break
						}
					}
					if allFiltered {
						continue
					}
				} else {
					if tableFilter.ShouldDiscardDDL(model.ActionType(rawEvent.Type), rawEvent.CurrentSchemaName, rawEvent.CurrentTableName) &&
						tableFilter.ShouldDiscardDDL(model.ActionType(rawEvent.Type), rawEvent.PrevSchemaName, rawEvent.PrevTableName) {
						continue
					}
				}
			}
			events = append(events, buildDDLEvent(&rawEvent, tableFilter))
		}
		storageSnap.Close()
		if len(events) >= limit {
			return events, nil
		}
		nextStartTs = allTargetTs[len(allTargetTs)-1]
	}
}

func (p *persistentStorage) buildVersionedTableInfoStore(store *versionedTableInfoStore) error {
	tableID := store.getTableID()
	// get snapshot from disk before get current gc ts to make sure data is not deleted by gc process
	storageSnap := p.db.NewSnapshot()
	defer storageSnap.Close()

	p.mu.RLock()
	kvSnapVersion := p.gcTs
	var allDDLFinishedTs []uint64
	allDDLFinishedTs = append(allDDLFinishedTs, p.tablesDDLHistory[tableID]...)
	p.mu.RUnlock()

	if err := addTableInfoFromKVSnap(store, kvSnapVersion, storageSnap); err != nil {
		return err
	}

	for _, version := range allDDLFinishedTs {
		ddlEvent := readPersistedDDLEvent(storageSnap, version)
		store.applyDDLFromPersistStorage(&ddlEvent)
	}
	store.setTableInfoInitialized()
	return nil
}

func addTableInfoFromKVSnap(
	store *versionedTableInfoStore,
	kvSnapVersion uint64,
	snap *pebble.Snapshot,
) error {
	tableInfo := readTableInfoInKVSnap(snap, store.getTableID(), kvSnapVersion)
	if tableInfo != nil {
		store.addInitialTableInfo(tableInfo)
	}
	return nil
}

func (p *persistentStorage) gc(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Minute)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			gcSafePoint, err := p.pdCli.UpdateServiceGCSafePoint(ctx, "cdc-new-store", 0, 0)
			if err != nil {
				log.Warn("get ts failed", zap.Error(err))
				continue
			}
			p.doGc(gcSafePoint)
		}
	}
}

func (p *persistentStorage) doGc(gcTs uint64) error {
	p.mu.Lock()
	if gcTs > p.upperBound.ResolvedTs {
		log.Panic("gc safe point is larger than resolvedTs",
			zap.Uint64("gcTs", gcTs),
			zap.Uint64("resolvedTs", p.upperBound.ResolvedTs))
	}
	if gcTs <= p.gcTs {
		p.mu.Unlock()
		return nil
	}
	oldGcTs := p.gcTs
	p.mu.Unlock()

	serverConfig := config.GetGlobalServerConfig()
	if !serverConfig.Debug.SchemaStore.EnableGC {
		log.Info("gc is disabled",
			zap.Uint64("gcTs", gcTs))
		return nil
	}

	start := time.Now()
	_, _, err := writeSchemaSnapshotAndMeta(p.db, p.kvStorage, gcTs, false)
	if err != nil {
		log.Warn("fail to write kv snapshot during gc",
			zap.Uint64("gcTs", gcTs))
		// TODO: return err and retry?
		return nil
	}
	log.Info("gc finish write schema snapshot",
		zap.Uint64("gcTs", gcTs),
		zap.Any("duration", time.Since(start)))

	// clean data in memory before clean data on disk
	p.cleanObsoleteDataInMemory(gcTs)
	log.Info("gc finish clean in memory data",
		zap.Uint64("gcTs", gcTs),
		zap.Any("duration", time.Since(start)))

	cleanObsoleteData(p.db, oldGcTs, gcTs)
	log.Info("gc finish",
		zap.Uint64("gcTs", gcTs),
		zap.Any("duration", time.Since(start)))

	return nil
}

func (p *persistentStorage) cleanObsoleteDataInMemory(gcTs uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.gcTs = gcTs

	// clean tablesDDLHistory
	tablesToRemove := make(map[int64]interface{})
	for tableID := range p.tablesDDLHistory {
		i := sort.Search(len(p.tablesDDLHistory[tableID]), func(i int) bool {
			return p.tablesDDLHistory[tableID][i] > gcTs
		})
		if i == len(p.tablesDDLHistory[tableID]) {
			tablesToRemove[tableID] = nil
			continue
		}
		p.tablesDDLHistory[tableID] = p.tablesDDLHistory[tableID][i:]
	}
	for tableID := range tablesToRemove {
		delete(p.tablesDDLHistory, tableID)
	}

	// clean tableTriggerDDLHistory
	i := sort.Search(len(p.tableTriggerDDLHistory), func(i int) bool {
		return p.tableTriggerDDLHistory[i] > gcTs
	})
	p.tableTriggerDDLHistory = p.tableTriggerDDLHistory[i:]

	// clean tableInfoStoreMap
	// Note: tableInfoStoreMap need to keep one version before gcTs,
	//  so it has different gc logic with tablesDDLHistory
	tablesToRemove = make(map[int64]interface{})
	for tableID, store := range p.tableInfoStoreMap {
		if needRemove := store.gc(gcTs); needRemove {
			tablesToRemove[tableID] = nil
		}
	}
	for tableID := range tablesToRemove {
		delete(p.tableInfoStoreMap, tableID)
	}
}

func (p *persistentStorage) updateUpperBound(upperBound UpperBoundMeta) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.upperBound = upperBound
	p.upperBoundChanged = true
}

func (p *persistentStorage) getUpperBound() UpperBoundMeta {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.upperBound
}

func (p *persistentStorage) persistUpperBoundPeriodically(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			p.mu.Lock()
			if !p.upperBoundChanged {
				log.Warn("schema store upper bound not changed")
				p.mu.Unlock()
				continue
			}
			upperBound := p.upperBound
			p.upperBoundChanged = false
			p.mu.Unlock()

			writeUpperBoundMeta(p.db, upperBound)
		}
	}
}

func (p *persistentStorage) handleDDLJob(job *model.Job) error {
	p.mu.Lock()

	if shouldSkipDDL(job, p.tableMap) {
		p.mu.Unlock()
		return nil
	}

	handler, ok := allDDLHandlers[job.Type]
	if !ok {
		log.Panic("unknown ddl type", zap.Any("ddlType", job.Type), zap.String("query", job.Query))
	}
	ddlEvent := handler.buildPersistedDDLEventFunc(buildPersistedDDLEventFuncArgs{
		job:          job,
		databaseMap:  p.databaseMap,
		tableMap:     p.tableMap,
		partitionMap: p.partitionMap,
	})

	p.mu.Unlock()

	if ddlEvent.Type == byte(model.ActionExchangeTablePartition) {
		ddlEvent.PreTableInfo, _ = p.forceGetTableInfo(ddlEvent.PrevTableID, ddlEvent.FinishedTs)
	}

	// Note: need write ddl event to disk before update ddl history,
	// because other goroutines may read ddl events from disk according to ddl history
	writePersistedDDLEvent(p.db, &ddlEvent)

	p.mu.Lock()
	defer p.mu.Unlock()
	// Note: `updateDDLHistory` must be before `updateDatabaseInfoAndTableInfo`,
	// because `updateDDLHistory` will refer to the info in databaseMap and tableMap,
	// and `updateDatabaseInfoAndTableInfo` may delete some info from databaseMap and tableMap
	p.tableTriggerDDLHistory = handler.updateDDLHistoryFunc(updateDDLHistoryFuncArgs{
		ddlEvent:               &ddlEvent,
		databaseMap:            p.databaseMap,
		tableMap:               p.tableMap,
		partitionMap:           p.partitionMap,
		tablesDDLHistory:       p.tablesDDLHistory,
		tableTriggerDDLHistory: p.tableTriggerDDLHistory,
	})

	handler.updateSchemaMetadataFunc(updateSchemaMetadataFuncArgs{
		event:        &ddlEvent,
		databaseMap:  p.databaseMap,
		tableMap:     p.tableMap,
		partitionMap: p.partitionMap,
	})

	handler.iterateEventTablesFunc(&ddlEvent, func(tableIDs ...int64) {
		for _, tableID := range tableIDs {
			if store, ok := p.tableInfoStoreMap[tableID]; ok {
				// do some safety check
				switch model.ActionType(job.Type) {
				case model.ActionCreateTable, model.ActionCreateTables:
					// newly created tables should not be registered before this ddl are handled
					log.Panic("should not be registered", zap.Int64("tableID", tableID))
				default:
				}
				store.applyDDL(&ddlEvent)
			}
		}
	})

	return nil
}

func shouldSkipDDL(job *model.Job, tableMap map[int64]*BasicTableInfo) bool {
	switch model.ActionType(job.Type) {
	// Skipping ActionCreateTable and ActionCreateTables when the table already exists:
	// 1. It is possible to receive ActionCreateTable and ActionCreateTables multiple times,
	//    and filtering duplicates in a generic way is challenging.
	//    (SchemaVersion checks are unreliable because versions might not be strictly ordered in some cases.)
	// 2. ActionCreateTable and ActionCreateTables for the same table may have different commit ts.
	//    One of these actions could be garbage collected, leaving the table present in the snapshot.
	//    Therefore, the only reliable way to determine if a later DDL operation is redundant
	//    is by verifying whether the table already exists.
	case model.ActionCreateTable:
		// Note: partition table's logical table id is also in tableMap
		if _, ok := tableMap[job.BinlogInfo.TableInfo.ID]; ok {
			log.Warn("table already exists. ignore DDL",
				zap.String("DDL", job.Query),
				zap.Int64("jobID", job.ID),
				zap.Int64("schemaID", job.SchemaID),
				zap.Int64("tableID", job.BinlogInfo.TableInfo.ID),
				zap.Uint64("finishedTs", job.BinlogInfo.FinishedTS),
				zap.Int64("jobSchemaVersion", job.BinlogInfo.SchemaVersion))
			return true
		}
	case model.ActionCreateTables:
		// For duplicate create tables ddl job, the tables in the job should be same, check the first table is enough
		if _, ok := tableMap[job.BinlogInfo.MultipleTableInfos[0].ID]; ok {
			log.Warn("table already exists. ignore DDL",
				zap.String("DDL", job.Query),
				zap.Int64("jobID", job.ID),
				zap.Int64("schemaID", job.SchemaID),
				zap.Int64("tableID", job.BinlogInfo.MultipleTableInfos[0].ID),
				zap.Uint64("finishedTs", job.BinlogInfo.FinishedTS),
				zap.Int64("jobSchemaVersion", job.BinlogInfo.SchemaVersion))
			return true
		}
	// DDLs ignored
	case model.ActionCreateSequence,
		model.ActionAlterSequence,
		model.ActionDropSequence,
		model.ActionAlterTableAttributes,
		model.ActionAlterTablePartitionAttributes,
		model.ActionCreateResourceGroup,
		model.ActionAlterResourceGroup,
		model.ActionDropResourceGroup:
		log.Info("ignore ddl",
			zap.String("DDL", job.Query),
			zap.Int64("jobID", job.ID),
			zap.Uint64("finishedTs", job.BinlogInfo.FinishedTS),
			zap.Any("type", job.Type))
		return true
	}
	return false
}

func buildDDLEvent(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) commonEvent.DDLEvent {
	handler, ok := allDDLHandlers[model.ActionType(rawEvent.Type)]
	if !ok {
		log.Panic("unknown ddl type", zap.Any("ddlType", rawEvent.Type), zap.String("query", rawEvent.Query))
	}
	return handler.buildDDLEventFunc(rawEvent, tableFilter)
}
