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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/flowbehappy/tigate/logservice/logpuller"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/filter"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
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
	gcRunning atomic.Bool

	pdCli pd.Client

	kvStorage kv.Storage

	db *pebble.DB

	mu sync.RWMutex

	// the current gcTs on disk
	gcTs uint64

	tablesBasicInfo map[common.TableID]*VersionedTableBasicInfo

	// schemaID -> database info
	// it contains all databases and deleted databases
	// will only be removed when its delete version is smaller than gc ts
	databaseMap map[common.SchemaID]*DatabaseInfo

	// table id -> a sorted list of finished ts for the table's ddl events
	tablesDDLHistory map[common.TableID][]uint64

	// it has two use cases:
	// 1. store the ddl events need to send to a table dispatcher
	//    Note: some ddl events in the history may never be send,
	//          for example the create table ddl, truncate table ddl(usually the first event)
	// 2. build table info store for a table
	tableTriggerDDLHistory []uint64

	// tableID -> versioned store
	// it just contains tables which is used by dispatchers
	tableInfoStoreMap map[common.TableID]*versionedTableInfoStore

	// tableID -> total registered count
	tableRegisteredCount map[common.TableID]int
}

type upperBoundMeta struct {
	FinishedDDLTs common.Ts `json:"finished_ddl_ts"`
	SchemaVersion int64     `json:"schema_version"`
	ResolvedTs    common.Ts `json:"resolved_ts"`
}

func newPersistentStorage(
	ctx context.Context,
	root string,
	pdCli pd.Client,
	storage kv.Storage,
) (*persistentStorage, upperBoundMeta) {
	gcSafePoint, err := pdCli.UpdateServiceGCSafePoint(ctx, "cdc-new-store", 0, 0)
	if err != nil {
		log.Panic("get ts failed", zap.Error(err))
	}

	dbPath := fmt.Sprintf("%s/%s", root, dataDir)
	// FIXME: avoid remove
	if err := os.RemoveAll(dbPath); err != nil {
		log.Panic("fail to remove path")
	}

	// TODO: update pebble options
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		log.Fatal("open db failed", zap.Error(err))
	}

	// check whether the data on disk is reusable
	isDataReusable := true
	gcTs, err := readGcTs(db)
	// TODO: distiguish non-exist key with other io errors
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

	// initialize persistent storage
	dataStorage := &persistentStorage{
		pdCli:                  pdCli,
		kvStorage:              storage,
		db:                     db,
		gcTs:                   gcTs,
		databaseMap:            make(map[common.SchemaID]*DatabaseInfo),
		tablesBasicInfo:        make(map[common.TableID]*VersionedTableBasicInfo),
		tablesDDLHistory:       make(map[common.TableID][]uint64),
		tableTriggerDDLHistory: make([]uint64, 0),
		tableInfoStoreMap:      make(map[common.TableID]*versionedTableInfoStore),
		tableRegisteredCount:   make(map[common.TableID]int),
	}
	if isDataReusable {
		dataStorage.initializeFromDisk(upperBound)
	} else {
		upperBound = dataStorage.initializeFromKVStorage(dbPath, storage, gcSafePoint)
	}

	go func() {
		dataStorage.gc(ctx)
	}()

	go func() {
		dataStorage.updateUpperBound(ctx)
	}()

	return dataStorage, upperBound
}

func (p *persistentStorage) initializeFromKVStorage(dbPath string, storage kv.Storage, gcTs common.Ts) upperBoundMeta {
	// TODO: avoid recreate db if the path is empty at start
	if err := os.RemoveAll(dbPath); err != nil {
		log.Panic("fail to remove path")
	}

	var err error
	// TODO: update pebble options
	if p.db, err = pebble.Open(dbPath, &pebble.Options{}); err != nil {
		log.Fatal("open db failed", zap.Error(err))
	}
	log.Info("schema store create a fresh storage")

	var upperBound upperBoundMeta
	if p.databaseMap, p.tablesBasicInfo, upperBound, err = writeSchemaSnapshotAndMeta(p.db, storage, gcTs); err != nil {
		// TODO: retry
		log.Fatal("fail to initialize from kv snapshot")
	}
	p.gcTs = gcTs
	return upperBound
}

func (p *persistentStorage) initializeFromDisk(upperBound upperBoundMeta) {
	// TODO: cleanObseleteData?

	storageSnap := p.db.NewSnapshot()
	defer storageSnap.Close()

	var err error
	if p.tablesBasicInfo, err = loadTablesInKVSnap(storageSnap, p.gcTs); err != nil {
		log.Fatal("load tables in kv snapshot failed")
	}

	if p.databaseMap, p.tablesDDLHistory, p.tableTriggerDDLHistory, err = loadDatabaseInfoAndDDLHistory(
		storageSnap,
		p.gcTs,
		upperBound,
		p.tablesBasicInfo); err != nil {
		log.Fatal("fail to initialize from disk")
	}
}

// FIXME: load the info from disk
func (p *persistentStorage) getAllPhysicalTables(snapTs common.Ts, tableFilter filter.Filter) ([]common.Table, error) {
	meta := logpuller.GetSnapshotMeta(p.kvStorage, uint64(snapTs))
	dbinfos, err := meta.ListDatabases()
	if err != nil {
		log.Fatal("list databases failed", zap.Error(err))
	}

	tables := make([]common.Table, 0)

	for _, dbinfo := range dbinfos {
		if filter.IsSysSchema(dbinfo.Name.O) ||
			(tableFilter != nil && tableFilter.ShouldIgnoreSchema(dbinfo.Name.O)) {
			continue
		}
		rawTables, err := meta.GetMetasByDBID(dbinfo.ID)
		log.Info("get database", zap.Any("dbinfo", dbinfo), zap.Int("rawTablesLen", len(rawTables)))
		if err != nil {
			log.Fatal("get tables failed", zap.Error(err))
		}
		for _, rawTable := range rawTables {
			if !isTableRawKey(rawTable.Field) {
				continue
			}
			tbName := &model.TableNameInfo{}
			err := json.Unmarshal(rawTable.Value, tbName)
			if err != nil {
				log.Fatal("get table info failed", zap.Error(err))
			}
			// TODO: support ignore sequence / forcereplicate / view cases
			if tableFilter != nil && tableFilter.ShouldIgnoreTable(dbinfo.Name.O, tbName.Name.O) {
				continue
			}
			tables = append(tables, common.Table{
				SchemaID: dbinfo.ID,
				TableID:  tbName.ID,
			})
		}
	}

	return tables, nil
}

// only return when table info is initialized
func (p *persistentStorage) registerTable(tableID common.TableID) error {
	p.mu.Lock()
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
	return nil
}

func (p *persistentStorage) unregisterTable(tableID common.TableID) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tableRegisteredCount[tableID] -= 1
	if p.tableRegisteredCount[tableID] <= 0 {
		if _, ok := p.tableInfoStoreMap[tableID]; !ok {
			return fmt.Errorf(fmt.Sprintf("table %d not found", tableID))
		}
		delete(p.tableInfoStoreMap, tableID)
	}
	return nil
}

func (p *persistentStorage) getTableInfo(tableID common.TableID, ts common.Ts) (*common.TableInfo, error) {
	p.mu.Lock()
	store, ok := p.tableInfoStoreMap[tableID]
	if !ok {
		return nil, fmt.Errorf(fmt.Sprintf("table %d not found", tableID))
	}
	p.mu.Unlock()
	return store.getTableInfo(ts)
}

// TODO: not all ddl in p.tablesDDLHistory should be sent to the dispatcher, verify dispatcher will set the right range
func (p *persistentStorage) fetchTableDDLEvents(tableID common.TableID, start, end common.Ts) []common.DDLEvent {
	p.mu.Lock()
	history, ok := p.tablesDDLHistory[tableID]
	if !ok {
		return nil
	}
	index := sort.Search(len(history), func(i int) bool {
		return history[i] > start
	})
	// no events to read
	if index == len(history) {
		return nil
	}
	// copy all target ts to a new slice
	allTargetTs := make([]common.Ts, 0)
	for i := index; i < len(history); i++ {
		if history[i] <= end {
			allTargetTs = append(allTargetTs, history[i])
		}
	}

	storageSnap := p.db.NewSnapshot()
	defer storageSnap.Close()
	p.mu.Unlock()

	rawEvents := make([]PersistedDDLEvent, len(allTargetTs))
	for i, ts := range allTargetTs {
		rawEvents[i] = readDDLEvent(storageSnap, ts)
	}

	p.mu.Lock()
	events := make([]common.DDLEvent, len(allTargetTs))
	for i, rawEvent := range rawEvents {
		events[i] = buildDDLEvent(rawEvent, p.tablesBasicInfo, p.databaseMap)
	}
	p.mu.Unlock()

	return events
}

func (p *persistentStorage) fetchTableTriggerDDLEvents(tableFilter filter.Filter, start common.Ts, limit int) []common.DDLEvent {
	events := make([]common.DDLEvent, 0)
	nextStartTs := start
	storageSnap := p.db.NewSnapshot()
	defer storageSnap.Close()
	for {
		allTargetTs := make([]common.Ts, 0, limit)
		p.mu.Lock()
		index := sort.Search(len(p.tableTriggerDDLHistory), func(i int) bool {
			return p.tableTriggerDDLHistory[i] > nextStartTs
		})
		// no more events to read
		if index == len(p.tableTriggerDDLHistory) {
			return events
		}
		for i := index; i < len(p.tableTriggerDDLHistory); i++ {
			allTargetTs = append(allTargetTs, p.tableTriggerDDLHistory[i])
			if len(allTargetTs) >= limit-len(events) {
				break
			}
		}
		p.mu.Unlock()

		if len(allTargetTs) == 0 {
			return events
		}

		for _, ts := range allTargetTs {
			rawEvent := readDDLEvent(storageSnap, ts)
			if tableFilter.ShouldDiscardDDL(model.ActionType(rawEvent.Type), rawEvent.SchemaName, rawEvent.TableName) {
				continue
			}
			events = append(events, common.DDLEvent{
				Job: rawEvent.Job,
				// FIXME: rename to finished ts
				CommitTS: rawEvent.CommitTS,
				// FIXME
				BlockedTables:     nil,
				NeedDroppedTables: nil,
				NeedAddedTables:   nil,
			})
		}
		nextStartTs = allTargetTs[len(allTargetTs)-1]
	}
}

func (p *persistentStorage) buildVersionedTableInfoStore(
	store *versionedTableInfoStore,
) error {
	tableID := store.getTableID()
	// get snapshot from disk before get current gc ts to make sure data is not deleted by gc process
	storageSnap := p.db.NewSnapshot()
	defer storageSnap.Close()

	p.mu.RLock()
	kvSnapVersion := p.gcTs
	tableBasicInfo, ok := p.tablesBasicInfo[tableID]
	if !ok {
		log.Panic("table not found", zap.Int64("tableID", int64(tableID)))
	}
	inKVSnap := tableBasicInfo.CreateVersion == kvSnapVersion
	var allDDLFinishedTs []uint64
	allDDLFinishedTs = append(allDDLFinishedTs, p.tablesDDLHistory[tableID]...)
	p.mu.RUnlock()

	getSchemaName := func(schemaID common.SchemaID) (string, error) {
		p.mu.RLock()
		defer func() {
			p.mu.RUnlock()
		}()

		databaseInfo, ok := p.databaseMap[schemaID]
		if !ok {
			return "", errors.New("database not found")
		}
		return databaseInfo.Name, nil
	}

	if inKVSnap {
		if err := addTableInfoFromKVSnap(store, kvSnapVersion, storageSnap, getSchemaName); err != nil {
			return err
		}
	}

	for _, version := range allDDLFinishedTs {
		ddlEvent := readDDLEvent(storageSnap, version)
		// TODO: check ddlEvent type
		// TODO: no need fill it here, schemaName should be in event
		schemaName, err := getSchemaName(common.SchemaID(ddlEvent.SchemaID))
		if err != nil {
			log.Fatal("get schema name failed", zap.Error(err))
		}
		ddlEvent.SchemaName = schemaName

		store.applyDDLFromPersistStorage(ddlEvent)
	}
	store.setTableInfoInitialized()
	return nil
}

func addTableInfoFromKVSnap(
	store *versionedTableInfoStore,
	kvSnapVersion common.Ts,
	snap *pebble.Snapshot,
	getSchemaName func(schemaID common.SchemaID) (string, error),
) error {
	schemaID, rawTableInfo := readSchemaIDAndTableInfoFromKVSnap(snap, store.getTableID(), kvSnapVersion)
	schemaName, err := getSchemaName(schemaID)
	if err != nil {
		return err
	}
	tableInfo := common.WrapTableInfo(int64(schemaID), schemaName, uint64(kvSnapVersion), rawTableInfo)
	store.addInitialTableInfo(tableInfo)
	return nil
}

func (p *persistentStorage) gc(ctx context.Context) error {
	// if p.gcRunning.CompareAndSwap(false, true) {
	// 	return nil
	// }
	// defer p.gcRunning.Store(false)
	// p.gcTS.Store(uint64(gcTS))
	// // TODO: write snapshot(schema and table) to disk(don't need to be in the same batch) and maintain the key that need be deleted(or just write it to a delete batch)

	// // update gcTS in disk, must do it before delete any data
	// batch := p.db.NewBatch()
	// if err := writeTSToBatch(batch, gcTSKey(), gcTS); err != nil {
	// 	return err
	// }
	// if err := batch.Commit(pebble.NoSync); err != nil {
	// 	return err
	// }
	// // TODO: delete old data(including index data, so we need to read data one by one)
	// // may be write and delete in the same batch?

	for {
		select {
		case <-ctx.Done():
			return nil
			// TODO: get gc ts periodically
		}
	}
}

func (p *persistentStorage) doGc() error {
	return nil
}

func (p *persistentStorage) updateUpperBound(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
			// TODO: update upper bound periodically
		}
	}
}

func (p *persistentStorage) handleSortedDDLEvents(ddlEvents ...PersistedDDLEvent) error {
	// TODO: ignore some ddl event

	// TODO: check ddl events are sorted

	// TODO: build PersistedDDLEvent here

	p.mu.Lock()
	for _, event := range ddlEvents {
		// TODO: may be don't need to fillSchemaName?
		fillSchemaName(event, p.databaseMap)
		skip, err := updateDatabaseInfoAndTableInfo(&event, p.databaseMap, p.tablesBasicInfo)
		if err != nil {
			return err
		}
		// even if the ddl is skipped here, it can still be written to disk.
		// because when apply this ddl at restart, it will be skipped again.
		if skip {
			continue
		}
		if p.tableTriggerDDLHistory, err = updateDDLHistory(
			&event,
			p.databaseMap,
			p.tablesBasicInfo,
			p.tablesDDLHistory,
			p.tableTriggerDDLHistory); err != nil {
			return err
		}
		if err := updateRegisteredTableInfoStore(event, p.tableInfoStoreMap); err != nil {
			return err
		}
	}
	p.mu.Unlock()

	writeDDLEvents(p.db, ddlEvents...)
	return nil
}

func updateDatabaseInfoAndTableInfo(
	event *PersistedDDLEvent,
	databaseMap map[common.SchemaID]*DatabaseInfo,
	tablesBasicInfo map[common.TableID]*VersionedTableBasicInfo,
) (bool, error) {
	addTableToDB := func(schemaID common.SchemaID, tableID common.TableID) {
		databaseInfo, ok := databaseMap[schemaID]
		if !ok {
			log.Panic("database not found.",
				zap.String("DDL", event.Query),
				zap.Int64("jobID", event.ID),
				zap.Int64("schemaID", int64(schemaID)),
				zap.Int64("tableID", int64(tableID)),
				zap.Uint64("finishTs", event.FinishedTs),
				zap.Int64("jobSchemaVersion", event.SchemaVersion))
		}
		databaseInfo.Tables[tableID] = true
	}

	removeTableFromDB := func(schemaID common.SchemaID, tableID common.TableID) {
		databaseInfo, ok := databaseMap[schemaID]
		if !ok {
			log.Panic("database not found. ",
				zap.String("DDL", event.Query),
				zap.Int64("jobID", event.ID),
				zap.Int64("schemaID", int64(schemaID)),
				zap.Int64("tableID", int64(tableID)),
				zap.Uint64("finishTs", event.FinishedTs),
				zap.Int64("jobSchemaVersion", event.SchemaVersion))
		}
		delete(databaseInfo.Tables, tableID)
	}

	createTable := func(schemaID common.SchemaID, tableID common.TableID) bool {
		if _, ok := tablesBasicInfo[tableID]; ok {
			return false
		}
		addTableToDB(schemaID, tableID)
		tablesBasicInfo[tableID] = &VersionedTableBasicInfo{
			SchemaIDs:     []SchemaIDWithVersion{{SchemaID: schemaID, CreateVersion: common.Ts(event.FinishedTs)}},
			Names:         []TableNameWithVersion{{Name: event.TableInfo.Name.O, CreateVersion: common.Ts(event.FinishedTs)}},
			CreateVersion: common.Ts(event.FinishedTs),
		}
		return true
	}

	dropTable := func(schemaID common.SchemaID, tableID common.TableID) {
		removeTableFromDB(schemaID, tableID)
		delete(tablesBasicInfo, tableID)
	}

	switch model.ActionType(event.Type) {
	case model.ActionCreateSchema:
		if _, ok := databaseMap[common.SchemaID(event.SchemaID)]; ok {
			log.Warn("database already exists. ignore DDL ",
				zap.String("DDL", event.Query),
				zap.Int64("jobID", event.ID),
				zap.Int64("schemaID", event.SchemaID),
				zap.Uint64("finishTs", event.FinishedTs),
				zap.Int64("jobSchemaVersion", event.SchemaVersion))
			return true, nil
		}
		databaseMap[common.SchemaID(event.SchemaID)] = &DatabaseInfo{
			Name:          event.SchemaName,
			Tables:        make(map[common.TableID]bool),
			CreateVersion: common.Ts(event.FinishedTs),
			DeleteVersion: math.MaxUint64,
		}
	case model.ActionDropSchema:
		databaseInfo, ok := databaseMap[common.SchemaID(event.SchemaID)]
		if !ok {
			log.Warn("database not found. ignore DDL ",
				zap.String("DDL", event.Query),
				zap.Int64("jobID", event.ID),
				zap.Int64("schemaID", event.SchemaID),
				zap.Uint64("finishTs", event.FinishedTs),
				zap.Int64("jobSchemaVersion", event.SchemaVersion))
			return true, nil
		}
		if databaseInfo.DeleteVersion != math.MaxUint64 {
			log.Panic("should not happen")
		}
		databaseInfo.DeleteVersion = common.Ts(event.FinishedTs)
	case model.ActionCreateTable:
		ok := createTable(common.SchemaID(event.SchemaID), common.TableID(event.TableID))
		if !ok {
			log.Warn("table already exists. ignore DDL ",
				zap.String("DDL", event.Query),
				zap.Int64("jobID", event.ID),
				zap.Int64("schemaID", event.SchemaID),
				zap.Int64("tableID", event.TableID),
				zap.Uint64("finishTs", event.FinishedTs),
				zap.Int64("jobSchemaVersion", event.SchemaVersion))
			return true, nil
		}
	case model.ActionDropTable:
		dropTable(common.SchemaID(event.SchemaID), common.TableID(event.TableID))
	case model.ActionAddColumn,
		model.ActionDropColumn,
		model.ActionAddIndex,
		model.ActionDropIndex,
		model.ActionAddForeignKey,
		model.ActionDropForeignKey,
		model.ActionModifyColumn,
		model.ActionRebaseAutoID:
		// ignore
	case model.ActionTruncateTable:
		dropTable(common.SchemaID(event.SchemaID), common.TableID(event.TableID))
		// TODO: do we need to the return value of createTable?
		createTable(common.SchemaID(event.SchemaID), event.TableInfo.ID)
	case model.ActionRenameTable:
		oldSchemaID := getSchemaID(tablesBasicInfo, common.TableID(event.TableID), common.Ts(event.FinishedTs-1))
		if oldSchemaID != common.SchemaID(event.SchemaID) {
			modifySchemaID(tablesBasicInfo, common.TableID(event.TableID), common.SchemaID(event.SchemaID), common.Ts(event.FinishedTs))
			removeTableFromDB(oldSchemaID, common.TableID(event.TableID))
			addTableToDB(common.SchemaID(event.SchemaID), common.TableID(event.TableID))
		}
		oldTableName := getTableName(tablesBasicInfo, common.TableID(event.TableID), common.Ts(event.FinishedTs-1))
		if oldTableName != event.TableInfo.Name.O {
			modifyTableName(tablesBasicInfo, common.TableID(event.TableID), event.TableInfo.Name.O, common.Ts(event.FinishedTs))
		}
	case model.ActionSetDefaultValue,
		model.ActionShardRowID,
		model.ActionModifyTableComment,
		model.ActionRenameIndex,
		model.ActionCreateView:
		// TODO
		// seems can be ignored
	case model.ActionAddTablePartition:
		// TODO
	default:
		log.Panic("unknown ddl type",
			zap.Any("ddlType", event.Type),
			zap.String("DDL", event.Query))
	}

	return false, nil
}

func updateDDLHistory(
	ddlEvent *PersistedDDLEvent,
	databaseMap map[common.SchemaID]*DatabaseInfo,
	tablesBasicInfo map[common.TableID]*VersionedTableBasicInfo,
	tablesDDLHistory map[common.TableID][]uint64,
	tableTriggerDDLHistory []uint64,
) ([]uint64, error) {
	addTableHistory := func(tableID common.TableID) {
		tablesDDLHistory[tableID] = append(tablesDDLHistory[tableID], ddlEvent.FinishedTs)
	}

	switch model.ActionType(ddlEvent.Type) {
	case model.ActionCreateSchema,
		model.ActionCreateView:
		tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
		for tableID := range tablesBasicInfo {
			addTableHistory(tableID)
		}
	case model.ActionDropSchema:
		tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
		databaseInfo, ok := databaseMap[common.SchemaID(ddlEvent.SchemaID)]
		if !ok {
			log.Panic("cannot find database", zap.Int64("schemaID", ddlEvent.SchemaID))
		}
		for tableID := range databaseInfo.Tables {
			addTableHistory(tableID)
		}
	case model.ActionCreateTable,
		model.ActionDropTable:
		tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
		addTableHistory(common.TableID(ddlEvent.TableID))
	case model.ActionAddColumn,
		model.ActionDropColumn,
		model.ActionAddIndex,
		model.ActionDropIndex,
		model.ActionAddForeignKey,
		model.ActionDropForeignKey,
		model.ActionModifyColumn,
		model.ActionRebaseAutoID,
		model.ActionSetDefaultValue,
		model.ActionShardRowID,
		model.ActionModifyTableComment,
		model.ActionRenameIndex:
		addTableHistory(common.TableID(ddlEvent.TableID))
	case model.ActionTruncateTable:
		addTableHistory(common.TableID(ddlEvent.TableID))
		addTableHistory(common.TableID(ddlEvent.TableInfo.ID))
	case model.ActionRenameTable:
		tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
		addTableHistory(common.TableID(ddlEvent.TableID))
	default:
		log.Panic("unknown ddl type",
			zap.Any("ddlType", ddlEvent.Type),
			zap.String("DDL", ddlEvent.Query))
	}

	return tableTriggerDDLHistory, nil
}

func updateRegisteredTableInfoStore(
	event PersistedDDLEvent,
	tableInfoStoreMap map[common.TableID]*versionedTableInfoStore,
) error {
	switch model.ActionType(event.Type) {
	case model.ActionCreateSchema,
		model.ActionDropSchema,
		model.ActionCreateTable,
		model.ActionAddIndex,
		model.ActionDropIndex,
		model.ActionAddForeignKey,
		model.ActionDropForeignKey,
		model.ActionRenameTable,
		model.ActionCreateView:
		// ignore
	case model.ActionDropTable,
		model.ActionAddColumn,
		model.ActionDropColumn,
		model.ActionTruncateTable,
		model.ActionModifyColumn,
		model.ActionRebaseAutoID,
		model.ActionSetDefaultValue,
		model.ActionShardRowID,
		model.ActionModifyTableComment,
		model.ActionRenameIndex:
		store, ok := tableInfoStoreMap[common.TableID(event.TableID)]
		if ok {
			store.applyDDL(event)
		}
	default:
		log.Panic("unknown ddl type",
			zap.Any("ddlType", event.Type),
			zap.String("DDL", event.Query))
	}
	return nil
}

func buildDDLEvent(
	rawEvent PersistedDDLEvent,
	tablesBasicInfo map[common.TableID]*VersionedTableBasicInfo,
	databaseMap map[common.SchemaID]*DatabaseInfo,
) common.DDLEvent {
	var event common.DDLEvent
	// event.Job = rawEvent.Job
	event.CommitTS = rawEvent.FinishedTs
	switch event.Job.Type {
	case model.ActionCreateSchema,
		model.ActionAddColumn,
		model.ActionDropColumn,
		model.ActionAddIndex,
		model.ActionDropIndex,
		model.ActionAddForeignKey,
		model.ActionDropForeignKey,
		model.ActionModifyColumn,
		model.ActionRebaseAutoID,
		model.ActionSetDefaultValue,
		model.ActionShardRowID,
		model.ActionModifyTableComment,
		model.ActionRenameIndex:
		// ignore
	case model.ActionDropSchema:
		event.NeedDroppedTables = &common.InfluencedTables{
			InfluenceType: common.DB,
			SchemaID:      event.Job.SchemaID,
		}
	case model.ActionCreateTable:
		event.NeedAddedTables = []common.Table{
			{
				SchemaID: event.Job.SchemaID,
				TableID:  event.Job.TableID,
			},
		}
	case model.ActionDropTable:
		event.NeedDroppedTables = &common.InfluencedTables{
			InfluenceType: common.Normal,
			TableIDs:      []int64{event.Job.TableID},
		}
	case model.ActionTruncateTable:
		event.NeedDroppedTables = &common.InfluencedTables{
			InfluenceType: common.Normal,
			TableIDs:      []int64{event.Job.TableID},
		}
		event.NeedAddedTables = []common.Table{
			{
				SchemaID: event.Job.SchemaID,
				// TODO: may be we cannot read it?
				TableID: event.Job.BinlogInfo.TableInfo.ID,
			},
		}
	case model.ActionRenameTable:
		event.BlockedTables = &common.InfluencedTables{
			InfluenceType: common.Normal,
			TableIDs:      []int64{event.Job.TableID},
		}
	case model.ActionCreateView:
		event.BlockedTables = &common.InfluencedTables{
			InfluenceType: common.All,
		}
	default:
		log.Panic("unknown ddl type",
			zap.Any("ddlType", event.Job.Type),
			zap.String("DDL", event.Job.Query))
	}
	return event
}

func fillSchemaName(event PersistedDDLEvent, databaseMap map[common.SchemaID]*DatabaseInfo) error {
	// FIXME: only fill schema name for needed ddl

	if model.ActionType(event.Type) == model.ActionCreateSchema || model.ActionType(event.Type) == model.ActionDropSchema {
		return nil
	}

	schemaID := common.SchemaID(event.SchemaID)
	databaseInfo, ok := databaseMap[schemaID]
	if !ok {
		log.Error("database not found", zap.Any("schemaID", schemaID))
		return errors.New("database not found")
	}
	if databaseInfo.CreateVersion > common.Ts(event.FinishedTs) {
		return errors.New("database is not created")
	}
	if databaseInfo.DeleteVersion < common.Ts(event.FinishedTs) {
		return errors.New("database is deleted")
	}
	event.SchemaName = databaseInfo.Name
	return nil
}

func modifySchemaID(
	tablesBasicInfo map[common.TableID]*VersionedTableBasicInfo,
	tableID common.TableID,
	schemaID common.SchemaID,
	version common.Ts,
) {
	info, ok := tablesBasicInfo[tableID]
	if !ok {
		log.Panic("table not found", zap.Int64("tableID", int64(tableID)))
	}

	info.SchemaIDs = append(info.SchemaIDs, SchemaIDWithVersion{
		SchemaID:      schemaID,
		CreateVersion: version,
	})
}

// return the schema id with largest version which is less than or equal to the given version
func getSchemaID(
	tablesBasicInfo map[common.TableID]*VersionedTableBasicInfo,
	tableID common.TableID,
	version common.Ts,
) common.SchemaID {
	info, ok := tablesBasicInfo[tableID]
	if !ok {
		log.Panic("table not found", zap.Int64("tableID", int64(tableID)))
	}

	index := sort.Search(len(info.SchemaIDs), func(i int) bool {
		return info.SchemaIDs[i].CreateVersion > version
	})
	if index == 0 {
		log.Panic("should not happen")
	}
	return info.SchemaIDs[index-1].SchemaID
}

func modifyTableName(
	tablesBasicInfo map[common.TableID]*VersionedTableBasicInfo,
	tableID common.TableID,
	tableName string,
	version common.Ts,
) {
	info, ok := tablesBasicInfo[tableID]
	if !ok {
		log.Panic("table not found", zap.Int64("tableID", int64(tableID)))
	}
	info.Names = append(info.Names, TableNameWithVersion{
		Name:          tableName,
		CreateVersion: version,
	})
}

// return the table name with largest version which is less than or equal to the given version
func getTableName(
	tablesBasicInfo map[common.TableID]*VersionedTableBasicInfo,
	tableID common.TableID,
	version common.Ts,
) string {
	info, ok := tablesBasicInfo[tableID]
	if !ok {
		log.Panic("table not found", zap.Int64("tableID", int64(tableID)))
	}
	index := sort.Search(len(info.Names), func(i int) bool {
		return info.Names[i].CreateVersion > version
	})
	if index == 0 {
		log.Panic("should not happen")
	}
	return info.Names[index-1].Name
}