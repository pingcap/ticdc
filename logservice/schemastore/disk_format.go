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
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"go.uber.org/zap"
)

// Data format:
//  1. snapshot data
//     {prefix11}{snapshot_ts}{table_id} -> table info and schema id
//     {prefix12}{snapshot_ts}{schema_id} -> database info
//  2. ddl jobs
//     {prefix21}{finished_ddl_ts} -> ddl job
//     NOTE: {finished_ddl_ts} must be unique
//  3. metadata
//     {key31} -> {snapshot_ts}
//     {key32} -> {max_finished_ddl_ts}{schema_version}{resolved_ts}
//     the valid data keys on disk includes snapshot data at `snapshot_ts`
//     and ddl jobs in the range (snapshot_ts, max_finished_ddl_ts]
//     and we will pull ddl job from `resolved_ts` at restart if the current gc ts is smaller than resolved_ts.

const (
	snapshotSchemaKeyPrefix    = "ss_"
	snapshotTableKeyPrefix     = "st_"
	snapshotPartitionKeyPrefix = "sp_"
	ddlKeyPrefix               = "ds_"
)

func gcTsKey() []byte {
	return []byte("gc")
}

func upperBoundKey() []byte {
	return []byte("up")
}

func schemaInfoKey(ts uint64, schemaID int64) ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteString(snapshotSchemaKeyPrefix)
	if err := binary.Write(buf, binary.BigEndian, ts); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, schemaID); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func tableInfoKey(ts uint64, tableID int64) ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteString(snapshotTableKeyPrefix)
	if err := binary.Write(buf, binary.BigEndian, ts); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, tableID); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func partitionInfoKey(ts uint64, partitionID int64) ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteString(snapshotPartitionKeyPrefix)
	if err := binary.Write(buf, binary.BigEndian, ts); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, partitionID); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func ddlJobKey(ts uint64) ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteString(ddlKeyPrefix)
	if err := binary.Write(buf, binary.BigEndian, ts); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func readGcTs(db *pebble.DB) (uint64, error) {
	snap := db.NewSnapshot()
	defer snap.Close()
	value, closer, err := snap.Get(gcTsKey())
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	var gcTs uint64
	buf := bytes.NewBuffer(value)
	if err := binary.Read(buf, binary.BigEndian, &gcTs); err != nil {
		log.Fatal("read gc ts failed", zap.Error(err))
	}
	return gcTs, nil
}

func writeGcTs(db *pebble.DB, gcTs uint64) {
	batch := db.NewBatch()
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, gcTs)
	if err != nil {
		log.Fatal("marshal gc ts failed")
	}
	batch.Set(gcTsKey(), buf.Bytes(), pebble.NoSync)
	if err := batch.Commit(pebble.NoSync); err != nil {
		log.Fatal("write gc ts failed")
	}
}

func readUpperBoundMeta(db *pebble.DB) (UpperBoundMeta, error) {
	snap := db.NewSnapshot()
	defer snap.Close()
	value, closer, err := snap.Get(upperBoundKey())
	if err != nil {
		return UpperBoundMeta{}, err
	}
	defer closer.Close()
	var meta UpperBoundMeta
	_, err = meta.UnmarshalMsg(value)
	if err != nil {
		log.Fatal("unmarshal upper bound meta failed", zap.Error(err))
	}
	return meta, nil
}

func writeUpperBoundMeta(db *pebble.DB, upperBound UpperBoundMeta) {
	batch := db.NewBatch()
	value, err := upperBound.MarshalMsg(nil)
	if err != nil {
		log.Fatal("marshal upper bound failed")
	}
	batch.Set(upperBoundKey(), value, pebble.NoSync)
	if err := batch.Commit(pebble.NoSync); err != nil {
		log.Fatal("write upper bound failed")
	}
}

func loadDatabasesInKVSnap(snap *pebble.Snapshot, gcTs uint64) (map[int64]*BasicDatabaseInfo, error) {
	databaseMap := make(map[int64]*BasicDatabaseInfo)

	startKey, err := schemaInfoKey(gcTs, 0)
	if err != nil {
		log.Fatal("generate lower bound failed", zap.Error(err))
	}
	endKey, err := schemaInfoKey(gcTs, math.MaxInt64)
	if err != nil {
		log.Fatal("generate upper bound failed", zap.Error(err))
	}
	snapIter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	})
	if err != nil {
		log.Fatal("new iterator failed", zap.Error(err))
	}
	defer snapIter.Close()
	for snapIter.First(); snapIter.Valid(); snapIter.Next() {
		var dbInfo model.DBInfo
		if err := json.Unmarshal(snapIter.Value(), &dbInfo); err != nil {
			log.Fatal("unmarshal db info failed", zap.Error(err))
		}

		databaseInfo := &BasicDatabaseInfo{
			Name:   dbInfo.Name.O,
			Tables: make(map[int64]bool),
		}
		databaseMap[dbInfo.ID] = databaseInfo
	}

	return databaseMap, nil
}

func loadTablesInKVSnap(
	snap *pebble.Snapshot,
	gcTs uint64,
	databaseMap map[int64]*BasicDatabaseInfo,
) (map[int64]*BasicTableInfo, map[int64]BasicPartitionInfo, error) {
	tablesInKVSnap := make(map[int64]*BasicTableInfo)
	partitionsInKVSnap := make(map[int64]BasicPartitionInfo)

	startKey, err := tableInfoKey(gcTs, 0)
	if err != nil {
		log.Fatal("generate lower bound failed", zap.Error(err))
	}
	endKey, err := tableInfoKey(gcTs, math.MaxInt64)
	if err != nil {
		log.Fatal("generate upper bound failed", zap.Error(err))
	}
	snapIter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	})
	if err != nil {
		log.Fatal("new iterator failed", zap.Error(err))
	}
	defer snapIter.Close()
	for snapIter.First(); snapIter.Valid(); snapIter.Next() {
		var table_info_entry PersistedTableInfoEntry
		if _, err := table_info_entry.UnmarshalMsg(snapIter.Value()); err != nil {
			log.Fatal("unmarshal table info entry failed", zap.Error(err))
		}

		tableInfo := model.TableInfo{}
		if err := json.Unmarshal(table_info_entry.TableInfoValue, &tableInfo); err != nil {
			log.Fatal("unmarshal table info failed", zap.Error(err))
		}
		databaseInfo, ok := databaseMap[table_info_entry.SchemaID]
		if !ok {
			log.Panic("database not found",
				zap.Int64("schemaID", table_info_entry.SchemaID),
				zap.String("schemaName", table_info_entry.SchemaName),
				zap.String("tableName", tableInfo.Name.O))
		}
		// TODO: add a unit test for this case
		databaseInfo.Tables[tableInfo.ID] = true
		tablesInKVSnap[tableInfo.ID] = &BasicTableInfo{
			SchemaID: table_info_entry.SchemaID,
			Name:     tableInfo.Name.O,
		}
		if tableInfo.Partition != nil {
			partitionInfo := make(BasicPartitionInfo)
			for _, partition := range tableInfo.Partition.Definitions {
				partitionInfo[partition.ID] = nil
			}
			partitionsInKVSnap[tableInfo.ID] = partitionInfo
		}
	}
	return tablesInKVSnap, partitionsInKVSnap, nil
}

func loadFullTablesInKVSnap(
	snap *pebble.Snapshot,
	gcTs uint64,
	databaseMap map[int64]*BasicDatabaseInfo,
) (map[int64]*model.TableInfo, map[int64]*BasicTableInfo, map[int64]BasicPartitionInfo, error) {
	tableInfosInKVSnap := make(map[int64]*model.TableInfo)
	tablesInKVSnap := make(map[int64]*BasicTableInfo)
	partitionsInKVSnap := make(map[int64]BasicPartitionInfo)

	startKey, err := tableInfoKey(gcTs, 0)
	if err != nil {
		log.Fatal("generate lower bound failed", zap.Error(err))
	}
	endKey, err := tableInfoKey(gcTs, math.MaxInt64)
	if err != nil {
		log.Fatal("generate upper bound failed", zap.Error(err))
	}
	snapIter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	})
	if err != nil {
		log.Fatal("new iterator failed", zap.Error(err))
	}
	defer snapIter.Close()
	for snapIter.First(); snapIter.Valid(); snapIter.Next() {
		var table_info_entry PersistedTableInfoEntry
		if _, err := table_info_entry.UnmarshalMsg(snapIter.Value()); err != nil {
			log.Fatal("unmarshal table info entry failed", zap.Error(err))
		}

		tableInfo := model.TableInfo{}
		if err := json.Unmarshal(table_info_entry.TableInfoValue, &tableInfo); err != nil {
			log.Fatal("unmarshal table info failed", zap.Error(err))
		}
		databaseInfo, ok := databaseMap[table_info_entry.SchemaID]
		if !ok {
			log.Panic("database not found",
				zap.Int64("schemaID", table_info_entry.SchemaID),
				zap.String("schemaName", table_info_entry.SchemaName),
				zap.String("tableName", tableInfo.Name.O))
		}
		// TODO: add a unit test for this case
		tableInfosInKVSnap[tableInfo.ID] = &tableInfo
		databaseInfo.Tables[tableInfo.ID] = true
		tablesInKVSnap[tableInfo.ID] = &BasicTableInfo{
			SchemaID: table_info_entry.SchemaID,
			Name:     tableInfo.Name.O,
		}
		if tableInfo.Partition != nil {
			partitionInfo := make(BasicPartitionInfo)
			for _, partition := range tableInfo.Partition.Definitions {
				partitionInfo[partition.ID] = nil
			}
			partitionsInKVSnap[tableInfo.ID] = partitionInfo
		}
	}
	return tableInfosInKVSnap, tablesInKVSnap, partitionsInKVSnap, nil
}

// load the ddl jobs in the range (gcTs, upperBound] and apply the ddl job to update database and table info
func loadAndApplyDDLHistory(
	snap *pebble.Snapshot,
	gcTs uint64,
	maxFinishedDDLTs uint64,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
	partitionMap map[int64]BasicPartitionInfo,
) (map[int64][]uint64, []uint64, error) {
	tablesDDLHistory := make(map[int64][]uint64)
	tableTriggerDDLHistory := make([]uint64, 0)

	// apply ddl jobs
	startKey, err := ddlJobKey(gcTs + 1)
	if err != nil {
		log.Fatal("generate lower bound failed", zap.Error(err))
	}
	endKey, err := ddlJobKey(maxFinishedDDLTs)
	if err != nil {
		log.Fatal("generate upper bound failed", zap.Error(err))
	}
	snapIter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	})
	if err != nil {
		log.Fatal("new iterator failed", zap.Error(err))
	}
	defer snapIter.Close()
	for snapIter.First(); snapIter.Valid(); snapIter.Next() {
		ddlEvent := unmarshalPersistedDDLEvent(snapIter.Value())
		// Note: no need to skip ddl here
		// 1. for create table and create tables, we always store the ddl with smaller commit ts, so the ddl won't conflict with the tables in the gc snapshot.
		// 2. for other ddls to be ignored, they are already filtered before write to disk.
		handler, ok := allDDLHandlers[model.ActionType(ddlEvent.Type)]
		if !ok {
			log.Panic("unknown ddl type", zap.Any("ddlType", ddlEvent.Type), zap.String("query", ddlEvent.Query))
		}
		tableTriggerDDLHistory = handler.updateDDLHistoryFunc(updateDDLHistoryFuncArgs{
			ddlEvent:               &ddlEvent,
			databaseMap:            databaseMap,
			tableMap:               tableMap,
			partitionMap:           partitionMap,
			tablesDDLHistory:       tablesDDLHistory,
			tableTriggerDDLHistory: tableTriggerDDLHistory,
		})
		handler.updateSchemaMetadataFunc(updateSchemaMetadataFuncArgs{
			event:        &ddlEvent,
			databaseMap:  databaseMap,
			tableMap:     tableMap,
			partitionMap: partitionMap,
		})
	}

	return tablesDDLHistory, tableTriggerDDLHistory, nil
}

// if tableID is a physical partition id, return the logic table id of it
func tryReadLogicalTableID(snap *pebble.Snapshot, tableID int64, version uint64) int64 {
	key, err := partitionInfoKey(version, tableID)
	if err != nil {
		return 0
	}
	val, closer, err := snap.Get(key)
	if err == pebble.ErrNotFound {
		return 0
	}
	if err != nil {
		log.Fatal("read partition meta failed", zap.Error(err))
		return 0
	}
	defer closer.Close()

	if len(val) != 8 {
		log.Fatal("invalid meta value length", zap.Int("len", len(val)))
		return 0
	}
	return int64(binary.BigEndian.Uint64(val))
}

func readTableInfoInKVSnap(snap *pebble.Snapshot, tableID int64, version uint64) *common.TableInfo {
	readRawTableInfo := func(targetTableID int64) (string, *model.TableInfo) {
		targetKey, err := tableInfoKey(version, targetTableID)
		if err != nil {
			log.Fatal("generate table info failed", zap.Error(err))
		}
		value, closer, err := snap.Get(targetKey)
		if err == pebble.ErrNotFound {
			return "", nil
		}
		if err != nil {
			log.Fatal("get table info failed", zap.Error(err))
		}
		defer closer.Close()

		var table_info_entry PersistedTableInfoEntry
		if _, err := table_info_entry.UnmarshalMsg(value); err != nil {
			log.Fatal("unmarshal table info entry failed", zap.Error(err))
		}

		tableInfo := &model.TableInfo{}
		err = json.Unmarshal(table_info_entry.TableInfoValue, tableInfo)
		if err != nil {
			log.Fatal("unmarshal table info failed", zap.Error(err))
		}
		return table_info_entry.SchemaName, tableInfo
	}
	schemaName, tableInfo := readRawTableInfo(tableID)
	if tableInfo == nil {
		// check whether it a physical partition id
		logicalTableID := tryReadLogicalTableID(snap, tableID, version)
		if logicalTableID != 0 {
			schemaName, tableInfo = readRawTableInfo(logicalTableID)
			if tableInfo == nil {
				return nil
			}
		} else {
			return nil
		}
	}
	return common.WrapTableInfo(schemaName, tableInfo)
}

func unmarshalPersistedDDLEvent(value []byte) PersistedDDLEvent {
	var ddlEvent PersistedDDLEvent
	if _, err := ddlEvent.UnmarshalMsg(value); err != nil {
		log.Fatal("unmarshal ddl job failed", zap.Error(err))
	}

	ddlEvent.TableInfo = &model.TableInfo{}
	if err := json.Unmarshal(ddlEvent.TableInfoValue, &ddlEvent.TableInfo); err != nil {
		log.Fatal("unmarshal table info failed", zap.Error(err))
	}
	ddlEvent.TableInfoValue = nil

	if ddlEvent.ExtraTableInfoValue != nil {
		var err error
		ddlEvent.ExtraTableInfo, err = common.UnmarshalJSONToTableInfo(ddlEvent.ExtraTableInfoValue)
		if err != nil {
			log.Fatal("unmarshal pre table info failed", zap.Error(err))
		}
		ddlEvent.ExtraTableInfoValue = nil
	}

	if len(ddlEvent.MultipleTableInfosValue) > 0 {
		ddlEvent.MultipleTableInfos = make([]*model.TableInfo, len(ddlEvent.MultipleTableInfosValue))
		for i := range ddlEvent.MultipleTableInfosValue {
			if err := json.Unmarshal(ddlEvent.MultipleTableInfosValue[i], &ddlEvent.MultipleTableInfos[i]); err != nil {
				log.Fatal("unmarshal multi table info failed", zap.Error(err))
			}
		}
	}
	ddlEvent.MultipleTableInfosValue = nil
	return ddlEvent
}

func readPersistedDDLEvent(snap *pebble.Snapshot, version uint64) PersistedDDLEvent {
	ddlKey, err := ddlJobKey(version)
	if err != nil {
		log.Fatal("generate ddl job key failed", zap.Error(err))
	}
	ddlValue, closer, err := snap.Get(ddlKey)
	if err != nil {
		log.Fatal("get ddl job failed",
			zap.Uint64("version", version),
			zap.Error(err))
	}
	defer closer.Close()
	return unmarshalPersistedDDLEvent(ddlValue)
}

func writePersistedDDLEvent(db *pebble.DB, ddlEvent *PersistedDDLEvent) error {
	batch := db.NewBatch()
	ddlKey, err := ddlJobKey(ddlEvent.FinishedTs)
	if err != nil {
		return err
	}
	ddlEvent.TableInfoValue, err = json.Marshal(ddlEvent.TableInfo)
	if err != nil {
		return err
	}
	if ddlEvent.ExtraTableInfo != nil {
		ddlEvent.ExtraTableInfoValue, err = ddlEvent.ExtraTableInfo.Marshal()
		if err != nil {
			return err
		}
	}
	if len(ddlEvent.MultipleTableInfos) > 0 {
		ddlEvent.MultipleTableInfosValue = make([][]byte, len(ddlEvent.MultipleTableInfos))
		for i := range ddlEvent.MultipleTableInfos {
			ddlEvent.MultipleTableInfosValue[i], err = json.Marshal(ddlEvent.MultipleTableInfos[i])
			if err != nil {
				return err
			}
		}
	}
	ddlValue, err := ddlEvent.MarshalMsg(nil)
	if err != nil {
		return err
	}
	batch.Set(ddlKey, ddlValue, pebble.NoSync)
	return batch.Commit(pebble.NoSync)
}

const mTablePrefix = "Table"

func isTableRawKey(key []byte) bool {
	return strings.HasPrefix(string(key), mTablePrefix)
}

func addSchemaInfoToBatch(batch *pebble.Batch, ts uint64, info *model.DBInfo) {
	schemaKey, err := schemaInfoKey(ts, info.ID)
	if err != nil {
		log.Fatal("generate schema key failed", zap.Error(err))
	}
	schemaValue, err := json.Marshal(info)
	if err != nil {
		log.Fatal("marshal schema info failed", zap.Error(err))
	}
	batch.Set(schemaKey, schemaValue, pebble.NoSync)
}

func addTableInfoToBatch(
	batch *pebble.Batch,
	ts uint64,
	dbInfo *model.DBInfo,
	tableInfoValue []byte,
) (int64, string, []int64) {
	tableInfo := model.TableInfo{}
	if err := json.Unmarshal(tableInfoValue, &tableInfo); err != nil {
		log.Fatal("unmarshal table info failed", zap.Error(err))
	}
	// write table info to batch
	tableKey, err := tableInfoKey(ts, tableInfo.ID)
	if err != nil {
		log.Fatal("generate table key failed", zap.Error(err))
	}
	tableInfoEntry := PersistedTableInfoEntry{
		SchemaID:       dbInfo.ID,
		SchemaName:     dbInfo.Name.O,
		TableInfoValue: tableInfoValue,
	}
	tableInfoEntryValue, err := tableInfoEntry.MarshalMsg(nil)
	if err != nil {
		log.Fatal("marshal table info entry failed", zap.Error(err))
	}
	batch.Set(tableKey, tableInfoEntryValue, pebble.NoSync)

	// write partition info to batch if the table is a partition table
	var partitionIDs []int64
	if tableInfo.Partition != nil {
		for _, partition := range tableInfo.Partition.Definitions {
			partitionKey, err := partitionInfoKey(ts, partition.ID)
			if err != nil {
				log.Fatal("generate partition key failed", zap.Error(err))
			}
			valueBuf := new(bytes.Buffer)
			if err := binary.Write(valueBuf, binary.BigEndian, tableInfo.ID); err != nil {
				log.Fatal("generate partition value failed", zap.Error(err))
			}
			batch.Set(partitionKey, valueBuf.Bytes(), pebble.NoSync)
			partitionIDs = append(partitionIDs, partition.ID)
		}
	}
	return tableInfo.ID, tableInfo.Name.O, partitionIDs
}

// persistSchemaSnapshot write database/table/partition info to disks.
func persistSchemaSnapshot(
	db *pebble.DB,
	tiStore kv.Storage,
	snapTs uint64,
	collectMetaInfo bool,
) (map[int64]*BasicDatabaseInfo, map[int64]*BasicTableInfo, map[int64]BasicPartitionInfo, error) {
	meta := getSnapshotMeta(tiStore, snapTs)
	start := time.Now()
	dbInfos, err := meta.ListDatabases()
	if err != nil {
		log.Fatal("list databases failed", zap.Error(err))
	}

	var databaseMap map[int64]*BasicDatabaseInfo
	var tableMap map[int64]*BasicTableInfo
	var partitionMap map[int64]BasicPartitionInfo
	if collectMetaInfo {
		databaseMap = make(map[int64]*BasicDatabaseInfo)
		tableMap = make(map[int64]*BasicTableInfo)
		partitionMap = make(map[int64]BasicPartitionInfo)
	}
	for _, dbInfo := range dbInfos {
		if filter.IsSysSchema(dbInfo.Name.O) {
			continue
		}
		batch := db.NewBatch()
		addSchemaInfoToBatch(batch, snapTs, dbInfo)
		rawTables, err := meta.GetMetasByDBID(dbInfo.ID)
		if err != nil {
			log.Fatal("get tables failed", zap.Error(err))
		}
		var tablesInDB map[int64]bool
		if collectMetaInfo {
			tablesInDB = make(map[int64]bool)
		}
		for _, rawTable := range rawTables {
			if !isTableRawKey(rawTable.Field) {
				continue
			}
			tableID, tableName, partitionIDs := addTableInfoToBatch(batch, snapTs, dbInfo, rawTable.Value)
			if collectMetaInfo {
				tableMap[tableID] = &BasicTableInfo{
					SchemaID: dbInfo.ID,
					Name:     tableName,
				}
				tablesInDB[tableID] = true
				if len(partitionIDs) > 0 {
					partitionMap[tableID] = make(BasicPartitionInfo)
					for _, partitionID := range partitionIDs {
						partitionMap[tableID].AddPartitionIDs(partitionID)
					}
				}
			}
			// 8M is arbitrary, we can adjust it later
			if batch.Len() >= 8*1024*1024 {
				if err := batch.Commit(pebble.NoSync); err != nil {
					return nil, nil, nil, err
				}
				batch = db.NewBatch()
			}
		}
		if collectMetaInfo {
			databaseInfo := &BasicDatabaseInfo{
				Name:   dbInfo.Name.O,
				Tables: tablesInDB,
			}
			databaseMap[dbInfo.ID] = databaseInfo
		}
		if err := batch.Commit(pebble.NoSync); err != nil {
			return nil, nil, nil, err
		}
	}

	writeGcTs(db, snapTs)

	log.Info("finish write schema snapshot",
		zap.Any("duration", time.Since(start).Seconds()))
	return databaseMap, tableMap, partitionMap, nil
}

func cleanObsoleteData(db *pebble.DB, oldGcTs uint64, gcTs uint64) {
	batch := db.NewBatch()
	defer batch.Close()

	// table info
	{
		startKey, err := tableInfoKey(oldGcTs, 0)
		if err != nil {
			log.Fatal("generate lower bound failed", zap.Error(err))
		}
		endKey, err := tableInfoKey(gcTs, 0)
		if err != nil {
			log.Fatal("generate upper bound failed", zap.Error(err))
		}
		batch.DeleteRange(startKey, endKey, pebble.NoSync)
	}
	// database info
	{
		startKey, err := schemaInfoKey(oldGcTs, 0)
		if err != nil {
			log.Fatal("generate lower bound failed", zap.Error(err))
		}
		endKey, err := schemaInfoKey(gcTs, 0)
		if err != nil {
			log.Fatal("generate upper bound failed", zap.Error(err))
		}
		batch.DeleteRange(startKey, endKey, pebble.NoSync)
	}
	// ddl job
	{
		startKey, err := ddlJobKey(oldGcTs)
		if err != nil {
			log.Fatal("generate lower bound failed", zap.Error(err))
		}
		endKey, err := ddlJobKey(gcTs)
		if err != nil {
			log.Fatal("generate upper bound failed", zap.Error(err))
		}
		batch.DeleteRange(startKey, endKey, pebble.NoSync)
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		log.Fatal("clean obsolete data failed", zap.Error(err))
	}
}

func loadAllPhysicalTablesAtTs(
	storageSnap *pebble.Snapshot,
	gcTs uint64,
	snapVersion uint64,
	tableFilter filter.Filter,
) ([]commonEvent.Table, error) {
	// TODO: respect tableFilter(filter table in kv snap is easy, filter ddl jobs need more attention)
	databaseMap, err := loadDatabasesInKVSnap(storageSnap, gcTs)
	if err != nil {
		return nil, err
	}

	tableInfoMap, tableMap, partitionMap, err := loadFullTablesInKVSnap(storageSnap, gcTs, databaseMap)
	if err != nil {
		return nil, err
	}
	log.Info("after load tables in kv snap",
		zap.Int("tableInfoMapLen", len(tableInfoMap)),
		zap.Int("tableMapLen", len(tableMap)),
		zap.Int("partitionMapLen", len(partitionMap)))

	// apply ddl jobs in range (gcTs, snapVersion]
	startKey, err := ddlJobKey(gcTs + 1)
	if err != nil {
		log.Fatal("generate lower bound failed", zap.Error(err))
	}
	endKey, err := ddlJobKey(snapVersion + 1)
	if err != nil {
		log.Fatal("generate upper bound failed", zap.Error(err))
	}
	snapIter, err := storageSnap.NewIter(&pebble.IterOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	})
	if err != nil {
		log.Fatal("new iterator failed", zap.Error(err))
	}
	defer snapIter.Close()
	for snapIter.First(); snapIter.Valid(); snapIter.Next() {
		ddlEvent := unmarshalPersistedDDLEvent(snapIter.Value())
		handler, ok := allDDLHandlers[model.ActionType(ddlEvent.Type)]
		if !ok {
			log.Panic("unknown ddl type", zap.Any("ddlType", ddlEvent.Type), zap.String("query", ddlEvent.Query))
		}
		// Note: updateFullTableInfoFunc must be called before updateSchemaMetadataFunc,
		// because it depends on some info which may be updated by updateSchemaMetadataFunc.
		handler.updateFullTableInfoFunc(updateFullTableInfoFuncArgs{
			event:        &ddlEvent,
			databaseMap:  databaseMap,
			tableInfoMap: tableInfoMap,
		})
		handler.updateSchemaMetadataFunc(updateSchemaMetadataFuncArgs{
			event:        &ddlEvent,
			databaseMap:  databaseMap,
			tableMap:     tableMap,
			partitionMap: partitionMap,
		})
	}
	log.Info("after load tables from ddl",
		zap.Int("tableInfoMapLen", len(tableInfoMap)),
		zap.Int("tableMapLen", len(tableMap)),
		zap.Int("partitionMapLen", len(partitionMap)))
	tables := make([]commonEvent.Table, 0)
	for tableID, tableInfo := range tableMap {
		if _, ok := databaseMap[tableInfo.SchemaID]; !ok {
			log.Panic("database not found",
				zap.Int64("schemaID", tableInfo.SchemaID),
				zap.Int64("tableID", tableID),
				zap.String("tableName", tableInfo.Name),
				zap.Any("databaseMapLen", len(databaseMap)))
		}
		schemaName := databaseMap[tableInfo.SchemaID].Name
		fullTableInfo, ok := tableInfoMap[tableID]
		if !ok {
			log.Panic("table info not found", zap.Int64("tableID", tableID))
		}
		if tableFilter != nil && tableFilter.ShouldIgnoreTable(schemaName, tableInfo.Name, fullTableInfo) {
			continue
		}
		splitable := isSplitable(fullTableInfo)
		if partitionInfo, ok := partitionMap[tableID]; ok {
			for partitionID := range partitionInfo {
				tables = append(tables, commonEvent.Table{
					SchemaID:  tableInfo.SchemaID,
					TableID:   partitionID,
					Splitable: splitable,
					SchemaTableName: &commonEvent.SchemaTableName{
						SchemaName: schemaName,
						TableName:  tableInfo.Name,
					},
				})
			}
		} else {
			tables = append(tables, commonEvent.Table{
				SchemaID:  tableInfo.SchemaID,
				TableID:   tableID,
				Splitable: splitable,
				SchemaTableName: &commonEvent.SchemaTableName{
					SchemaName: schemaName,
					TableName:  tableInfo.Name,
				},
			})
		}
	}
	log.Info("loadAllPhysicalTablesAtTs",
		zap.Int("tableLen", len(tables)))
	return tables, nil
}
