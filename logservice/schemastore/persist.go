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
	"fmt"
	"github.com/pingcap/tiflow/pkg/errors"
	"io"
	"math"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/flowbehappy/tigate/logservice/logpuller"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/filter"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"go.uber.org/zap"
)

// The parent folder to store schema data
const dataDir = "schema_store"

type persistentStorage struct {
	gcRunning atomic.Bool
	// only store ddl event which finished ts is larger than gcTS
	// TODO: > gcTS or >= gcTS
	gcTS atomic.Uint64

	db *pebble.DB
}

type schemaMetaTS struct {
	FinishedDDLTS common.Ts `json:"finished_ddl_ts"`
	SchemaVersion common.Ts `json:"schema_version"`
	ResolvedTS    common.Ts `json:"resolved_ts"`
}

func newPersistentStorage(
	root string, storage kv.Storage, currentGCTS common.Ts,
) (*persistentStorage, schemaMetaTS, DatabaseInfoMap) {
	dbPath := fmt.Sprintf("%s/%s", root, dataDir)
	// FIXME: avoid remove
	err := os.RemoveAll(dbPath)
	if err != nil {
		log.Panic("persistentStorage: fail to remove path", zap.String("path", dbPath), zap.Error(err))
	}
	// TODO: update pebble options
	// TODO: close pebble db at exit
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		log.Fatal("persistentStorage: open db failed", zap.String("path", dbPath), zap.Error(err))
	}

	// TODO: cleanObseleteData?

	dataStorage, metaTS, databaseMap := loadPersistentStorage(db, currentGCTS)
	if dataStorage != nil {
		return dataStorage, metaTS, databaseMap
	}

	// TODO: create a fresh db instance
	databaseMap, err = writeSchemaSnapshotToDisk(db, storage, currentGCTS)
	if err != nil {
		log.Fatal("persistentStorage: write schema snapshot failed", zap.Error(err))
	}
	dataStorage = &persistentStorage{
		gcRunning: atomic.Bool{},
		gcTS:      atomic.Uint64{},
		db:        db,
	}
	dataStorage.gcRunning.Store(false)
	dataStorage.gcTS.Store(currentGCTS)
	// TODO: check whether the following values are correct
	metaTS = schemaMetaTS{
		FinishedDDLTS: currentGCTS,
		SchemaVersion: currentGCTS,
		ResolvedTS:    currentGCTS,
	}

	batch := db.NewBatch()
	err = writeTSToBatch(batch, gcTSKey(), currentGCTS)
	if err != nil {
		log.Fatal("persistentStorage: write gcTS failed", zap.Error(err))
	}
	err = writeTSToBatch(batch, metaTSKey(), metaTS.ResolvedTS, metaTS.FinishedDDLTS, metaTS.SchemaVersion)
	if err != nil {
		log.Fatal("persistentStorage: write metaTS failed", zap.Error(err))
	}
	err = batch.Commit(pebble.NoSync)
	if err != nil {
		log.Fatal("persistentStorage: commit batch failed", zap.Error(err))
	}

	log.Info("persistentStorage: schema store create a fresh storage", zap.String("path", dbPath))
	return dataStorage, metaTS, databaseMap
}

func loadPersistentStorage(db *pebble.DB, minRequiredTS common.Ts) (*persistentStorage, schemaMetaTS, DatabaseInfoMap) {
	snap := db.NewSnapshot()
	defer func() {
		err := snap.Close()
		if err != nil {
			log.Warn("persistentStorage: close snapshot failed", zap.Error(err))
		}
	}()

	dataStorage := &persistentStorage{
		gcRunning: atomic.Bool{},
		gcTS:      atomic.Uint64{},
		db:        db,
	}
	dataStorage.gcRunning.Store(false)
	values, err := readTSFromSnapshot(snap, gcTSKey())
	// TODO: distinguish between non exist key error and other error
	if err != nil || len(values) != 1 {
		return nil, schemaMetaTS{}, nil
	}
	dataStorage.gcTS.Store(values[0])

	// gcTS cannot go back
	if minRequiredTS < dataStorage.gcTS.Load() {
		log.Panic("persistentStorage: gcSafePoint < gcTs, shouldn't happen",
			zap.Uint64("gcSafePoint", minRequiredTS), zap.Uint64("gcTS", dataStorage.gcTS.Load()))
	}

	var metaTS schemaMetaTS
	values, err = readTSFromSnapshot(snap, metaTSKey())
	if err != nil || len(values) != 3 {
		return nil, schemaMetaTS{}, nil
	}
	metaTS.ResolvedTS = values[0]
	metaTS.FinishedDDLTS = values[1]
	metaTS.SchemaVersion = values[2]

	// FIXME: > or >=?
	if minRequiredTS > metaTS.ResolvedTS {
		return nil, schemaMetaTS{}, nil
	}

	databaseMap := make(DatabaseInfoMap)

	snapshotLowerBound, err := snapshotSchemaKey(dataStorage.getGCTS(), 0)
	if err != nil {
		log.Fatal("persistentStorage: generate lower bound failed", zap.Error(err))
	}
	snapshotUpperBound, err := snapshotSchemaKey(dataStorage.getGCTS(), int64(math.MaxInt64))
	if err != nil {
		log.Fatal("persistentStorage: generate upper bound failed", zap.Error(err))
	}
	snapIter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: snapshotLowerBound,
		UpperBound: snapshotUpperBound,
	})
	if err != nil {
		log.Fatal("persistentStorage: new iterator failed", zap.Error(err))
	}
	defer func() {
		err = snapIter.Close()
		if err != nil {
			log.Warn("persistentStorage: close iterator failed", zap.Error(err))
		}
	}()
	for snapIter.First(); snapIter.Valid(); snapIter.Next() {
		dbInfo := &model.DBInfo{}
		value, err := snapIter.ValueAndErr()
		if err != nil {
			log.Fatal("persistentStorage: read value failed", zap.Error(err))
		}
		err = json.Unmarshal(value, dbInfo)
		if err != nil {
			log.Fatal("persistentStorage: unmarshal db info failed", zap.Error(err))
		}
		databaseMap[dbInfo.ID] = &DatabaseInfo{
			Name:          dbInfo.Name.O,
			Tables:        make([]common.TableID, 0),
			CreateVersion: dataStorage.getGCTS(),
			DeleteVersion: common.Ts(math.MaxUint64),
		}
	}

	ddlJobLowerBound, err := ddlJobSchemaKey(dataStorage.getGCTS(), 0)
	if err != nil {
		log.Fatal("persistentStorage: generate lower bound failed", zap.Error(err))
	}
	ddlJobUpperBound, err := ddlJobSchemaKey(common.Ts(math.MaxUint64), int64(math.MaxInt64))
	if err != nil {
		log.Fatal("persistentStorage: generate upper bound failed", zap.Error(err))
	}
	ddlJobIter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: ddlJobLowerBound,
		UpperBound: ddlJobUpperBound,
	})
	if err != nil {
		log.Fatal("persistentStorage: new ddl job iterator failed", zap.Error(err))
	}
	defer func() {
		err = ddlJobIter.Close()
		if err != nil {
			log.Warn("persistentStorage: close ddl job iterator failed", zap.Error(err))
		}
	}()

	for ddlJobIter.First(); ddlJobIter.Valid(); ddlJobIter.Next() {
		ddlJob := &model.Job{}
		value, err := ddlJobIter.ValueAndErr()
		if err != nil {
			log.Fatal("persistentStorage: read value failed", zap.Error(err))
		}
		err = json.Unmarshal(value, ddlJob)
		if err != nil {
			log.Fatal("persistentStorage: unmarshal ddl job info failed", zap.Error(err))
		}
		err = handleResolvedDDLJob(ddlJob, databaseMap, nil)
		if err != nil {
			log.Fatal("persistentStorage: handle ddl job failed", zap.Error(err))
		}
	}

	return dataStorage, metaTS, databaseMap
}

func (p *persistentStorage) writeDDLEvent(ddlEvent DDLEvent) error {
	ddlValue, err := json.Marshal(ddlEvent)
	if err != nil {
		return err
	}
	batch := p.db.NewBatch()
	switch ddlEvent.Job.Type {
	case model.ActionCreateSchema, model.ActionModifySchemaCharsetAndCollate, model.ActionDropSchema:
		ddlKey, err := ddlJobSchemaKey(
			ddlEvent.Job.BinlogInfo.FinishedTS,
			ddlEvent.Job.SchemaID)
		if err != nil {
			return err
		}
		batch.Set(ddlKey, ddlValue, pebble.NoSync)
		return batch.Commit(pebble.NoSync)
	default:
		// TODO: for cross table ddl, need write two events(may be we need a table_id -> name map?)
		ddlKey, err := ddlJobTableKey(
			ddlEvent.Job.BinlogInfo.FinishedTS,
			ddlEvent.Job.TableID)
		if err != nil {
			return err
		}

		batch.Set(ddlKey, ddlValue, pebble.NoSync)
		indexDDLKey, err := indexDDLJobKey(ddlEvent.Job.TableID, ddlEvent.Job.BinlogInfo.FinishedTS)
		if err != nil {
			return err
		}
		batch.Set(indexDDLKey, nil, pebble.NoSync)
		return batch.Commit(pebble.NoSync)
	}
}

func (p *persistentStorage) updateStoreMeta(resolvedTS common.Ts, finishedDDLTS common.Ts, schemaVersion common.Ts) error {
	batch := p.db.NewBatch()
	err := writeTSToBatch(batch, metaTSKey(), resolvedTS, finishedDDLTS, schemaVersion)
	if err != nil {
		return errors.Trace(err)
	}
	err = batch.Commit(pebble.NoSync)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func tryReadTableInfoFromSnapshot(
	snap *pebble.Snapshot,
	tableID common.TableID,
	startTS common.Ts,
	getSchemaName func(schemaID int64) (string, error),
) (*common.TableInfo, error) {
	lowerBound, err := generateKey(indexSnapshotKeyPrefix, uint64(tableID))
	if err != nil {
		log.Fatal("generate lower bound failed", zap.Error(err))
	}
	upperBound, err := generateKey(indexSnapshotKeyPrefix, uint64(tableID+1))
	if err != nil {
		log.Fatal("generate upper bound failed", zap.Error(err))
	}
	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		log.Fatal("new iterator failed", zap.Error(err))
	}
	defer iter.Close()
	for iter.Last(); iter.Valid(); iter.Prev() {
		_, version, schemaID, err := parseIndexSnapshotKey(iter.Key())
		if err != nil {
			log.Fatal("parse index key failed", zap.Any("key", iter.Key()), zap.Error(err))
		}
		if version > startTS {
			continue
		}
		targetKey, err := snapshotTableKey(version, tableID)
		if err != nil {
			return nil, err
		}
		value, closer, err := snap.Get(targetKey)
		if err != nil {
			return nil, err
		}
		defer closer.Close()

		tableInfo := &model.TableInfo{}
		err = json.Unmarshal(value, tableInfo)
		if err != nil {
			return nil, err
		}
		schemaName, err := getSchemaName(schemaID)
		if err != nil {
			return nil, err
		}
		return common.WrapTableInfo(schemaID, schemaName, version, tableInfo), nil
	}
	return nil, nil
}

func readDDLJobTimestampForTable(snap *pebble.Snapshot, tableID common.TableID, endTS common.Ts) []common.Ts {
	lowerBound, err := generateKey(indexDDLJobKeyPrefix, uint64(tableID))
	if err != nil {
		log.Fatal("generate lower bound failed", zap.Error(err))
	}
	upperBound, err := generateKey(indexDDLJobKeyPrefix, uint64(tableID), endTS+1)
	if err != nil {
		log.Fatal("generate upper bound failed", zap.Error(err))
	}
	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		log.Fatal("new iterator failed", zap.Error(err))
	}
	defer iter.Close()
	result := make([]common.Ts, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		_, version, err := parseIndexDDLJobKey(iter.Key())
		if err != nil {
			log.Fatal("parse index key failed", zap.Error(err))
		}
		result = append(result, version)
	}
	return result
}

// build a versionedTableInfoStore within the time range (startTS, endTS]
func (p *persistentStorage) buildVersionedTableInfoStore(
	store *versionedTableInfoStore,
	startTS common.Ts,
	endTS common.Ts,
	getSchemaName func(schemaID int64) (string, error),
) error {
	tableID := store.getTableID()
	snap := p.db.NewSnapshot()
	defer snap.Close()
	tableInfoFromSnap, err := tryReadTableInfoFromSnapshot(snap, tableID, startTS, getSchemaName)
	if err != nil {
		return err
	}
	if tableInfoFromSnap != nil {
		store.addInitialTableInfo(tableInfoFromSnap)
	}
	allDDLJobTS := readDDLJobTimestampForTable(snap, tableID, endTS)
	for _, ts := range allDDLJobTS {
		if tableInfoFromSnap != nil && ts <= tableInfoFromSnap.Version {
			continue
		}
		ddlKey, err := ddlJobTableKey(ts, tableID)
		if err != nil {
			log.Fatal("generate ddl key failed", zap.Error(err))
		}
		value, closer, err := snap.Get(ddlKey)
		if err != nil {
			log.Fatal("get ddl job failed", zap.Error(err))
		}
		defer closer.Close()
		var ddlEvent DDLEvent
		err = json.Unmarshal(value, &ddlEvent)
		if err != nil {
			log.Fatal("unmarshal ddl job failed", zap.Error(err))
		}
		schemaName, err := getSchemaName(ddlEvent.Job.SchemaID)
		if err != nil {
			log.Fatal("get schema name failed", zap.Error(err))
		}
		ddlEvent.Job.SchemaName = schemaName
		store.applyDDL(ddlEvent.Job)
	}
	return nil
}

func (p *persistentStorage) getGCTS() common.Ts {
	return p.gcTS.Load()
}

func (p *persistentStorage) gc(gcTS common.Ts) error {
	if p.gcRunning.CompareAndSwap(false, true) {
		return nil
	}
	defer p.gcRunning.Store(false)
	p.gcTS.Store(gcTS)
	// TODO: write snapshot(schema and table) to disk(don't need to be in the same batch) and maintain the key that need be deleted(or just write it to a delete batch)

	// update gcTS in disk, must do it before delete any data
	batch := p.db.NewBatch()
	if err := writeTSToBatch(batch, gcTSKey(), gcTS); err != nil {
		return err
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return err
	}
	// TODO: delete old data(including index data, so we need to read data one by one)
	// may be write and delete in the same batch?

	return nil
}

const mTablePrefix = "Table"

func isTableRawKey(key []byte) bool {
	return strings.HasPrefix(string(key), mTablePrefix)
}

const snapshotSchemaKeyPrefix = "ss_"
const snapshotTableKeyPrefix = "st_"

const ddlJobSchemaKeyPrefix = "ds_"
const ddlJobTableKeyPrefix = "dt_"

// table_id -> common.Ts
const indexSnapshotKeyPrefix = "is_"

// table_id -> common.Ts
const indexDDLJobKeyPrefix = "id_"

func gcTSKey() []byte {
	return []byte("gc")
}

func metaTSKey() []byte {
	return []byte("me")
}

// key format: <prefix><values[0]><values[1]>...
func generateKey(prefix string, values ...uint64) ([]byte, error) {
	buf := new(bytes.Buffer)
	_, err := buf.WriteString(prefix)
	if err != nil {
		return nil, err
	}
	for _, v := range values {
		err = binary.Write(buf, binary.BigEndian, v)
		if err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func checkAndParseKey(key []byte, prefix string) ([]uint64, error) {
	if !strings.HasPrefix(string(key), prefix) {
		return nil, fmt.Errorf("invalid key prefix: %s", string(key))
	}
	buf := bytes.NewBuffer(key)
	buf.Next(len(prefix))
	var values []uint64
	for {
		var v uint64
		err := binary.Read(buf, binary.BigEndian, &v)
		if err != nil {
			if err == io.EOF {
				return values, nil
			}
			return nil, err
		}
		values = append(values, v)
	}
}

func snapshotSchemaKey(ts common.Ts, schemaID int64) ([]byte, error) {
	return generateKey(snapshotSchemaKeyPrefix, ts, uint64(schemaID))
}

func snapshotTableKey(ts common.Ts, tableID common.TableID) ([]byte, error) {
	return generateKey(snapshotTableKeyPrefix, ts, uint64(tableID))
}

func ddlJobSchemaKey(ts common.Ts, schemaID int64) ([]byte, error) {
	return generateKey(ddlJobSchemaKeyPrefix, ts, uint64(schemaID))
}

func ddlJobTableKey(ts common.Ts, tableID common.TableID) ([]byte, error) {
	return generateKey(ddlJobTableKeyPrefix, ts, uint64(tableID))
}

func indexSnapshotKey(tableID common.TableID, commitTS common.Ts, schemaID int64) ([]byte, error) {
	return generateKey(indexSnapshotKeyPrefix, uint64(tableID), commitTS, uint64(schemaID))
}

func indexDDLJobKey(tableID common.TableID, commitTS common.Ts) ([]byte, error) {
	return generateKey(indexDDLJobKeyPrefix, uint64(tableID), commitTS)
}

func parseIndexSnapshotKey(key []byte) (common.TableID, common.Ts, int64, error) {
	values, err := checkAndParseKey(key, indexSnapshotKeyPrefix)
	if err != nil || len(values) != 3 {
		log.Fatal("parse index key failed",
			zap.Any("key", key),
			zap.Any("keyLength", len(key)),
			zap.Any("values", values),
			zap.Error(err))
	}
	return common.TableID(values[0]), values[1], int64(values[2]), nil
}

func parseIndexDDLJobKey(key []byte) (common.TableID, common.Ts, error) {
	values, err := checkAndParseKey(key, indexDDLJobKeyPrefix)
	if err != nil || len(values) != 2 {
		log.Fatal("parse index key failed", zap.Error(err))
	}
	return common.TableID(values[0]), values[1], nil
}

func writeTSToBatch(batch *pebble.Batch, key []byte, ts ...common.Ts) error {
	buf := new(bytes.Buffer)
	for _, t := range ts {
		err := binary.Write(buf, binary.BigEndian, t)
		if err != nil {
			return err
		}
	}
	batch.Set(key, buf.Bytes(), pebble.NoSync)
	return nil
}

func readTSFromSnapshot(snap *pebble.Snapshot, key []byte) ([]common.Ts, error) {
	value, closer, err := snap.Get(key)
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	buf := bytes.NewBuffer(value)
	var values []common.Ts
	for {
		var ts common.Ts
		err := binary.Read(buf, binary.BigEndian, &ts)
		if err != nil {
			if err == io.EOF {
				return values, nil
			}
			return nil, err
		}
		values = append(values, ts)
	}
}

func writeSchemaSnapshotToDisk(db *pebble.DB, tiStore kv.Storage, ts common.Ts) (DatabaseInfoMap, error) {
	meta := logpuller.GetSnapshotMeta(tiStore, ts)
	start := time.Now()
	dbinfos, err := meta.ListDatabases()
	if err != nil {
		log.Fatal("list databases failed", zap.Error(err))
	}

	databaseMap := make(DatabaseInfoMap, len(dbinfos))
	for _, dbinfo := range dbinfos {
		if filter.IsSysSchema(dbinfo.Name.O) {
			continue
		}
		batch := db.NewBatch()
		defer batch.Close()
		databaseInfo := &DatabaseInfo{
			Name:          dbinfo.Name.O,
			Tables:        make([]common.TableID, 0),
			CreateVersion: ts,
			DeleteVersion: common.Ts(math.MaxUint64),
		}
		databaseMap[dbinfo.ID] = databaseInfo
		schemaKey, err := snapshotSchemaKey(ts, dbinfo.ID)
		if err != nil {
			log.Fatal("generate schema key failed", zap.Error(err))
		}
		schemaValue, err := json.Marshal(dbinfo)
		if err != nil {
			log.Fatal("marshal schema failed", zap.Error(err))
		}
		batch.Set(schemaKey, schemaValue, pebble.NoSync)
		rawTables, err := meta.GetMetasByDBID(dbinfo.ID)
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
			databaseInfo.Tables = append(databaseInfo.Tables, tbName.ID)
			tableKey, err := snapshotTableKey(ts, tbName.ID)
			if err != nil {
				log.Fatal("generate table key failed", zap.Error(err))
			}
			batch.Set(tableKey, rawTable.Value, pebble.NoSync)
			indexKey, err := indexSnapshotKey(tbName.ID, ts, dbinfo.ID)
			if err != nil {
				log.Fatal("generate index key failed", zap.Error(err))
			}
			batch.Set(indexKey, nil, pebble.NoSync)
		}
		if err := batch.Commit(pebble.NoSync); err != nil {
			return nil, err
		}
	}

	log.Info("finish write schema snapshot",
		zap.Any("duration", time.Since(start).Seconds()))
	return databaseMap, nil
}
