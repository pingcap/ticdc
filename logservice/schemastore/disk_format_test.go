// Copyright 2026 PingCAP, Inc.
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
	"context"
	"encoding/binary"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
)

type countingEncryptionManagerForTest struct {
	encryptCalls int
	decryptCalls int
}

func (m *countingEncryptionManagerForTest) EncryptData(
	ctx context.Context, keyspaceID uint32, data []byte,
) ([]byte, error) {
	m.encryptCalls++
	return data, nil
}

func (m *countingEncryptionManagerForTest) DecryptData(
	ctx context.Context, keyspaceID uint32, data []byte,
) ([]byte, error) {
	m.decryptCalls++
	return data, nil
}

func TestPersistSchemaSnapshotReturnsWhenListDatabasesSnapshotLost(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	require.NoError(t, err)
	defer db.Close()

	snapshotLostErr := snapshotLostByGCError{}
	_, _, _, err = persistSchemaSnapshot(db, &snapshotLostStorage{err: snapshotLostErr}, 100, true)
	require.ErrorIs(t, err, snapshotLostErr)
}

func TestSchemaStoreDoesNotDecryptLegacyValuesWhenManagerIsConfigured(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	const snapshotTs = uint64(100)
	dbInfo := &model.DBInfo{ID: 100, Name: ast.NewCIStr("test")}
	tableInfo := newEligibleTableInfoForTest(200, "t1")
	tableInfoValue, err := json.Marshal(tableInfo)
	require.NoError(t, err)
	schemaValue, err := json.Marshal(dbInfo)
	require.NoError(t, err)
	schemaKey, err := schemaInfoKey(snapshotTs, dbInfo.ID)
	require.NoError(t, err)
	tableEntryValue, err := (&PersistedTableInfoEntry{
		SchemaID:       dbInfo.ID,
		SchemaName:     dbInfo.Name.O,
		TableInfoValue: tableInfoValue,
	}).MarshalMsg(nil)
	require.NoError(t, err)
	tableKey, err := tableInfoKey(snapshotTs, tableInfo.ID)
	require.NoError(t, err)

	batch := db.NewBatch()
	require.NoError(t, batch.Set(schemaKey, schemaValue, pebble.NoSync))
	require.NoError(t, batch.Set(tableKey, tableEntryValue, pebble.NoSync))
	require.NoError(t, batch.Commit(pebble.NoSync))

	manager := &countingEncryptionManagerForTest{}
	snapshot := db.NewSnapshot()
	defer snapshot.Close()

	databaseMap, err := loadDatabasesInKVSnapWithEncryption(snapshot, snapshotTs, manager, 42)
	require.NoError(t, err)
	_, _, err = loadTablesInKVSnapWithEncryption(snapshot, snapshotTs, databaseMap, manager, 42)
	require.NoError(t, err)
	require.Zero(t, manager.decryptCalls)
}

func TestSchemaStoreDoesNotDecryptLegacyDDLWhenManagerIsConfigured(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	ddlEvent := &PersistedDDLEvent{
		FinishedTs: 100,
		TableInfo:  newEligibleTableInfoForTest(200, "t1"),
	}
	require.NoError(t, writePersistedDDLEvent(db, ddlEvent))
	plainKey, err := ddlJobKey(ddlEvent.FinishedTs)
	require.NoError(t, err)
	maskedKey := keyWithMask(plainKey, 0)
	value, closer, err := db.Get(maskedKey)
	require.NoError(t, err)
	legacyValue := append([]byte(nil), value...)
	require.NoError(t, closer.Close())
	batch := db.NewBatch()
	require.NoError(t, batch.Delete(maskedKey, pebble.NoSync))
	require.NoError(t, batch.Set(plainKey, legacyValue, pebble.NoSync))
	require.NoError(t, batch.Commit(pebble.NoSync))

	manager := &countingEncryptionManagerForTest{}
	snapshot := db.NewSnapshot()
	defer snapshot.Close()
	readPersistedDDLEventWithEncryption(snapshot, ddlEvent.FinishedTs, manager, 42)
	require.Zero(t, manager.decryptCalls)
}

func TestEncryptionLayerKeyUsesEightByteMask(t *testing.T) {
	plainKey, err := ddlJobKey(100)
	require.NoError(t, err)

	unencryptedKey := keyWithMask(plainKey, 0)
	require.Len(t, unencryptedKey, len(plainKey)+8)
	require.Zero(t, binary.BigEndian.Uint64(unencryptedKey[len(plainKey):]))
	require.False(t, keyUsesEncryptionLayer(unencryptedKey))

	encryptionLayerKey := keyWithEncryptionLayer(plainKey)
	require.Len(t, encryptionLayerKey, len(plainKey)+8)
	require.Equal(t, uint64(1), binary.BigEndian.Uint64(encryptionLayerKey[len(plainKey):]))
	require.True(t, keyUsesEncryptionLayer(encryptionLayerKey))

	binary.BigEndian.PutUint64(encryptionLayerKey[len(plainKey):], uint64(1)<<1)
	require.False(t, keyUsesEncryptionLayer(encryptionLayerKey))
	binary.BigEndian.PutUint64(encryptionLayerKey[len(plainKey):], uint64(1)|(uint64(1)<<1))
	require.True(t, keyUsesEncryptionLayer(encryptionLayerKey))
}

func TestPersistedDDLEventUsesZeroMaskWithoutEncryption(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	ddlEvent := &PersistedDDLEvent{
		FinishedTs: 100,
		TableInfo:  newEligibleTableInfoForTest(200, "t1"),
	}
	require.NoError(t, writePersistedDDLEvent(db, ddlEvent))

	plainKey, err := ddlJobKey(ddlEvent.FinishedTs)
	require.NoError(t, err)
	value, closer, err := db.Get(keyWithMask(plainKey, 0))
	require.NoError(t, err)
	require.NotEmpty(t, value)
	require.NoError(t, closer.Close())
	_, plainCloser, err := db.Get(plainKey)
	require.ErrorIs(t, err, pebble.ErrNotFound)
	require.Nil(t, plainCloser)
}

func TestPersistedDDLEventUsesEncryptionLayerKey(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	ddlEvent := &PersistedDDLEvent{
		FinishedTs: 100,
		TableInfo:  newEligibleTableInfoForTest(200, "t1"),
	}
	manager := &prefixEncryptionManagerForTest{}
	require.NoError(t, writePersistedDDLEventWithEncryption(db, ddlEvent, manager, 42))

	plainKey, err := ddlJobKey(ddlEvent.FinishedTs)
	require.NoError(t, err)
	_, plainCloser, err := db.Get(plainKey)
	require.ErrorIs(t, err, pebble.ErrNotFound)
	require.Nil(t, plainCloser)

	encryptionLayerKey := keyWithEncryptionLayer(plainKey)
	require.True(t, keyUsesEncryptionLayer(encryptionLayerKey))
	value, closer, err := db.Get(encryptionLayerKey)
	require.NoError(t, err)
	require.True(t, bytes.HasPrefix(value, encryptedPrefixForTest))
	require.NoError(t, closer.Close())

	snapshot := db.NewSnapshot()
	defer snapshot.Close()
	readEvent := readPersistedDDLEventWithEncryption(snapshot, ddlEvent.FinishedTs, manager, 42)
	require.Equal(t, ddlEvent.FinishedTs, readEvent.FinishedTs)
}

func TestPersistSchemaSnapshotEncryptionRoundTrip(t *testing.T) {
	tikvStore, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, tikvStore.Close())
	}()

	dbInfo := &model.DBInfo{ID: 100, Name: ast.NewCIStr("test")}
	tableInfo := newEligibleTableInfoForTest(200, "t1")
	txn, err := tikvStore.Begin()
	require.NoError(t, err)
	metaMutator := meta.NewMutator(txn)
	require.NoError(t, metaMutator.CreateDatabase(dbInfo))
	require.NoError(t, metaMutator.CreateTableOrView(dbInfo.ID, tableInfo))
	require.NoError(t, txn.Commit(context.Background()))

	version, err := tikvStore.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	encMgr := &prefixEncryptionManagerForTest{}
	storage := &persistentStorage{
		keyspaceID:        42,
		kvStorage:         tikvStore,
		encryptionManager: encMgr,
	}
	storage.initializeFromKVStorage(filepath.Join(t.TempDir(), "schema-store"), version.Ver)
	defer func() {
		require.NoError(t, storage.db.Close())
	}()

	schemaKey, err := schemaInfoKey(version.Ver, dbInfo.ID)
	require.NoError(t, err)
	schemaKey = keyWithEncryptionLayer(schemaKey)
	require.True(t, keyUsesEncryptionLayer(schemaKey))
	schemaValue, schemaCloser, err := storage.db.Get(schemaKey)
	require.NoError(t, err)
	schemaEncrypted := bytes.HasPrefix(schemaValue, encryptedPrefixForTest)
	require.NoError(t, schemaCloser.Close())
	require.True(t, schemaEncrypted)

	tableKey, err := tableInfoKey(version.Ver, tableInfo.ID)
	require.NoError(t, err)
	tableKey = keyWithEncryptionLayer(tableKey)
	require.True(t, keyUsesEncryptionLayer(tableKey))
	tableValue, tableCloser, err := storage.db.Get(tableKey)
	require.NoError(t, err)
	tableEncrypted := bytes.HasPrefix(tableValue, encryptedPrefixForTest)
	require.NoError(t, tableCloser.Close())
	require.True(t, tableEncrypted)

	storage.databaseMap = make(map[int64]*BasicDatabaseInfo)
	storage.tableMap = make(map[int64]*BasicTableInfo)
	storage.partitionMap = make(map[int64]BasicPartitionInfo)
	storage.tablesDDLHistory = make(map[int64][]uint64)
	storage.tableTriggerDDLHistory = nil
	storage.initializeFromDisk()
	require.Contains(t, storage.databaseMap, dbInfo.ID)
	require.Contains(t, storage.tableMap, tableInfo.ID)
	storageSnapshot := storage.db.NewSnapshot()
	storedTableInfo := readTableInfoInKVSnapWithEncryption(
		storageSnapshot, tableInfo.ID, version.Ver, encMgr, storage.keyspaceID,
	)
	require.NoError(t, storageSnapshot.Close())
	require.NotNil(t, storedTableInfo)
	require.Equal(t, tableInfo.ID, storedTableInfo.TableName.TableID)

	tables, err := storage.getAllPhysicalTables(version.Ver, nil)
	require.NoError(t, err)
	require.Len(t, tables, 1)
	require.Equal(t, int64(200), tables[0].TableID)
	require.Equal(t, "test", tables[0].SchemaName)
	require.Equal(t, "t1", tables[0].TableName)
}

type snapshotLostByGCError struct{}

func (snapshotLostByGCError) Error() string {
	return "GC life time is shorter than transaction duration"
}

type snapshotLostStorage struct {
	kv.Storage
	err error
}

func (s *snapshotLostStorage) GetSnapshot(kv.Version) kv.Snapshot {
	return &snapshotLostSnapshot{err: s.err}
}

type snapshotLostSnapshot struct {
	err error
}

func (s *snapshotLostSnapshot) Get(context.Context, kv.Key, ...kv.GetOption) (kv.ValueEntry, error) {
	return kv.ValueEntry{}, s.err
}

func (s *snapshotLostSnapshot) Iter(kv.Key, kv.Key) (kv.Iterator, error) {
	return nil, s.err
}

func (s *snapshotLostSnapshot) IterReverse(kv.Key, kv.Key) (kv.Iterator, error) {
	return nil, s.err
}

func (s *snapshotLostSnapshot) BatchGet(context.Context, []kv.Key, ...kv.BatchGetOption) (map[string]kv.ValueEntry, error) {
	return nil, s.err
}

func (s *snapshotLostSnapshot) SetOption(int, any) {}
