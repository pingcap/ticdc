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

func TestPersistSchemaSnapshotReturnsWhenListDatabasesSnapshotLost(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	require.NoError(t, err)
	defer db.Close()

	snapshotLostErr := snapshotLostByGCError{}
	_, _, _, err = persistSchemaSnapshot(db, &snapshotLostStorage{err: snapshotLostErr}, 100, true)
	require.ErrorIs(t, err, snapshotLostErr)
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
	schemaValue, schemaCloser, err := storage.db.Get(schemaKey)
	require.NoError(t, err)
	schemaEncrypted := bytes.HasPrefix(schemaValue, encryptedPrefixForTest)
	require.NoError(t, schemaCloser.Close())
	require.True(t, schemaEncrypted)

	tableKey, err := tableInfoKey(version.Ver, tableInfo.ID)
	require.NoError(t, err)
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
