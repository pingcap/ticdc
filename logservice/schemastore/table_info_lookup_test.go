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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schemastore

import (
	"fmt"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestGetTableInfoAtTs(t *testing.T) {
	normal := newEligibleTableInfoForTest(100, "normal")
	partition := newEligiblePartitionTableInfoForTest(200, "partitioned", []model.PartitionDefinition{{ID: 201}, {ID: 202}})
	renamed := newEligibleTableInfoForTest(100, "renamed")
	other := newEligibleTableInfoForTest(300, "other")
	rename := PersistedDDLEvent{
		Type: byte(model.ActionRenameTable), TableID: 100, SchemaName: "test", TableInfo: renamed, FinishedTs: 20,
	}
	like := PersistedDDLEvent{
		Type: byte(model.ActionCreateTable), TableID: 300, ExtraTableID: 100,
		SchemaName: "test", TableInfo: other, FinishedTs: 30,
	}
	drop := PersistedDDLEvent{Type: byte(model.ActionDropTable), TableID: 100, TableInfo: normal, FinishedTs: 20}
	dropPartition := PersistedDDLEvent{
		Type: byte(model.ActionDropTablePartition), TableID: 200, SchemaName: "test", FinishedTs: 20,
		PrevPartitions: []int64{201, 202},
		TableInfo:      newEligiblePartitionTableInfoForTest(200, "partitioned", []model.PartitionDefinition{{ID: 201}}),
	}
	truncate := PersistedDDLEvent{
		Type: byte(model.ActionTruncateTable), TableID: 100, ExtraTableID: 300,
		SchemaName: "test", TableInfo: other, FinishedTs: 20,
	}
	exchange := PersistedDDLEvent{
		Type: byte(model.ActionExchangeTablePartition), TableID: 100, ExtraTableID: 200,
		SchemaName: "test", ExtraSchemaName: "test", TableName: "normal", FinishedTs: 20,
		PrevPartitions: []int64{201, 202}, ExtraTableInfo: common.WrapTableInfo("test", normal),
		TableInfo: newEligiblePartitionTableInfoForTest(200, "partitioned", []model.PartitionDefinition{{ID: 100}, {ID: 202}}),
	}
	for _, tc := range []struct {
		name    string
		tableID int64
		ts      uint64
		events  []PersistedDDLEvent
		deleted bool
	}{
		{name: "snapshot", tableID: 100, ts: 10},
		{name: "partition snapshot", tableID: 201, ts: 10},
		{name: "before DDL", tableID: 100, ts: 19, events: []PersistedDDLEvent{rename}},
		{name: "at DDL", tableID: 100, ts: 20, events: []PersistedDDLEvent{rename}},
		{name: "skip referenced CREATE LIKE", tableID: 100, ts: 30, events: []PersistedDDLEvent{rename, like}},
		{name: "CREATE LIKE snapshot fallback", tableID: 100, ts: 30, events: []PersistedDDLEvent{like}},
		{name: "multiple table infos", tableID: 100, ts: 20, events: []PersistedDDLEvent{{
			Type: byte(model.ActionRenameTables), FinishedTs: 20,
			MultipleTableInfos: []*model.TableInfo{other, renamed}, SchemaNames: []string{"test", "test"},
		}}},
		{name: "create tables", tableID: 300, ts: 20, events: []PersistedDDLEvent{{
			Type: byte(model.ActionCreateTables), FinishedTs: 20, SchemaName: "test",
			MultipleTableInfos: []*model.TableInfo{other},
		}}},
		{name: "drop", tableID: 100, ts: 20, events: []PersistedDDLEvent{drop}, deleted: true},
		{name: "drop schema", tableID: 100, ts: 20, events: []PersistedDDLEvent{{
			Type: byte(model.ActionDropSchema), FinishedTs: 20,
		}}, deleted: true},
		{name: "recover", tableID: 100, ts: 30, events: []PersistedDDLEvent{drop, {
			Type: byte(model.ActionRecoverTable), TableID: 100, SchemaName: "test", TableInfo: normal, FinishedTs: 30,
		}}},
		{name: "remaining partition", tableID: 201, ts: 20, events: []PersistedDDLEvent{dropPartition}},
		{name: "dropped partition", tableID: 202, ts: 20, events: []PersistedDDLEvent{dropPartition}, deleted: true},
		{name: "truncate old table", tableID: 100, ts: 20, events: []PersistedDDLEvent{truncate}, deleted: true},
		{name: "truncate new table", tableID: 300, ts: 20, events: []PersistedDDLEvent{truncate}},
		{name: "exchange old normal", tableID: 100, ts: 20, events: []PersistedDDLEvent{exchange}},
		{name: "exchange old partition", tableID: 201, ts: 20, events: []PersistedDDLEvent{exchange}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storage := newPersistentStorageForTest(t.TempDir(), []mockDBInfo{{
				dbInfo: &model.DBInfo{ID: 1, Name: ast.NewCIStr("test")}, tables: []*model.TableInfo{normal, partition},
			}})
			t.Cleanup(func() { require.NoError(t, storage.close()) })
			for _, event := range tc.events {
				require.NoError(t, writePersistedDDLEvent(storage.db, &event))
				storage.tablesDDLHistory[tc.tableID] = append(storage.tablesDDLHistory[tc.tableID], event.FinishedTs)
			}
			actual, err := storage.getTableInfoAtTs(tc.tableID, tc.ts)
			if tc.deleted {
				require.ErrorAs(t, err, new(*TableDeletedError))
				require.Nil(t, actual)
			} else {
				require.NoError(t, err)
				store := newEmptyVersionedTableInfoStore(tc.tableID)
				require.NoError(t, storage.buildVersionedTableInfoStore(store))
				expected, err := store.getTableInfo(tc.ts)
				require.NoError(t, err)
				require.Equal(t, expected.ToTiDBTableInfo(), actual.ToTiDBTableInfo())
				require.Equal(t, expected.GetSchemaName(), actual.GetSchemaName())
			}
			require.Empty(t, storage.tableInfoStoreMap)
			_, err = storage.getTableInfoAtTs(999, tc.ts)
			require.ErrorIs(t, err, errors.ErrSchemaStorageTableMiss)
			storage.cleanObsoleteDataInMemory(tc.ts + 1)
			_, err = storage.getTableInfoAtTs(tc.tableID, tc.ts)
			require.ErrorIs(t, err, errors.ErrSnapshotLostByGC)
		})
	}
}

func TestGetTableInfoAtTsReadsOnlyLatestSchema(t *testing.T) {
	const historyLength = 100
	storage := newPersistentStorageForTest(t.TempDir(), nil)
	t.Cleanup(func() { require.NoError(t, storage.close()) })
	manager := &countingEncryptionManagerForTest{}
	storage.encryptionManager = manager
	tableInfo := newEligibleTableInfoForTest(100, "a")
	for ts := uint64(1); ts <= historyLength; ts++ {
		event := PersistedDDLEvent{
			Type: byte(model.ActionModifyColumn), TableID: 100, SchemaName: "test", TableInfo: tableInfo, FinishedTs: ts,
		}
		require.NoError(t, writePersistedDDLEventWithEncryption(storage.db, &event, manager, 0))
		storage.tablesDDLHistory[100] = append(storage.tablesDDLHistory[100], ts)
	}
	info, err := storage.getTableInfoAtTs(100, historyLength)
	require.NoError(t, err)
	require.True(t, info.IsEligible(false))
	require.Equal(t, 1, manager.decryptCalls)
	require.Empty(t, storage.tableInfoStoreMap)
	manager.decryptCalls = 0
	require.NoError(t, storage.buildVersionedTableInfoStore(newEmptyVersionedTableInfoStore(100)))
	require.Equal(t, historyLength, manager.decryptCalls)
}

func TestGetTableInfoAtTsConcurrentGC(t *testing.T) {
	initial := []mockDBInfo{{
		dbInfo: &model.DBInfo{ID: 1, Name: ast.NewCIStr("test")},
		tables: []*model.TableInfo{newEligibleTableInfoForTest(100, "a")},
	}}
	storage := newPersistentStorageForTest(t.TempDir(), initial)
	t.Cleanup(func() { require.NoError(t, storage.close()) })
	// All these records refer to, but do not change, table 100. Readers must
	// traverse their captured history and fall back to the matching GC snapshot.
	for ts := uint64(1); ts <= 50; ts++ {
		event := PersistedDDLEvent{
			Type: byte(model.ActionCreateTable), TableID: int64(300 + ts), ExtraTableID: 100,
			SchemaName: "test", TableInfo: newEligibleTableInfoForTest(int64(300+ts), "b"), FinishedTs: ts,
		}
		require.NoError(t, writePersistedDDLEvent(storage.db, &event))
		storage.tablesDDLHistory[100] = append(storage.tablesDDLHistory[100], ts)
	}
	type lookupResult struct {
		info *common.TableInfo
		err  error
	}
	readerDone := make(chan lookupResult, 1)
	go func() {
		var result lookupResult
		for range 100 {
			result.info, result.err = storage.getTableInfoAtTs(100, 50)
			if result.err != nil || result.info.GetTableName() != "a" || !result.info.IsEligible(false) {
				break
			}
		}
		readerDone <- result
	}()
	for gcTs := uint64(1); gcTs <= 50; gcTs++ {
		mockWriteKVSnapOnDisk(storage.db, gcTs, initial)
		storage.cleanObsoleteDataInMemory(gcTs)
		cleanObsoleteData(storage.db, gcTs-1, gcTs)
	}
	result := <-readerDone
	require.NoError(t, result.err)
	require.Equal(t, "a", result.info.GetTableName())
	require.True(t, result.info.IsEligible(false))
	info, err := storage.getTableInfoAtTs(100, 50)
	require.NoError(t, err)
	require.Equal(t, "a", info.GetTableName())
}

func BenchmarkEligibilityTableInfoLookup(b *testing.B) {
	for _, historyLength := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("history=%d", historyLength), func(b *testing.B) {
			tableInfo := newEligibleTableInfoForTest(100, "a")
			storage := newPersistentStorageForTest(b.TempDir(), []mockDBInfo{{
				dbInfo: &model.DBInfo{ID: 1, Name: ast.NewCIStr("test")}, tables: []*model.TableInfo{tableInfo},
			}})
			b.Cleanup(func() { require.NoError(b, storage.close()) })
			// Keep schema size fixed, and build history outside the timed lookup.
			for ts := uint64(1); ts <= uint64(historyLength); ts++ {
				event := PersistedDDLEvent{
					Type: byte(model.ActionModifyColumn), TableID: 100, SchemaName: "test", TableInfo: tableInfo, FinishedTs: ts,
				}
				require.NoError(b, writePersistedDDLEvent(storage.db, &event))
				storage.tablesDDLHistory[100] = append(storage.tablesDDLHistory[100], ts)
			}
			for _, lookup := range []struct {
				name string
				get  func(int64, uint64) (*common.TableInfo, error)
			}{
				{"latest", storage.getTableInfoAtTs},
				{"fullHistory", func(tableID int64, ts uint64) (*common.TableInfo, error) {
					store := newEmptyVersionedTableInfoStore(tableID)
					if err := storage.buildVersionedTableInfoStore(store); err != nil {
						return nil, err
					}
					return store.getTableInfo(ts)
				}},
			} {
				b.Run(lookup.name, func(b *testing.B) {
					require.Empty(b, storage.tableInfoStoreMap)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						info, err := lookup.get(100, uint64(historyLength))
						if err != nil || !info.IsEligible(false) {
							b.Fatalf("eligibility lookup failed: %v", err)
						}
					}
					b.StopTimer()
					require.Empty(b, storage.tableInfoStoreMap)
				})
			}
		})
	}
}
