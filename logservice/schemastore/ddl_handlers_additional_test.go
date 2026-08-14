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
	"math"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestGetAllPhysicalTablesReplaysTopologyChangingDDL(t *testing.T) {
	testCases := []struct {
		name        string
		initial     []mockDBInfo
		jobs        []*model.Job
		snapshotTs  uint64
		expectedIDs []int64
	}{
		{
			name: "drop schema",
			initial: []mockDBInfo{
				{
					dbInfo: &model.DBInfo{ID: 10, Name: ast.NewCIStr("test")},
					tables: []*model.TableInfo{
						newEligibleTableInfoForTest(100, "t"),
						newEligiblePartitionTableInfoForTest(200, "pt", []model.PartitionDefinition{{ID: 201}, {ID: 202}}),
					},
				},
			},
			jobs:        []*model.Job{buildDropSchemaJobForTest(10, 1000)},
			snapshotTs:  1000,
			expectedIDs: nil,
		},
		{
			name: "exchange partition",
			initial: []mockDBInfo{
				{
					dbInfo: &model.DBInfo{ID: 10, Name: ast.NewCIStr("test")},
					tables: []*model.TableInfo{
						newEligiblePartitionTableInfoForTest(200, "pt", []model.PartitionDefinition{{ID: 201}, {ID: 202}, {ID: 203}}),
					},
				},
				{
					dbInfo: &model.DBInfo{ID: 20, Name: ast.NewCIStr("test2")},
					tables: []*model.TableInfo{newEligibleTableInfoForTest(300, "t")},
				},
			},
			jobs: []*model.Job{
				buildExchangePartitionJobForTest(20, 300, 200, "pt", []int64{201, 202, 300}, 1000),
			},
			snapshotTs:  1000,
			expectedIDs: []int64{201, 202, 203, 300},
		},
		{
			name: "rename tables",
			initial: []mockDBInfo{
				{
					dbInfo: &model.DBInfo{ID: 10, Name: ast.NewCIStr("test")},
					tables: []*model.TableInfo{
						newEligibleTableInfoForTest(100, "t1"),
						newEligibleTableInfoForTest(101, "t2"),
					},
				},
			},
			jobs: []*model.Job{
				buildRenameTablesJobForTest(
					[]int64{10, 10}, []int64{10, 10}, []int64{100, 101},
					[]string{"test", "test"}, []string{"t1", "t2"}, []string{"t1_new", "t2_new"}, 1000),
			},
			snapshotTs:  1000,
			expectedIDs: []int64{100, 101},
		},
		{
			name: "create tables",
			initial: []mockDBInfo{
				{dbInfo: &model.DBInfo{ID: 10, Name: ast.NewCIStr("test")}},
			},
			jobs: []*model.Job{
				buildCreateTablesJobForTest(10, []int64{100, 101}, []string{"t1", "t2"}, 1000),
			},
			snapshotTs:  1000,
			expectedIDs: []int64{100, 101},
		},
		{
			name: "reorganize partition",
			initial: []mockDBInfo{
				{
					dbInfo: &model.DBInfo{ID: 10, Name: ast.NewCIStr("test")},
					tables: []*model.TableInfo{
						newEligiblePartitionTableInfoForTest(200, "pt", []model.PartitionDefinition{{ID: 201}, {ID: 202}, {ID: 203}}),
					},
				},
			},
			jobs: []*model.Job{
				buildPartitionTableRelatedJobForTest(
					model.ActionReorganizePartition, 10, 200, "pt", []int64{201, 204, 205}, 1000),
			},
			snapshotTs:  1000,
			expectedIDs: []int64{201, 204, 205},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dbPath := t.TempDir()
			storage := newPersistentStorageForTest(dbPath, tc.initial)
			for _, job := range tc.jobs {
				require.NoError(t, storage.handleDDLJob(job))
			}

			assertPhysicalTableIDs(t, storage, tc.snapshotTs, tc.expectedIDs)
			require.NoError(t, storage.close())

			storage = loadPersistentStorageFromPathForTest(dbPath, math.MaxUint64)
			t.Cleanup(func() { require.NoError(t, storage.close()) })
			assertPhysicalTableIDs(t, storage, tc.snapshotTs, tc.expectedIDs)
		})
	}
}

func TestRegisteredTableInfoForComplexDDL(t *testing.T) {
	t.Run("rename tables", func(t *testing.T) {
		storage := newPersistentStorageForTest(t.TempDir(), []mockDBInfo{
			{
				dbInfo: &model.DBInfo{ID: 10, Name: ast.NewCIStr("test")},
				tables: []*model.TableInfo{
					newEligibleTableInfoForTest(100, "t1"),
					newEligiblePartitionTableInfoForTest(200, "pt", []model.PartitionDefinition{{ID: 201}, {ID: 202}}),
				},
			},
		})
		t.Cleanup(func() { require.NoError(t, storage.close()) })

		require.NoError(t, storage.registerTable(100, 0))
		require.NoError(t, storage.registerTable(201, 0))
		job := buildRenameTablesJobForTest(
			[]int64{10, 10}, []int64{10, 10}, []int64{100, 200},
			[]string{"test", "test"}, []string{"t1", "pt"}, []string{"t1_new", "pt_new"}, 1000)
		job.BinlogInfo.MultipleTableInfos[1] = newEligiblePartitionTableInfoForTest(
			200, "pt_new", []model.PartitionDefinition{{ID: 201}, {ID: 202}})
		require.NoError(t, storage.handleDDLJob(job))

		assertTableInfoName(t, storage, 100, 1000, "t1_new")
		assertTableInfoName(t, storage, 201, 1000, "pt_new")
		require.NoError(t, storage.registerTable(202, 1000))
		assertTableInfoName(t, storage, 202, 1000, "pt_new")
	})

	t.Run("alter table partitioning", func(t *testing.T) {
		storage := newPersistentStorageForTest(t.TempDir(), []mockDBInfo{
			{
				dbInfo: &model.DBInfo{ID: 10, Name: ast.NewCIStr("test")},
				tables: []*model.TableInfo{newEligibleTableInfoForTest(100, "t")},
			},
		})
		t.Cleanup(func() { require.NoError(t, storage.close()) })

		require.NoError(t, storage.registerTable(100, 0))
		require.NoError(t, storage.handleDDLJob(
			buildAlterTablePartitioningJobForTest(10, 100, 200, []int64{201, 202}, "t", 1000)))
		assertTableDeleted(t, storage, 100, 1000)
		require.NoError(t, storage.registerTable(201, 1000))
		assertTableInfoName(t, storage, 201, 1000, "t")
	})

	t.Run("remove partitioning", func(t *testing.T) {
		storage := newPersistentStorageForTest(t.TempDir(), []mockDBInfo{
			{
				dbInfo: &model.DBInfo{ID: 10, Name: ast.NewCIStr("test")},
				tables: []*model.TableInfo{
					newEligiblePartitionTableInfoForTest(200, "pt", []model.PartitionDefinition{{ID: 201}, {ID: 202}}),
				},
			},
		})
		t.Cleanup(func() { require.NoError(t, storage.close()) })

		require.NoError(t, storage.registerTable(201, 0))
		require.NoError(t, storage.handleDDLJob(buildRemovePartitioningJobForTest(10, 200, 300, "pt", 1000)))
		assertTableDeleted(t, storage, 201, 1000)
		require.NoError(t, storage.registerTable(300, 1000))
		assertTableInfoName(t, storage, 300, 1000, "pt")
	})
}

func TestBuildSchemaAndViewDDLEvents(t *testing.T) {
	testCases := []struct {
		name                 string
		raw                  PersistedDDLEvent
		expectedInfluence    commonEvent.InfluenceType
		expectedSchemaID     int64
		expectedDropDatabase string
		expectDroppedTables  bool
	}{
		{
			name: "drop schema",
			raw: PersistedDDLEvent{
				Type: byte(model.ActionDropSchema), SchemaID: 10, SchemaName: "test", FinishedTs: 1000,
			},
			expectedInfluence:    commonEvent.InfluenceTypeDB,
			expectedSchemaID:     10,
			expectedDropDatabase: "test",
			expectDroppedTables:  true,
		},
		{
			name: "modify schema charset",
			raw: PersistedDDLEvent{
				Type: byte(model.ActionModifySchemaCharsetAndCollate), SchemaID: 10, SchemaName: "test", FinishedTs: 1000,
			},
			expectedInfluence: commonEvent.InfluenceTypeDB,
			expectedSchemaID:  10,
		},
		{
			name: "create view",
			raw: PersistedDDLEvent{
				Type: byte(model.ActionCreateView), SchemaID: 10, SchemaName: "test", TableName: "v", FinishedTs: 1000,
			},
			expectedInfluence: commonEvent.InfluenceTypeAll,
		},
		{
			name: "drop view",
			raw: PersistedDDLEvent{
				Type: byte(model.ActionDropView), SchemaID: 10, SchemaName: "test", TableName: "v", FinishedTs: 1000,
			},
			expectedInfluence: commonEvent.InfluenceTypeNormal,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ddlEvent, ok, err := buildDDLEvent(&tc.raw, nil, 0)
			require.NoError(t, err)
			require.True(t, ok)
			require.NotNil(t, ddlEvent.BlockedTables)
			require.Equal(t, tc.expectedInfluence, ddlEvent.BlockedTables.InfluenceType)
			require.Equal(t, tc.expectedSchemaID, ddlEvent.BlockedTables.SchemaID)
			if tc.name == "drop view" {
				require.Equal(t, []int64{common.DDLSpanTableID}, ddlEvent.BlockedTables.TableIDs)
			}
			if tc.expectDroppedTables {
				require.NotNil(t, ddlEvent.NeedDroppedTables)
				require.Equal(t, commonEvent.InfluenceTypeDB, ddlEvent.NeedDroppedTables.InfluenceType)
				require.Equal(t, tc.expectedSchemaID, ddlEvent.NeedDroppedTables.SchemaID)
			}
			if tc.expectedDropDatabase != "" {
				require.NotNil(t, ddlEvent.TableNameChange)
				require.Equal(t, tc.expectedDropDatabase, ddlEvent.TableNameChange.DropDatabaseName)
			}

			_, ok, err = buildDDLEvent(
				&tc.raw, buildTableFilterByNameForTest("unrelated", "table"), 0)
			require.NoError(t, err)
			require.False(t, ok)
		})
	}
}

func TestAddIndexPersistsIndexIDsInDDLEvent(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	t.Cleanup(helper.Close)
	helper.Tk().MustExec("use test")
	helper.DDL2Event("create table t (id int primary key, c1 int)")
	job := helper.DDL2Job("alter table t add index (c1)")
	expectedIndexIDs := getIndexIDs(job)
	require.Len(t, expectedIndexIDs, 1)

	storage := newPersistentStorageForTest(t.TempDir(), []mockDBInfo{
		{
			dbInfo: &model.DBInfo{ID: job.SchemaID, Name: ast.NewCIStr("test")},
			tables: []*model.TableInfo{newEligibleTableInfoForTest(job.TableID, "t")},
		},
	})
	t.Cleanup(func() { require.NoError(t, storage.close()) })
	require.NoError(t, storage.handleDDLJob(job))

	events, err := storage.fetchTableDDLEvents(
		common.NewDispatcherID(), job.TableID, nil,
		job.BinlogInfo.FinishedTS-1, job.BinlogInfo.FinishedTS)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, byte(model.ActionAddIndex), events[0].Type)
	require.Equal(t, expectedIndexIDs, events[0].IndexIDs)
}

func TestHandleDDLJobSkipRules(t *testing.T) {
	t.Run("duplicate create table", func(t *testing.T) {
		storage := newPersistentStorageForTest(t.TempDir(), []mockDBInfo{
			{dbInfo: &model.DBInfo{ID: 10, Name: ast.NewCIStr("test")}},
		})
		t.Cleanup(func() { require.NoError(t, storage.close()) })

		require.NoError(t, storage.handleDDLJob(buildCreateTableJobForTest(10, 100, "t", 1000)))
		require.NoError(t, storage.handleDDLJob(buildCreateTableJobForTest(10, 100, "t", 1010)))
		require.Equal(t, []uint64{1000}, storage.tableTriggerDDLHistory)
		require.Equal(t, []uint64{1000}, storage.tablesDDLHistory[100])
		require.Len(t, storage.tableMap, 1)
	})

	t.Run("duplicate create tables", func(t *testing.T) {
		storage := newPersistentStorageForTest(t.TempDir(), []mockDBInfo{
			{dbInfo: &model.DBInfo{ID: 10, Name: ast.NewCIStr("test")}},
		})
		t.Cleanup(func() { require.NoError(t, storage.close()) })

		first := buildCreateTablesJobForTest(10, []int64{100, 101}, []string{"t1", "t2"}, 1000)
		duplicate := buildCreateTablesJobForTest(10, []int64{100, 101}, []string{"t1", "t2"}, 1010)
		require.NoError(t, storage.handleDDLJob(first))
		require.NoError(t, storage.handleDDLJob(duplicate))
		require.Equal(t, []uint64{1000}, storage.tableTriggerDDLHistory)
		require.Equal(t, []uint64{1000}, storage.tablesDDLHistory[100])
		require.Equal(t, []uint64{1000}, storage.tablesDDLHistory[101])
		require.Len(t, storage.tableMap, 2)
	})

	t.Run("ignored and unknown actions", func(t *testing.T) {
		storage := newPersistentStorageForTest(t.TempDir(), nil)
		t.Cleanup(func() { require.NoError(t, storage.close()) })

		for _, action := range []model.ActionType{model.ActionLockTable, model.ActionNone} {
			require.NoError(t, storage.handleDDLJob(&model.Job{
				Type: action,
				BinlogInfo: &model.HistoryInfo{
					FinishedTS: 1000 + uint64(action),
				},
			}))
		}
		require.Empty(t, storage.tableTriggerDDLHistory)
		require.Empty(t, storage.tablesDDLHistory)
		require.Empty(t, storage.databaseMap)
		require.Empty(t, storage.tableMap)
	})
}

func assertPhysicalTableIDs(t *testing.T, storage *persistentStorage, ts uint64, expected []int64) {
	t.Helper()
	tables, err := storage.getAllPhysicalTables(ts, nil)
	require.NoError(t, err)
	actual := make([]int64, 0, len(tables))
	for _, table := range tables {
		actual = append(actual, table.TableID)
	}
	require.ElementsMatch(t, expected, actual)
}

func assertTableInfoName(t *testing.T, storage *persistentStorage, tableID int64, ts uint64, expected string) {
	t.Helper()
	info, err := storage.getTableInfo(tableID, ts)
	require.NoError(t, err)
	require.Equal(t, expected, info.GetTableName())
}

func assertTableDeleted(t *testing.T, storage *persistentStorage, tableID int64, ts uint64) {
	t.Helper()
	info, err := storage.getTableInfo(tableID, ts)
	require.Nil(t, info)
	require.IsType(t, &TableDeletedError{}, err)
}
