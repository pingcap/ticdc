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
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestBuildVersionedTableInfoStore(t *testing.T) {
	type QueryTableInfoTestCase struct {
		snapTs     uint64
		deleted    bool
		schemaName string
		tableName  string
	}
	testCases := []struct {
		testName      string
		tableID       int64
		ddlEvents     []*PersistedDDLEvent
		queryCases    []QueryTableInfoTestCase
		deleteVersion uint64
	}{
		{
			testName: "truncate table",
			tableID:  100,
			ddlEvents: func() []*PersistedDDLEvent {
				return []*PersistedDDLEvent{
					buildCreateTableEventForTest(10, 100, "test", "t", 1000),        // create table 100
					buildTruncateTableEventForTest(10, 100, 101, "test", "t", 1010), // truncate table 100 to 101
				}
			}(),
			queryCases: []QueryTableInfoTestCase{
				{
					snapTs:     1000,
					schemaName: "test",
					tableName:  "t",
				},
			},
			deleteVersion: 1010,
		},
		{
			testName: "truncate partition table 1",
			tableID:  301,
			ddlEvents: func() []*PersistedDDLEvent {
				return []*PersistedDDLEvent{
					buildCreatePartitionTableEventForTest(10, 100, "test", "t", []int64{301, 302, 303}, 1000),        // create table 100
					buildTruncatePartitionTableEventForTest(10, 100, 101, "test", "t", []int64{401, 402, 403}, 1010), // truncate partition table 100 to 101
				}
			}(),
			queryCases: []QueryTableInfoTestCase{
				{
					snapTs:     1000,
					schemaName: "test",
					tableName:  "t",
				},
			},
			deleteVersion: 1010,
		},
		{
			testName: "truncate partition table 2",
			tableID:  401,
			ddlEvents: func() []*PersistedDDLEvent {
				return []*PersistedDDLEvent{
					buildTruncatePartitionTableEventForTest(10, 100, 101, "test", "t", []int64{401, 402, 403}, 1010), // truncate partition table 100 to 101
				}
			}(),
			queryCases: []QueryTableInfoTestCase{
				{
					snapTs:     1010,
					schemaName: "test",
					tableName:  "t",
				},
			},
		},
		{
			testName: "drop partition table",
			tableID:  301,
			ddlEvents: func() []*PersistedDDLEvent {
				return []*PersistedDDLEvent{
					buildCreatePartitionTableEventForTest(10, 100, "test", "t", []int64{301, 302, 303}, 1000), // create table 100
					buildDropPartitionTableEventForTest(10, 100, "test", "t", []int64{301, 302, 303}, 1010),   // drop table 100
				}
			}(),
			queryCases: []QueryTableInfoTestCase{
				{
					snapTs:     1000,
					schemaName: "test",
					tableName:  "t",
				},
			},
		},
		{
			testName: "rename table",
			tableID:  101,
			ddlEvents: func() []*PersistedDDLEvent {
				return []*PersistedDDLEvent{
					buildTruncateTableEventForTest(10, 100, 101, "test", "t", 1010),            // truncate table 100 to 101
					buildRenameTableEventForTest(10, 10, 101, "test", "t", "test", "t2", 1020), // rename table 101
				}
			}(),
			queryCases: []QueryTableInfoTestCase{
				{
					snapTs:     1010,
					schemaName: "test",
					tableName:  "t",
				},
				{
					snapTs:     1020,
					schemaName: "test",
					tableName:  "t2",
				},
			},
		},
		// test exchange partition for partition table
		{
			testName: "exchange partition for partition table",
			tableID:  101,
			ddlEvents: func() []*PersistedDDLEvent {
				return []*PersistedDDLEvent{
					buildCreatePartitionTableEventForTest(10, 100, "test", "partition_table", []int64{101, 102, 103}, 1010),                                                             // create partition table 100 with partitions 101, 102, 103
					buildExchangePartitionTableEventForTest(12, 200, 10, 100, "test2", "normal_table", "test", "partition_table", []int64{101, 102, 103}, []int64{200, 102, 103}, 1020), // rename table 101
				}
			}(),
			queryCases: []QueryTableInfoTestCase{
				{
					snapTs:     1010,
					schemaName: "test",
					tableName:  "partition_table",
				},
				{
					snapTs:     1020,
					schemaName: "test2",
					tableName:  "normal_table",
				},
			},
		},
		// test exchange partition for normal table
		{
			testName: "exchange partition for normal table",
			tableID:  200,
			ddlEvents: func() []*PersistedDDLEvent {
				return []*PersistedDDLEvent{
					buildCreateTableEventForTest(10, 200, "test", "normal_table", 1010),                                                                                                 // create table 200
					buildExchangePartitionTableEventForTest(10, 200, 12, 100, "test", "normal_table", "test2", "partition_table", []int64{101, 102, 103}, []int64{200, 102, 103}, 1020), // rename table 101
				}
			}(),
			queryCases: []QueryTableInfoTestCase{
				{
					snapTs:     1010,
					schemaName: "test",
					tableName:  "normal_table",
				},
				{
					snapTs:     1020,
					schemaName: "test2",
					tableName:  "partition_table",
				},
			},
		},
		// test recover table
		{
			testName: "recover table",
			tableID:  200,
			ddlEvents: func() []*PersistedDDLEvent {
				return []*PersistedDDLEvent{
					buildCreateTableEventForTest(10, 200, "test", "normal_table", 1010),  // create table 200
					buildDropTableEventForTest(10, 200, "test", "normal_table", 1020),    // drop table 200
					buildRecoverTableEventForTest(10, 200, "test", "normal_table", 1030), // recover table 200
					buildDropTableEventForTest(10, 200, "test", "normal_table", 1040),    // drop table 200
				}
			}(),
			queryCases: []QueryTableInfoTestCase{
				{
					snapTs:     1010,
					schemaName: "test",
					tableName:  "normal_table",
				},
				// Note: In 1020, the table is dropped, but this information is overridden by a subsequent table recovery.
				// Since storing this information is meaningless, we retain the current behavior.
				{
					snapTs:     1030,
					schemaName: "test",
					tableName:  "normal_table",
				},
				{
					snapTs:  1040,
					deleted: true,
				},
			},
		},
	}
	for _, tt := range testCases {
		t.Run(tt.testName, func(t *testing.T) {
			store := newEmptyVersionedTableInfoStore(tt.tableID)
			store.setTableInfoInitialized()
			for _, event := range tt.ddlEvents {
				store.applyDDL(event)
			}
			for _, c := range tt.queryCases {
				tableInfo, err := store.getTableInfo(c.snapTs)
				if !c.deleted {
					require.Nil(t, err)
					require.Equal(t, c.schemaName, tableInfo.TableName.Schema)
					require.Equal(t, c.tableName, tableInfo.TableName.Table)
					if !tableInfo.TableName.IsPartition {
						require.Equal(t, tt.tableID, tableInfo.TableName.TableID)
					}
				} else {
					require.Nil(t, tableInfo)
					if _, ok := err.(*TableDeletedError); !ok {
						t.Error("expect TableDeletedError, but got", err)
					}
				}
			}
			if tt.deleteVersion != 0 {
				require.Equal(t, tt.deleteVersion, store.deleteVersion)
			}
		})
	}
}

func TestPartitionDDLUpdatesSurvivingPartitionTableInfo(t *testing.T) {
	const (
		logicalTableID = int64(100)
		survivorID     = int64(201)
		oldUpdateTS    = uint64(1000)
		newUpdateTS    = uint64(2000)
		ddlFinishedTS  = uint64(3000)
	)

	testCases := []struct {
		name                string
		ddlType             model.ActionType
		previousIDs         []int64
		currentIDs          []int64
		expectedAffectedIDs []int64
		expectedUpdateTS    uint64
		droppedID           int64
	}{
		{
			name:                "add partition",
			ddlType:             model.ActionAddTablePartition,
			previousIDs:         []int64{201, 202},
			currentIDs:          []int64{201, 202, 203},
			expectedAffectedIDs: []int64{201, 202, 203},
			expectedUpdateTS:    newUpdateTS,
		},
		{
			name:                "drop partition",
			ddlType:             model.ActionDropTablePartition,
			previousIDs:         []int64{201, 202, 203},
			currentIDs:          []int64{201, 202},
			expectedAffectedIDs: []int64{201, 202, 203},
			expectedUpdateTS:    newUpdateTS,
			droppedID:           203,
		},
		{
			name:                "reorganize partition",
			ddlType:             model.ActionReorganizePartition,
			previousIDs:         []int64{201, 202, 203},
			currentIDs:          []int64{201, 204, 205},
			expectedAffectedIDs: []int64{201, 202, 203, 204, 205},
			expectedUpdateTS:    newUpdateTS,
			droppedID:           202,
		},
		{
			name:                "truncate partition",
			ddlType:             model.ActionTruncateTablePartition,
			previousIDs:         []int64{201, 202, 203},
			currentIDs:          []int64{201, 202, 204},
			expectedAffectedIDs: []int64{201, 202, 203, 204},
			expectedUpdateTS:    oldUpdateTS,
			droppedID:           203,
		},
	}

	newPartitionTableInfo := func(partitionIDs []int64, updateTS uint64) *model.TableInfo {
		tableInfo := newEligibleTableInfoForTest(logicalTableID, "t")
		tableInfo.Partition = buildPartitionDefinitionsForTest(partitionIDs)
		tableInfo.UpdateTS = updateTS
		return tableInfo
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			event := &PersistedDDLEvent{
				Type:           byte(tc.ddlType),
				SchemaID:       10,
				TableID:        logicalTableID,
				SchemaName:     "test",
				TableName:      "t",
				TableInfo:      newPartitionTableInfo(tc.currentIDs, tc.expectedUpdateTS),
				PrevPartitions: tc.previousIDs,
				FinishedTs:     ddlFinishedTS,
			}
			handler := allDDLHandlers[tc.ddlType]

			newStore := func(initialized bool) *versionedTableInfoStore {
				store := newEmptyVersionedTableInfoStore(survivorID)
				store.addInitialTableInfo(
					common.WrapTableInfo("test", newPartitionTableInfo(tc.previousIDs, oldUpdateTS)),
					oldUpdateTS,
				)
				if initialized {
					store.setTableInfoInitialized()
				}
				return store
			}
			assertUpdated := func(store *versionedTableInfoStore) {
				tableInfo, err := store.getTableInfo(ddlFinishedTS)
				require.NoError(t, err)
				require.Equal(t, tc.expectedUpdateTS, tableInfo.GetUpdateTS())
				require.Len(t, store.infos, 2)
				require.Equal(t, ddlFinishedTS, store.infos[1].Version)
			}

			// Verify that the online path applies the DDL to a registered surviving partition.
			liveStore := newStore(true)
			affectedIDs := make([]int64, 0, len(tc.expectedAffectedIDs))
			handler.iterateEventTablesFunc(iterateEventTablesFuncArgs{
				event: event,
				apply: func(tableIDs ...int64) {
					affectedIDs = append(affectedIDs, tableIDs...)
					for _, tableID := range tableIDs {
						if tableID == survivorID {
							liveStore.applyDDL(event)
						}
					}
				},
			})
			require.ElementsMatch(t, tc.expectedAffectedIDs, affectedIDs)
			assertUpdated(liveStore)

			// Verify that the history path records and extracts the same update.
			tablesDDLHistory := make(map[int64][]uint64)
			handler.updateDDLHistoryFunc(updateDDLHistoryFuncArgs{
				ddlEvent:         event,
				tablesDDLHistory: tablesDDLHistory,
			})
			require.Len(t, tablesDDLHistory, len(tc.expectedAffectedIDs))
			for _, tableID := range tc.expectedAffectedIDs {
				require.Equal(t, []uint64{ddlFinishedTS}, tablesDDLHistory[tableID])
			}
			historyStore := newStore(false)
			historyStore.applyDDLFromPersistStorage(event)
			historyStore.setTableInfoInitialized()
			assertUpdated(historyStore)

			for _, tableID := range tc.currentIDs {
				tableInfo, deleted := handler.extractTableInfoFunc(event, tableID)
				require.NotNil(t, tableInfo)
				require.False(t, deleted)
				require.Equal(t, tc.expectedUpdateTS, tableInfo.GetUpdateTS())
			}
			if tc.droppedID != 0 {
				tableInfo, deleted := handler.extractTableInfoFunc(event, tc.droppedID)
				require.Nil(t, tableInfo)
				require.True(t, deleted)
			}
		})
	}
}

func TestGCMultiVersionTableInfo(t *testing.T) {
	tableID := int64(100)
	store := newEmptyVersionedTableInfoStore(tableID)
	store.setTableInfoInitialized()

	store.infos = append(store.infos, &tableInfoItem{Version: 100, Info: &common.TableInfo{}})
	store.infos = append(store.infos, &tableInfoItem{Version: 200, Info: &common.TableInfo{}})
	store.infos = append(store.infos, &tableInfoItem{Version: 300, Info: &common.TableInfo{}})
	store.deleteVersion = 1000

	require.False(t, store.gc(200))
	require.Equal(t, 2, len(store.infos))
	require.False(t, store.gc(300))
	require.Equal(t, 1, len(store.infos))
	require.False(t, store.gc(500))
	require.Equal(t, 1, len(store.infos))
	require.True(t, store.gc(1000))
}
