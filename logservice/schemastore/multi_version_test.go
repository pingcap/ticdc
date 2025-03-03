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
