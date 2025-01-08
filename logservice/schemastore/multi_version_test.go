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
		snapTs uint64
		name   string
	}
	testCases := []struct {
		tableID       int64
		ddlEvents     []*PersistedDDLEvent
		queryCases    []QueryTableInfoTestCase
		deleteVersion uint64
	}{
		{
			tableID: 100,
			ddlEvents: func() []*PersistedDDLEvent {
				return []*PersistedDDLEvent{
					buildCreateTableEventForTest(10, 100, "test", "t", 1000),        // create table 100
					buildTruncateTableEventForTest(10, 100, 101, "test", "t", 1010), // truncate table 100 to 101
				}
			}(),
			queryCases: []QueryTableInfoTestCase{
				{
					snapTs: 1000,
					name:   "t",
				},
			},
			deleteVersion: 1010,
		},
		{
			tableID: 101,
			ddlEvents: func() []*PersistedDDLEvent {
				return []*PersistedDDLEvent{
					buildTruncateTableEventForTest(10, 100, 101, "test", "t", 1010),            // truncate table 100 to 101
					buildRenameTableEventForTest(10, 10, 101, "test", "t", "test", "t2", 1020), // rename table 101
				}
			}(),
			queryCases: []QueryTableInfoTestCase{
				{
					snapTs: 1010,
					name:   "t",
				},
				{
					snapTs: 1020,
					name:   "t2",
				},
			},
		},
		// test exchange partition for partition table
		{
			tableID: 101,
			ddlEvents: func() []*PersistedDDLEvent {
				return []*PersistedDDLEvent{
					buildCreatePartitionTableEventForTest(10, 100, "test", "partition_table", []int64{101, 102, 103}, 1010),                                                            // create partition table 100 with partitions 101, 102, 103
					buildExchangePartitionTableEventForTest(10, 200, 10, 100, "test", "normal_table", "test", "partition_table", []int64{101, 102, 103}, []int64{200, 102, 103}, 1020), // rename table 101
				}
			}(),
			queryCases: []QueryTableInfoTestCase{
				{
					snapTs: 1010,
					name:   "partition_table",
				},
				{
					snapTs: 1020,
					name:   "normal_table",
				},
			},
		},
		// test exchange partition for normal table
		{
			tableID: 200,
			ddlEvents: func() []*PersistedDDLEvent {
				return []*PersistedDDLEvent{
					buildCreateTableEventForTest(10, 200, "test", "normal_table", 1010),                                                                                                // create table 200
					buildExchangePartitionTableEventForTest(10, 200, 10, 100, "test", "normal_table", "test", "partition_table", []int64{101, 102, 103}, []int64{200, 102, 103}, 1020), // rename table 101
				}
			}(),
			queryCases: []QueryTableInfoTestCase{
				{
					snapTs: 1010,
					name:   "normal_table",
				},
				{
					snapTs: 1020,
					name:   "partition_table",
				},
			},
		},
	}
	for _, tt := range testCases {
		store := newEmptyVersionedTableInfoStore(tt.tableID)
		store.setTableInfoInitialized()
		for _, event := range tt.ddlEvents {
			store.applyDDL(event)
		}
		for _, c := range tt.queryCases {
			tableInfo, err := store.getTableInfo(c.snapTs)
			require.Nil(t, err)
			require.Equal(t, c.name, tableInfo.TableName.Table)
			if !tableInfo.TableName.IsPartition {
				require.Equal(t, tt.tableID, tableInfo.TableName.TableID)
			}
		}
		if tt.deleteVersion != 0 {
			require.Equal(t, tt.deleteVersion, store.deleteVersion)
		}
	}
}

func TestGCMultiVersionTableInfo(t *testing.T) {
	tableID := int64(100)
	store := newEmptyVersionedTableInfoStore(tableID)
	store.setTableInfoInitialized()

	store.infos = append(store.infos, &tableInfoItem{version: 100, info: &common.TableInfo{}})
	store.infos = append(store.infos, &tableInfoItem{version: 200, info: &common.TableInfo{}})
	store.infos = append(store.infos, &tableInfoItem{version: 300, info: &common.TableInfo{}})
	store.deleteVersion = 1000

	require.False(t, store.gc(200))
	require.Equal(t, 2, len(store.infos))
	require.False(t, store.gc(300))
	require.Equal(t, 1, len(store.infos))
	require.False(t, store.gc(500))
	require.Equal(t, 1, len(store.infos))
	require.True(t, store.gc(1000))
}
