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
	"fmt"
	"math"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestApplyDDLJobs(t *testing.T) {
	type PhysicalTableQueryTestCase struct {
		snapTs      uint64
		tableFilter filter.Filter
		result      []commonEvent.Table // order doesn't matter
	}
	type FetchTableDDLEventsTestCase struct {
		tableID     int64
		tableFilter filter.Filter
		startTs     uint64
		endTs       uint64
		result      []commonEvent.DDLEvent // Note: not all fields in DDLEvent are compared
	}
	type FetchTableTriggerDDLEventsTestCase struct {
		tableFilter filter.Filter
		startTs     uint64
		limit       int
		result      []commonEvent.DDLEvent // Note: not all fields in DDLEvent are compared
	}
	testCases := []struct {
		initailDBInfos              []mockDBInfo
		ddlJobs                     []*model.Job
		tableMap                    map[int64]*BasicTableInfo
		partitionMap                map[int64]BasicPartitionInfo
		databaseMap                 map[int64]*BasicDatabaseInfo
		tablesDDLHistory            map[int64][]uint64
		tableTriggerDDLHistory      []uint64
		physicalTableQueryTestCases []PhysicalTableQueryTestCase         // test cases for getAllPhysicalTables, nil means not check it
		fetchTableDDLEventsTestCase []FetchTableDDLEventsTestCase        // test cases for fetchTableDDLEvents, nil means not check it
		fetchTableTriggerDDLEvents  []FetchTableTriggerDDLEventsTestCase //	test cases for fetchTableTriggerDDLEvents, nil means not check it
	}{
		// test drop schema can clear table info and partition info
		{
			nil,
			func() []*model.Job {
				return []*model.Job{
					buildCreateSchemaJobForTest(100, "test", 1000),                                    // create schema 100
					buildCreateTableJobForTest(100, 200, "t1", 1010),                                  // create table 200
					buildCreatePartitionTableJobForTest(100, 300, "t1", []int64{301, 302, 303}, 1020), // create partition table 300
					buildDropSchemaJobForTest(100, 1030),                                              // drop schema 100
				}
			}(),
			nil,
			nil,
			nil,
			map[int64][]uint64{
				200: {1010, 1030},
				301: {1020, 1030},
				302: {1020, 1030},
				303: {1020, 1030},
			},
			[]uint64{1000, 1010, 1020, 1030},
			nil,
			nil,
			nil,
		},
		// test create table/drop table/truncate table
		{
			nil,
			func() []*model.Job {
				return []*model.Job{
					buildCreateSchemaJobForTest(100, "test", 1000),          // create schema 100
					buildCreateTableJobForTest(100, 200, "t1", 1010),        // create table 200
					buildCreateTableJobForTest(100, 201, "t2", 1020),        // create table 201
					buildDropTableJobForTest(100, 201, 1030),                // drop table 201
					buildTruncateTableJobForTest(100, 200, 202, "t1", 1040), // truncate table 200 to 202
				}
			}(),
			map[int64]*BasicTableInfo{
				202: {
					SchemaID: 100,
					Name:     "t1",
				},
			},
			nil,
			map[int64]*BasicDatabaseInfo{
				100: {
					Name: "test",
					Tables: map[int64]bool{
						202: true,
					},
				},
			},
			map[int64][]uint64{
				200: {1010, 1040},
				201: {1020, 1030},
				202: {1040},
			},
			[]uint64{1000, 1010, 1020, 1030, 1040},
			[]PhysicalTableQueryTestCase{
				{
					snapTs: 1010,
					result: []commonEvent.Table{
						{
							SchemaID: 100,
							TableID:  200,
							SchemaTableName: &commonEvent.SchemaTableName{
								SchemaName: "test",
								TableName:  "t1",
							},
						},
					},
				},
				{
					snapTs: 1020,
					result: []commonEvent.Table{
						{
							SchemaID: 100,
							TableID:  200,
							SchemaTableName: &commonEvent.SchemaTableName{
								SchemaName: "test",
								TableName:  "t1",
							},
						},
						{
							SchemaID: 100,
							TableID:  201,
							SchemaTableName: &commonEvent.SchemaTableName{
								SchemaName: "test",
								TableName:  "t2",
							},
						},
					},
				},
				{
					snapTs:      1040,
					tableFilter: buildTableFilterByNameForTest("test", "t1"),
					result: []commonEvent.Table{
						{
							SchemaID: 100,
							TableID:  202,
							SchemaTableName: &commonEvent.SchemaTableName{
								SchemaName: "test",
								TableName:  "t1",
							},
						},
					},
				},
				{
					snapTs:      1040,
					tableFilter: buildTableFilterByNameForTest("test", "t2"),
					result:      []commonEvent.Table{},
				},
			},
			nil,
			[]FetchTableTriggerDDLEventsTestCase{
				{
					startTs: 1000,
					limit:   100,
					result: []commonEvent.DDLEvent{
						{
							Type:       byte(model.ActionCreateTable),
							FinishedTs: 1010,
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{0},
							},
							NeedAddedTables: []commonEvent.Table{
								{
									SchemaID: 100,
									TableID:  200,
								},
							},
							TableNameChange: &commonEvent.TableNameChange{
								AddName: []commonEvent.SchemaTableName{
									{
										SchemaName: "test",
										TableName:  "t1",
									},
								},
							},
						},
						{
							Type:       byte(model.ActionCreateTable),
							FinishedTs: 1020,
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{0},
							},
							NeedAddedTables: []commonEvent.Table{
								{
									SchemaID: 100,
									TableID:  201,
								},
							},
							TableNameChange: &commonEvent.TableNameChange{
								AddName: []commonEvent.SchemaTableName{
									{
										SchemaName: "test",
										TableName:  "t2",
									},
								},
							},
						},
						{
							Type:       byte(model.ActionDropTable),
							FinishedTs: 1030,
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{0, 201},
							},
							NeedDroppedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{201},
							},
							TableNameChange: &commonEvent.TableNameChange{
								DropName: []commonEvent.SchemaTableName{
									{
										SchemaName: "test",
										TableName:  "t2",
									},
								},
							},
							Query: "DROP TABLE `test`.`t2`",
						},
						{
							Type:       byte(model.ActionTruncateTable),
							FinishedTs: 1040,
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{0, 200},
							},
							NeedAddedTables: []commonEvent.Table{
								{
									SchemaID: 100,
									TableID:  202,
								},
							},
							NeedDroppedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{200},
							},
						},
					},
				},
			},
		},
		// test partition table related ddl
		{
			nil,
			func() []*model.Job {
				return []*model.Job{
					buildCreateSchemaJobForTest(100, "test", 1000),                                           // create schema 100
					buildCreatePartitionTableJobForTest(100, 200, "t1", []int64{201, 202, 203}, 1010),        // create partition table 200
					buildTruncatePartitionTableJobForTest(100, 200, 300, "t1", []int64{204, 205, 206}, 1020), // truncate partition table 200 to 300
					buildAddPartitionJobForTest(100, 300, "t1", []int64{204, 205, 206, 207}, 1030),           // add partition 207
					buildDropPartitionJobForTest(100, 300, "t1", []int64{205, 206, 207}, 1040),               // drop partition 204
					buildTruncatePartitionJobForTest(100, 300, "t1", []int64{206, 207, 208}, 1050),           // truncate partition 205 to 208
				}
			}(),
			map[int64]*BasicTableInfo{
				300: {
					SchemaID: 100,
					Name:     "t1",
				},
			},
			map[int64]BasicPartitionInfo{
				300: {
					206: nil,
					207: nil,
					208: nil,
				},
			},
			map[int64]*BasicDatabaseInfo{
				100: {
					Name: "test",
					Tables: map[int64]bool{
						300: true,
					},
				},
			},
			map[int64][]uint64{
				201: {1010, 1020},
				202: {1010, 1020},
				203: {1010, 1020},
				204: {1020, 1030, 1040},
				205: {1020, 1030, 1040, 1050},
				206: {1020, 1030, 1040, 1050},
				207: {1030, 1040, 1050},
				208: {1050},
			},
			[]uint64{1000, 1010, 1020, 1030, 1040, 1050},
			[]PhysicalTableQueryTestCase{
				{
					snapTs: 1010,
					result: []commonEvent.Table{
						{
							SchemaID: 100,
							TableID:  201,
							SchemaTableName: &commonEvent.SchemaTableName{
								SchemaName: "test",
								TableName:  "t1",
							},
						},
						{
							SchemaID: 100,
							TableID:  202,
							SchemaTableName: &commonEvent.SchemaTableName{
								SchemaName: "test",
								TableName:  "t1",
							},
						},
						{
							SchemaID: 100,
							TableID:  203,
							SchemaTableName: &commonEvent.SchemaTableName{
								SchemaName: "test",
								TableName:  "t1",
							},
						},
					},
				},
				{
					snapTs: 1050,
					result: []commonEvent.Table{
						{
							SchemaID: 100,
							TableID:  206,
							SchemaTableName: &commonEvent.SchemaTableName{
								SchemaName: "test",
								TableName:  "t1",
							},
						},
						{
							SchemaID: 100,
							TableID:  207,
							SchemaTableName: &commonEvent.SchemaTableName{
								SchemaName: "test",
								TableName:  "t1",
							},
						},
						{
							SchemaID: 100,
							TableID:  208,
							SchemaTableName: &commonEvent.SchemaTableName{
								SchemaName: "test",
								TableName:  "t1",
							},
						},
					},
				},
			},
			[]FetchTableDDLEventsTestCase{
				{
					tableID: 207,
					startTs: 1030,
					endTs:   1050,
					result: []commonEvent.DDLEvent{
						{
							Type:       byte(model.ActionDropTablePartition),
							FinishedTs: 1040,
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{0, 204, 205, 206, 207},
							},
							NeedDroppedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{204},
							},
						},
						{
							Type:       byte(model.ActionTruncateTablePartition),
							FinishedTs: 1050,
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{0, 205, 206, 207},
							},
							NeedDroppedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{205},
							},
							NeedAddedTables: []commonEvent.Table{
								{
									SchemaID: 100,
									TableID:  208,
								},
							},
						},
					},
				},
			},
			[]FetchTableTriggerDDLEventsTestCase{
				{
					startTs: 999,
					limit:   1,
					result: []commonEvent.DDLEvent{
						{
							Type:       byte(model.ActionCreateSchema),
							FinishedTs: 1000,
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{0},
							},
						},
					},
				},
				{
					startTs: 1000,
					limit:   10,
					result: []commonEvent.DDLEvent{
						{
							Type:       byte(model.ActionCreateTable),
							FinishedTs: 1010,
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{0},
							},
							NeedAddedTables: []commonEvent.Table{
								{
									SchemaID: 100,
									TableID:  201,
								},
								{
									SchemaID: 100,
									TableID:  202,
								},
								{
									SchemaID: 100,
									TableID:  203,
								},
							},
							TableNameChange: &commonEvent.TableNameChange{
								AddName: []commonEvent.SchemaTableName{
									{
										SchemaName: "test",
										TableName:  "t1",
									},
								},
							},
						},
						{
							Type:       byte(model.ActionTruncateTable),
							FinishedTs: 1020,
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{0, 201, 202, 203},
							},
							NeedDroppedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{201, 202, 203},
							},
							NeedAddedTables: []commonEvent.Table{
								{
									SchemaID: 100,
									TableID:  204,
								},
								{
									SchemaID: 100,
									TableID:  205,
								},
								{
									SchemaID: 100,
									TableID:  206,
								},
							},
						},
						{
							Type:       byte(model.ActionAddTablePartition),
							FinishedTs: 1030,
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{0, 204, 205, 206},
							},
							NeedAddedTables: []commonEvent.Table{
								{
									SchemaID: 100,
									TableID:  207,
								},
							},
						},
						{
							Type:       byte(model.ActionDropTablePartition),
							FinishedTs: 1040,
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{0, 204, 205, 206, 207},
							},
							NeedDroppedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{204},
							},
						},
						{
							Type:       byte(model.ActionTruncateTablePartition),
							FinishedTs: 1050,
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{0, 205, 206, 207},
							},
							NeedDroppedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{205},
							},
							NeedAddedTables: []commonEvent.Table{
								{
									SchemaID: 100,
									TableID:  208,
								},
							},
						},
					},
				},
			},
		},
		// test exchange partition
		{
			[]mockDBInfo{
				{
					dbInfo: &model.DBInfo{
						ID:   100,
						Name: pmodel.NewCIStr("test"),
					},
				},
				{
					dbInfo: &model.DBInfo{
						ID:   105,
						Name: pmodel.NewCIStr("test2"),
					},
				},
			},
			func() []*model.Job {
				return []*model.Job{
					buildCreatePartitionTableJobForTest(100, 200, "t1", []int64{201, 202, 203}, 1010),   // create partition table 200
					buildCreateTableJobForTest(105, 300, "t2", 1020),                                    // create table 300
					buildExchangePartitionJobForTest(105, 300, 200, "t1", []int64{201, 202, 300}, 1030), // exchange partition 203 with table 300
				}
			}(),
			map[int64]*BasicTableInfo{
				200: {
					SchemaID: 100,
					Name:     "t1",
				},
				203: {
					SchemaID: 105,
					Name:     "t2",
				},
			},
			map[int64]BasicPartitionInfo{
				200: {
					201: nil,
					202: nil,
					300: nil,
				},
			},
			map[int64]*BasicDatabaseInfo{
				100: {
					Name: "test",
					Tables: map[int64]bool{
						200: true,
					},
				},
				105: {
					Name: "test2",
					Tables: map[int64]bool{
						203: true,
					},
				},
			},
			map[int64][]uint64{
				300: {1020, 1030},
				201: {1010},
				202: {1010},
				203: {1010, 1030},
			},
			[]uint64{1010, 1020, 1030},
			nil,
			[]FetchTableDDLEventsTestCase{
				{
					tableID: 203,
					startTs: 1010,
					endTs:   1030,
					result: []commonEvent.DDLEvent{
						{
							Type:       byte(model.ActionExchangeTablePartition),
							FinishedTs: 1030,
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{203, 300, 0},
							},
							UpdatedSchemas: []commonEvent.SchemaIDChange{
								{
									TableID:     300,
									OldSchemaID: 105,
									NewSchemaID: 100,
								},
								{
									TableID:     203,
									OldSchemaID: 100,
									NewSchemaID: 105,
								},
							},
						},
					},
				},
				// normal table is filtered out
				{
					tableID:     203,
					tableFilter: buildTableFilterByNameForTest("test", "t1"),
					startTs:     1010,
					endTs:       1030,
					result: []commonEvent.DDLEvent{
						{
							Type:       byte(model.ActionExchangeTablePartition),
							FinishedTs: 1030,
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{203, 0},
							},
							NeedDroppedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{203},
							},
							NeedAddedTables: []commonEvent.Table{
								{
									SchemaID: 100,
									TableID:  300,
								},
							},
						},
					},
				},
				// partition table is filtered out
				{
					tableID:     300,
					tableFilter: buildTableFilterByNameForTest("test2", "t2"),
					startTs:     1020,
					endTs:       1030,
					result: []commonEvent.DDLEvent{
						{
							Type:       byte(model.ActionExchangeTablePartition),
							FinishedTs: 1030,
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{300, 0},
							},
							NeedDroppedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{300},
							},
							NeedAddedTables: []commonEvent.Table{
								{
									SchemaID: 105,
									TableID:  203,
								},
							},
						},
					},
				},
			},
			nil,
		},
		// test rename table
		{
			[]mockDBInfo{
				{
					dbInfo: &model.DBInfo{
						ID:   100,
						Name: pmodel.NewCIStr("test"),
					},
				},
				{
					dbInfo: &model.DBInfo{
						ID:   105,
						Name: pmodel.NewCIStr("test2"),
					},
				},
			},
			func() []*model.Job {
				return []*model.Job{
					buildCreateTableJobForTest(100, 300, "t1", 1010), // create table 300
					buildRenameTableJobForTest(105, 300, "t2", 1020, &model.InvolvingSchemaInfo{
						Database: "test",
						Table:    "t1",
					}), // rename table 300 to schema 105
					buildRenameTableJobForTest(105, 300, "t3", 1030, &model.InvolvingSchemaInfo{
						Database: "test2",
						Table:    "t2",
					}), // rename table 300 in the same schema
					// rename table 300 to schema 105 with the same name again
					// check comments in buildPersistedDDLEventForRenameTable to see why this would happen
					buildRenameTableJobForTest(105, 300, "t3", 1040, nil),
				}
			}(),
			map[int64]*BasicTableInfo{
				300: {
					SchemaID: 105,
					Name:     "t3",
				},
			},
			nil,
			map[int64]*BasicDatabaseInfo{
				100: {
					Name:   "test",
					Tables: map[int64]bool{},
				},
				105: {
					Name: "test2",
					Tables: map[int64]bool{
						300: true,
					},
				},
			},
			map[int64][]uint64{
				300: {1010, 1020, 1030, 1040},
			},
			[]uint64{1010, 1020, 1030, 1040},
			nil,
			[]FetchTableDDLEventsTestCase{
				{
					tableID: 300,
					startTs: 1010,
					endTs:   1030,
					result: []commonEvent.DDLEvent{
						{
							Type:       byte(model.ActionRenameTable),
							Query:      "RENAME TABLE `test`.`t1` TO `test2`.`t2`",
							FinishedTs: 1020,
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{0, 300},
							},
							UpdatedSchemas: []commonEvent.SchemaIDChange{
								{
									TableID:     300,
									OldSchemaID: 100,
									NewSchemaID: 105,
								},
							},
							TableNameChange: &commonEvent.TableNameChange{
								AddName: []commonEvent.SchemaTableName{
									{
										SchemaName: "test2",
										TableName:  "t2",
									},
								},
								DropName: []commonEvent.SchemaTableName{
									{
										SchemaName: "test",
										TableName:  "t1",
									},
								},
							},
						},
						{
							Type:       byte(model.ActionRenameTable),
							Query:      "RENAME TABLE `test2`.`t2` TO `test2`.`t3`",
							FinishedTs: 1030,
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{0, 300},
							},
							TableNameChange: &commonEvent.TableNameChange{
								AddName: []commonEvent.SchemaTableName{
									{
										SchemaName: "test2",
										TableName:  "t3",
									},
								},
								DropName: []commonEvent.SchemaTableName{
									{
										SchemaName: "test2",
										TableName:  "t2",
									},
								},
							},
						},
					},
				},
				// test filter: after rename, the table is filtered out
				{
					tableID:     300,
					tableFilter: buildTableFilterByNameForTest("test", "*"),
					startTs:     1010,
					endTs:       1020,
					result: []commonEvent.DDLEvent{
						{
							Type:       byte(model.ActionRenameTable),
							FinishedTs: 1020,
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{0, 300},
							},
							NeedDroppedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{300},
							},
							TableNameChange: &commonEvent.TableNameChange{
								DropName: []commonEvent.SchemaTableName{
									{
										SchemaName: "test",
										TableName:  "t1",
									},
								},
							},
						},
					},
				},
			},
			[]FetchTableTriggerDDLEventsTestCase{
				// test filter: before rename, the table is filtered out, so only table trigger can get the event
				{
					tableFilter: buildTableFilterByNameForTest("test2", "*"),
					startTs:     1010,
					limit:       1,
					result: []commonEvent.DDLEvent{
						{
							Type:       byte(model.ActionRenameTable),
							FinishedTs: 1020,
							// This is an error event, so other fields are not set
							// TODO: check error
						},
					},
				},
				// test filter: the table is always filtered out
				{
					tableFilter: buildTableFilterByNameForTest("test3", "*"),
					startTs:     1010,
					limit:       10,
					result:      nil,
				},
			},
		},
		// test rename partition table
		{
			[]mockDBInfo{
				{
					dbInfo: &model.DBInfo{
						ID:   100,
						Name: pmodel.NewCIStr("test"),
					},
				},
				{
					dbInfo: &model.DBInfo{
						ID:   105,
						Name: pmodel.NewCIStr("test2"),
					},
				},
			},
			func() []*model.Job {
				return []*model.Job{
					buildCreatePartitionTableJobForTest(100, 300, "t1", []int64{301, 302, 303}, 1010), // create table 300
					buildRenamePartitionTableJobForTest(105, 300, "t2", []int64{301, 302, 303}, 1020), // rename table 300 to schema 105
				}
			}(),
			map[int64]*BasicTableInfo{
				300: {
					SchemaID: 105,
					Name:     "t2",
				},
			},
			map[int64]BasicPartitionInfo{
				300: {
					301: nil,
					302: nil,
					303: nil,
				},
			},
			map[int64]*BasicDatabaseInfo{
				100: {
					Name:   "test",
					Tables: map[int64]bool{},
				},
				105: {
					Name: "test2",
					Tables: map[int64]bool{
						300: true,
					},
				},
			},
			map[int64][]uint64{
				301: {1010, 1020},
				302: {1010, 1020},
				303: {1010, 1020},
			},
			[]uint64{1010, 1020},
			nil,
			[]FetchTableDDLEventsTestCase{
				{
					tableID: 301,
					startTs: 1010,
					endTs:   1020,
					result: []commonEvent.DDLEvent{
						{
							Type:       byte(model.ActionRenameTable),
							FinishedTs: 1020,
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{0, 301, 302, 303},
							},
							UpdatedSchemas: []commonEvent.SchemaIDChange{
								{
									TableID:     301,
									OldSchemaID: 100,
									NewSchemaID: 105,
								},
								{
									TableID:     302,
									OldSchemaID: 100,
									NewSchemaID: 105,
								},
								{
									TableID:     303,
									OldSchemaID: 100,
									NewSchemaID: 105,
								},
							},
							TableNameChange: &commonEvent.TableNameChange{
								AddName: []commonEvent.SchemaTableName{
									{
										SchemaName: "test2",
										TableName:  "t2",
									},
								},
								DropName: []commonEvent.SchemaTableName{
									{
										SchemaName: "test",
										TableName:  "t1",
									},
								},
							},
						},
					},
				},
				// test filter: after rename, the table is filtered out
				{
					tableID:     301,
					tableFilter: buildTableFilterByNameForTest("test", "*"),
					startTs:     1010,
					endTs:       1020,
					result: []commonEvent.DDLEvent{
						{
							Type:       byte(model.ActionRenameTable),
							FinishedTs: 1020,
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{0, 301, 302, 303},
							},
							NeedDroppedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{301, 302, 303},
							},
							TableNameChange: &commonEvent.TableNameChange{
								DropName: []commonEvent.SchemaTableName{
									{
										SchemaName: "test",
										TableName:  "t1",
									},
								},
							},
						},
					},
				},
			},
			[]FetchTableTriggerDDLEventsTestCase{
				// test filter: before rename, the table is filtered out, so only table trigger can get the event
				{
					tableFilter: buildTableFilterByNameForTest("test2", "*"),
					startTs:     1010,
					limit:       10,
					result: []commonEvent.DDLEvent{
						{
							Type:       byte(model.ActionRenameTable),
							FinishedTs: 1020,
							// TODO: check error
						},
					},
				},
			},
		},
		// test rename tables
		{
			[]mockDBInfo{
				{
					dbInfo: &model.DBInfo{
						ID:   100,
						Name: pmodel.NewCIStr("test"),
					},
					tables: []*model.TableInfo{
						{
							ID:   200,
							Name: pmodel.NewCIStr("t1"),
						},
						{
							ID:   201,
							Name: pmodel.NewCIStr("t2"),
						},
					},
				},
				{
					dbInfo: &model.DBInfo{
						ID:   105,
						Name: pmodel.NewCIStr("test2"),
					},
				},
			},
			func() []*model.Job {
				return []*model.Job{
					buildRenameTablesJobForTest(
						[]int64{100, 100},
						[]int64{100, 105},
						[]int64{200, 201},
						[]string{"test", "test"},
						[]string{"t1", "t2"},
						[]string{"t101", "t102"},
						1010), // rename table 200, 201
				}
			}(),
			map[int64]*BasicTableInfo{
				200: {
					SchemaID: 100,
					Name:     "t101",
				},
				201: {
					SchemaID: 105,
					Name:     "t102",
				},
			},
			nil,
			map[int64]*BasicDatabaseInfo{
				100: {
					Name: "test",
					Tables: map[int64]bool{
						200: true,
					},
				},
				105: {
					Name: "test2",
					Tables: map[int64]bool{
						201: true,
					},
				},
			},
			map[int64][]uint64{
				200: {1010},
				201: {1010},
			},
			[]uint64{1010},
			nil,
			[]FetchTableDDLEventsTestCase{
				{
					tableID: 200,
					startTs: 1000,
					endTs:   1010,
					result: []commonEvent.DDLEvent{
						{
							Type: byte(model.ActionRenameTables),
							// Query:      "RENAME TABLE `test`.`t1` TO `test`.`t101`;RENAME TABLE `test`.`t2` TO `test2`.`t102`;",
							FinishedTs: 1010,
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{0, 200, 201},
							},
							UpdatedSchemas: []commonEvent.SchemaIDChange{
								{
									TableID:     201,
									OldSchemaID: 100,
									NewSchemaID: 105,
								},
							},
							TableNameChange: &commonEvent.TableNameChange{
								AddName: []commonEvent.SchemaTableName{
									{
										SchemaName: "test",
										TableName:  "t101",
									},
									{
										SchemaName: "test2",
										TableName:  "t102",
									},
								},
								DropName: []commonEvent.SchemaTableName{
									{
										SchemaName: "test",
										TableName:  "t1",
									},
									{
										SchemaName: "test",
										TableName:  "t2",
									},
								},
							},
						},
					},
				},
				// test filter: after rename, t102 is filtered out
				{
					tableID:     200,
					tableFilter: buildTableFilterByNameForTest("test", "*"),
					startTs:     1000,
					endTs:       1010,
					result: []commonEvent.DDLEvent{
						{
							Type:       byte(model.ActionRenameTables),
							Query:      "RENAME TABLE `test`.`t1` TO `test`.`t101`;RENAME TABLE `test`.`t2` TO `test2`.`t102`;",
							FinishedTs: 1010,
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{0, 200, 201},
							},
							NeedDroppedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{201},
							},
							TableNameChange: &commonEvent.TableNameChange{
								AddName: []commonEvent.SchemaTableName{
									{
										SchemaName: "test",
										TableName:  "t101",
									},
								},
								DropName: []commonEvent.SchemaTableName{
									{
										SchemaName: "test",
										TableName:  "t1",
									},
									{
										SchemaName: "test",
										TableName:  "t2",
									},
								},
							},
						},
					},
				},
				// test filter: only test.t1 is qualified and is filtered out after rename
				// {
				// 	tableID:     200,
				// 	tableFilter: buildTableFilterByNameForTest("test", "t1"),
				// 	startTs:     1000,
				// 	endTs:       1010,
				// 	result: []commonEvent.DDLEvent{
				// 		{
				// 			Type:       byte(model.ActionRenameTables),
				// 			Query:      "RENAME TABLE `test`.`t1` TO `test`.`t101`;",
				// 			FinishedTs: 1010,
				// 			BlockedTables: &commonEvent.InfluencedTables{
				// 				InfluenceType: commonEvent.InfluenceTypeNormal,
				// 				TableIDs:      []int64{0, 200},
				// 			},
				// 			NeedDroppedTables: &commonEvent.InfluencedTables{
				// 				InfluenceType: commonEvent.InfluenceTypeNormal,
				// 				TableIDs:      []int64{200},
				// 			},
				// 			TableNameChange: &commonEvent.TableNameChange{
				// 				DropName: []commonEvent.SchemaTableName{
				// 					{
				// 						SchemaName: "test",
				// 						TableName:  "t1",
				// 					},
				// 				},
				// 			},
				// 		},
				// 	},
				// },
			},
			nil,
		},
		// test create tables
		{
			[]mockDBInfo{
				{
					dbInfo: &model.DBInfo{
						ID:   100,
						Name: pmodel.NewCIStr("test"),
					},
				},
			},
			func() []*model.Job {
				return []*model.Job{
					buildCreateTablesJobForTest(100, []int64{301, 302, 303}, []string{"t1", "t2", "t3"}, 1010), // create table 301, 302, 303
					buildCreateTablesJobWithQueryForTest(
						100,
						[]int64{304, 305},
						[]string{"t4", "t5"},
						[]string{"CREATE TABLE t4 (COL1 VARBINARY(10) NOT NULL, PRIMARY KEY(COL1));", "CREATE TABLE t5 (COL2 ENUM('ABC','IRG','KT;J'), COL3 TINYINT(50) NOT NULL, PRIMARY KEY(COL3));"},
						1020), // create table 304, 305, 306 with query
				}
			}(),
			map[int64]*BasicTableInfo{
				301: {
					SchemaID: 100,
					Name:     "t1",
				},
				302: {
					SchemaID: 100,
					Name:     "t2",
				},
				303: {
					SchemaID: 100,
					Name:     "t3",
				},
				304: {
					SchemaID: 100,
					Name:     "t4",
				},
				305: {
					SchemaID: 100,
					Name:     "t5",
				},
			},
			nil,
			map[int64]*BasicDatabaseInfo{
				100: {
					Name: "test",
					Tables: map[int64]bool{
						301: true,
						302: true,
						303: true,
						304: true,
						305: true,
					},
				},
			},
			map[int64][]uint64{
				301: {1010},
				302: {1010},
				303: {1010},
				304: {1020},
				305: {1020},
			},
			[]uint64{1010, 1020},
			nil,
			nil,
			[]FetchTableTriggerDDLEventsTestCase{
				{
					startTs: 1000,
					limit:   10,
					result: []commonEvent.DDLEvent{
						{
							Type:       byte(model.ActionCreateTables),
							FinishedTs: 1010,
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{0},
							},
							NeedAddedTables: []commonEvent.Table{
								{
									SchemaID: 100,
									TableID:  301,
								},
								{
									SchemaID: 100,
									TableID:  302,
								},
								{
									SchemaID: 100,
									TableID:  303,
								},
							},
							TableNameChange: &commonEvent.TableNameChange{
								AddName: []commonEvent.SchemaTableName{
									{
										SchemaName: "test",
										TableName:  "t1",
									},
									{
										SchemaName: "test",
										TableName:  "t2",
									},
									{
										SchemaName: "test",
										TableName:  "t3",
									},
								},
							},
						},
						{
							Type:       byte(model.ActionCreateTables),
							FinishedTs: 1020,
							Query:      "CREATE TABLE `t4` (`COL1` VARBINARY(10) NOT NULL,PRIMARY KEY(`COL1`));CREATE TABLE `t5` (`COL2` ENUM('ABC','IRG','KT;J'),`COL3` TINYINT(50) NOT NULL,PRIMARY KEY(`COL3`));",
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{0},
							},
							NeedAddedTables: []commonEvent.Table{
								{
									SchemaID: 100,
									TableID:  304,
								},
								{
									SchemaID: 100,
									TableID:  305,
								},
							},
							TableNameChange: &commonEvent.TableNameChange{
								AddName: []commonEvent.SchemaTableName{
									{
										SchemaName: "test",
										TableName:  "t4",
									},
									{
										SchemaName: "test",
										TableName:  "t5",
									},
								},
							},
						},
					},
				},
				// filter t2 and t3
				{
					tableFilter: buildTableFilterByNameForTest("test", "t1"),
					startTs:     1000,
					limit:       1,
					result: []commonEvent.DDLEvent{
						{
							Type:       byte(model.ActionCreateTables),
							FinishedTs: 1010,
							Query:      "CREATE TABLE `t1` (`a` INT PRIMARY KEY);",
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{0},
							},
							NeedAddedTables: []commonEvent.Table{
								{
									SchemaID: 100,
									TableID:  301,
								},
							},
							TableNameChange: &commonEvent.TableNameChange{
								AddName: []commonEvent.SchemaTableName{
									{
										SchemaName: "test",
										TableName:  "t1",
									},
								},
							},
						},
					},
				},
			},
		},
		// test create tables for partition table
		{
			[]mockDBInfo{
				{
					dbInfo: &model.DBInfo{
						ID:   100,
						Name: pmodel.NewCIStr("test"),
					},
				},
			},
			func() []*model.Job {
				return []*model.Job{
					buildCreatePartitionTablesJobForTest(100,
						[]int64{300, 400, 500},
						[]string{"t1", "t2", "t3"},
						[][]int64{{301, 302, 303}, {401, 402, 403}, {501, 502, 503}},
						1010), // create table 301, 302, 303
				}
			}(),
			map[int64]*BasicTableInfo{
				300: {
					SchemaID: 100,
					Name:     "t1",
				},
				400: {
					SchemaID: 100,
					Name:     "t2",
				},
				500: {
					SchemaID: 100,
					Name:     "t3",
				},
			},
			map[int64]BasicPartitionInfo{
				300: {
					301: nil,
					302: nil,
					303: nil,
				},
				400: {
					401: nil,
					402: nil,
					403: nil,
				},
				500: {
					501: nil,
					502: nil,
					503: nil,
				},
			},
			map[int64]*BasicDatabaseInfo{
				100: {
					Name: "test",
					Tables: map[int64]bool{
						300: true,
						400: true,
						500: true,
					},
				},
			},
			map[int64][]uint64{
				301: {1010},
				302: {1010},
				303: {1010},
				401: {1010},
				402: {1010},
				403: {1010},
				501: {1010},
				502: {1010},
				503: {1010},
			},
			[]uint64{1010},
			nil,
			nil,
			[]FetchTableTriggerDDLEventsTestCase{
				{
					startTs: 1000,
					limit:   10,
					result: []commonEvent.DDLEvent{
						{
							Type:       byte(model.ActionCreateTables),
							FinishedTs: 1010,
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{0},
							},
							NeedAddedTables: []commonEvent.Table{
								{
									SchemaID: 100,
									TableID:  301,
								},
								{
									SchemaID: 100,
									TableID:  302,
								},
								{
									SchemaID: 100,
									TableID:  303,
								},
								{
									SchemaID: 100,
									TableID:  401,
								},
								{
									SchemaID: 100,
									TableID:  402,
								},
								{
									SchemaID: 100,
									TableID:  403,
								},
								{
									SchemaID: 100,
									TableID:  501,
								},
								{
									SchemaID: 100,
									TableID:  502,
								},
								{
									SchemaID: 100,
									TableID:  503,
								},
							},
							TableNameChange: &commonEvent.TableNameChange{
								AddName: []commonEvent.SchemaTableName{
									{
										SchemaName: "test",
										TableName:  "t1",
									},
									{
										SchemaName: "test",
										TableName:  "t2",
									},
									{
										SchemaName: "test",
										TableName:  "t3",
									},
								},
							},
						},
					},
				},
				// filter t2 and t3
				{
					tableFilter: buildTableFilterByNameForTest("test", "t1"),
					startTs:     1000,
					limit:       10,
					result: []commonEvent.DDLEvent{
						{
							Type:       byte(model.ActionCreateTables),
							FinishedTs: 1010,
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{0},
							},
							NeedAddedTables: []commonEvent.Table{
								{
									SchemaID: 100,
									TableID:  301,
								},
								{
									SchemaID: 100,
									TableID:  302,
								},
								{
									SchemaID: 100,
									TableID:  303,
								},
							},
							TableNameChange: &commonEvent.TableNameChange{
								AddName: []commonEvent.SchemaTableName{
									{
										SchemaName: "test",
										TableName:  "t1",
									},
								},
							},
						},
					},
				},
			},
		},
		// test alter/remove partitioning
		{
			[]mockDBInfo{
				{
					dbInfo: &model.DBInfo{
						ID:   100,
						Name: pmodel.NewCIStr("test"),
					},
					tables: []*model.TableInfo{
						{
							ID:   300,
							Name: pmodel.NewCIStr("t1"),
						},
					},
				},
			},
			func() []*model.Job {
				return []*model.Job{
					buildAlterTablePartitioningJobForTest(100, 300, 301, []int64{501, 502, 503}, "t1", 1010), // alter table 300 partition
					buildAlterTablePartitioningJobForTest(100, 301, 302, []int64{504, 505, 506}, "t1", 1020), // alter table 301 partition
					buildRemovePartitioningJobForTest(100, 302, 303, "t1", 1030),                             // remove partition
				}
			}(),
			map[int64]*BasicTableInfo{
				303: {
					SchemaID: 100,
					Name:     "t1",
				},
			},
			nil,
			map[int64]*BasicDatabaseInfo{
				100: {
					Name: "test",
					Tables: map[int64]bool{
						303: true,
					},
				},
			},
			map[int64][]uint64{
				300: {1010},
				303: {1030},
				501: {1010, 1020},
				502: {1010, 1020},
				503: {1010, 1020},
				504: {1020, 1030},
				505: {1020, 1030},
				506: {1020, 1030},
			},
			[]uint64{1010, 1020, 1030},
			nil,
			nil,
			[]FetchTableTriggerDDLEventsTestCase{
				{
					startTs: 1000,
					limit:   10,
					result: []commonEvent.DDLEvent{
						{
							Type:       byte(model.ActionAlterTablePartitioning),
							FinishedTs: 1010,
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{0, 300},
							},
							NeedDroppedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{300},
							},
							NeedAddedTables: []commonEvent.Table{
								{
									SchemaID: 100,
									TableID:  501,
								},
								{
									SchemaID: 100,
									TableID:  502,
								},
								{
									SchemaID: 100,
									TableID:  503,
								},
							},
						},
						{
							Type:       byte(model.ActionAlterTablePartitioning),
							FinishedTs: 1020,
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{0, 501, 502, 503},
							},
							NeedDroppedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{501, 502, 503},
							},
							NeedAddedTables: []commonEvent.Table{
								{
									SchemaID: 100,
									TableID:  504,
								},
								{
									SchemaID: 100,
									TableID:  505,
								},
								{
									SchemaID: 100,
									TableID:  506,
								},
							},
						},
						{
							Type:       byte(model.ActionRemovePartitioning),
							FinishedTs: 1030,
							BlockedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{0, 504, 505, 506},
							},
							NeedDroppedTables: &commonEvent.InfluencedTables{
								InfluenceType: commonEvent.InfluenceTypeNormal,
								TableIDs:      []int64{504, 505, 506},
							},
							NeedAddedTables: []commonEvent.Table{
								{
									SchemaID: 100,
									TableID:  303,
								},
							},
						},
					},
				},
			},
		},
		// trivial ddls
		// test add/drop primary key and alter index visibility for table
		// test modify table charset
		// test alter table ttl/remove ttl
		// test set TiFlash replica
		// test multi schema change
		// test add/drop column
		{
			[]mockDBInfo{
				{
					dbInfo: &model.DBInfo{
						ID:   100,
						Name: pmodel.NewCIStr("test"),
					},
					tables: []*model.TableInfo{
						{
							ID:   300,
							Name: pmodel.NewCIStr("t1"),
						},
					},
				},
			},
			func() []*model.Job {
				return []*model.Job{
					buildAddPrimaryKeyJobForTest(100, 300, 1010, &model.IndexInfo{
						ID:        500,
						Name:      pmodel.NewCIStr("idx1"),
						Table:     pmodel.NewCIStr("t1"),
						Primary:   true,
						Invisible: true,
					}),
					buildAlterIndexVisibilityJobForTest(100, 300, 1020, &model.IndexInfo{
						ID:        500,
						Name:      pmodel.NewCIStr("idx1"),
						Table:     pmodel.NewCIStr("t1"),
						Primary:   true,
						Invisible: false,
					}),
					buildDropPrimaryKeyJobForTest(100, 300, 1030),
					buildModifyTableCharsetJobForTest(100, 300, 1040, charset.CharsetUTF8MB4),
					buildAlterTTLJobForTest(100, 300, 1050),
					buildRemoveTTLJobForTest(100, 300, 1060),
					buildMultiSchemaChangeJobForTest(100, 300, 1080),
					buildAddColumnJobForTest(100, 300, 1090),
					buildDropColumnJobForTest(100, 300, 1100),
					buildCreateViewJobForTest(100, 1110),
					buildDropViewJobForTest(100, 1120),
				}
			}(),
			map[int64]*BasicTableInfo{
				300: {
					SchemaID: 100,
					Name:     "t1",
				},
			},
			nil,
			map[int64]*BasicDatabaseInfo{
				100: {
					Name: "test",
					Tables: map[int64]bool{
						300: true,
					},
				},
			},
			map[int64][]uint64{
				300: {1010, 1020, 1030, 1040, 1050, 1060, 1080, 1090, 1100, 1110},
			},
			[]uint64{1110, 1120},
			nil,
			nil,
			nil,
		},
	}

	for index, tt := range testCases {
		dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
		pStorage := newPersistentStorageForTest(dbPath, tt.initailDBInfos)
		checkState := func(fromDisk bool) {
			if (tt.tableMap != nil && !reflect.DeepEqual(tt.tableMap, pStorage.tableMap)) ||
				(tt.tableMap == nil && len(pStorage.tableMap) != 0) {
				log.Warn("tableMap not equal", zap.Int("testIndex", index),
					zap.String("ddlJobs", formatDDLJobsForTest(tt.ddlJobs)), zap.Any("expected", tt.tableMap), zap.Any("actual", pStorage.tableMap), zap.Bool("fromDisk", fromDisk))
				t.Fatalf("tableMap not equal")
			}
			if (tt.partitionMap != nil && !reflect.DeepEqual(tt.partitionMap, pStorage.partitionMap)) ||
				(tt.partitionMap == nil && len(pStorage.partitionMap) != 0) {
				log.Warn("partitionMap not equal", zap.Int("testIndex", index), zap.String("ddlJobs", formatDDLJobsForTest(tt.ddlJobs)), zap.Any("expected", tt.partitionMap), zap.Any("actual", pStorage.partitionMap), zap.Bool("fromDisk", fromDisk))
				t.Fatalf("partitionMap not equal")
			}
			if (tt.databaseMap != nil && !reflect.DeepEqual(tt.databaseMap, pStorage.databaseMap)) ||
				(tt.databaseMap == nil && len(pStorage.databaseMap) != 0) {
				log.Warn("databaseMap not equal", zap.Int("testIndex", index), zap.String("ddlJobs", formatDDLJobsForTest(tt.ddlJobs)), zap.Any("expected", tt.databaseMap), zap.Any("actual", pStorage.databaseMap), zap.Bool("fromDisk", fromDisk))
				t.Fatalf("databaseMap not equal")
			}
			if (tt.tablesDDLHistory != nil && !reflect.DeepEqual(tt.tablesDDLHistory, pStorage.tablesDDLHistory)) ||
				(tt.tablesDDLHistory == nil && len(pStorage.tablesDDLHistory) != 0) {
				log.Warn("tablesDDLHistory not equal", zap.Int("testIndex", index), zap.String("ddlJobs", formatDDLJobsForTest(tt.ddlJobs)), zap.Any("expected", tt.tablesDDLHistory), zap.Any("actual", pStorage.tablesDDLHistory), zap.Bool("fromDisk", fromDisk))
				t.Fatalf("tablesDDLHistory not equal")
			}
			if (tt.tableTriggerDDLHistory != nil && !reflect.DeepEqual(tt.tableTriggerDDLHistory, pStorage.tableTriggerDDLHistory)) ||
				(tt.tableTriggerDDLHistory == nil && len(pStorage.tableTriggerDDLHistory) != 0) {
				log.Warn("tableTriggerDDLHistory not equal", zap.Int("testIndex", index), zap.String("ddlJobs", formatDDLJobsForTest(tt.ddlJobs)), zap.Any("expected", tt.tableTriggerDDLHistory), zap.Any("actual", pStorage.tableTriggerDDLHistory), zap.Bool("fromDisk", fromDisk))
				t.Fatalf("tableTriggerDDLHistory not equal")
			}
			for _, testCase := range tt.physicalTableQueryTestCases {
				allPhysicalTables, err := pStorage.getAllPhysicalTables(testCase.snapTs, testCase.tableFilter)
				require.Nil(t, err)
				sort.Slice(allPhysicalTables, func(i, j int) bool {
					// SchemaID and TableID should be enough?
					if allPhysicalTables[i].SchemaID != allPhysicalTables[j].SchemaID {
						return allPhysicalTables[i].SchemaID < allPhysicalTables[j].SchemaID
					} else {
						return allPhysicalTables[i].TableID < allPhysicalTables[j].TableID
					}
				})
				sort.Slice(testCase.result, func(i, j int) bool {
					// SchemaID and TableID should be enough?
					if testCase.result[i].SchemaID != testCase.result[j].SchemaID {
						return testCase.result[i].SchemaID < testCase.result[j].SchemaID
					} else {
						return testCase.result[i].TableID < testCase.result[j].TableID
					}
				})
				if !reflect.DeepEqual(testCase.result, allPhysicalTables) {
					log.Warn("getAllPhysicalTables result wrong",
						zap.Int("testIndex", index),
						zap.String("ddlJobs", formatDDLJobsForTest(tt.ddlJobs)),
						zap.Uint64("snapTs", testCase.snapTs),
						zap.Any("tableFilter", testCase.tableFilter),
						zap.Any("expected", testCase.result),
						zap.Any("actual", allPhysicalTables),
						zap.Bool("fromDisk", fromDisk))
					t.Fatalf("getAllPhysicalTables result wrong")
				}
			}
			checkDDLEvents := func(expected []commonEvent.DDLEvent, actual []commonEvent.DDLEvent) bool {
				if len(expected) != len(actual) {
					return false
				}
				for i := range expected {
					expectedDDLEvent := expected[i]
					actualDDLEvent := actual[i]
					if expectedDDLEvent.Type != actualDDLEvent.Type || expectedDDLEvent.FinishedTs != actualDDLEvent.FinishedTs {
						return false
					}
					// check query
					if expectedDDLEvent.Query != "" && expectedDDLEvent.Query != actualDDLEvent.Query {
						return false
					}
					// check BlockedTables
					if expectedDDLEvent.BlockedTables == nil && actualDDLEvent.BlockedTables != nil {
						return false
					}
					if expectedDDLEvent.BlockedTables != nil {
						if actualDDLEvent.BlockedTables == nil {
							return false
						}
						sort.Slice(expectedDDLEvent.BlockedTables.TableIDs, func(i, j int) bool {
							return expectedDDLEvent.BlockedTables.TableIDs[i] < expectedDDLEvent.BlockedTables.TableIDs[j]
						})
						sort.Slice(actualDDLEvent.BlockedTables.TableIDs, func(i, j int) bool {
							return actualDDLEvent.BlockedTables.TableIDs[i] < actualDDLEvent.BlockedTables.TableIDs[j]
						})
						if !reflect.DeepEqual(expectedDDLEvent.BlockedTables, actualDDLEvent.BlockedTables) {
							return false
						}
					}
					// check UpdatedSchemas
					sort.Slice(expectedDDLEvent.UpdatedSchemas, func(i, j int) bool {
						// TableID should be unique, so it is enough
						return expectedDDLEvent.UpdatedSchemas[i].TableID < expectedDDLEvent.UpdatedSchemas[j].TableID
					})
					if !reflect.DeepEqual(expectedDDLEvent.UpdatedSchemas, actualDDLEvent.UpdatedSchemas) {
						return false
					}
					// check NeedDroppedTables
					if expectedDDLEvent.NeedDroppedTables == nil && actualDDLEvent.NeedDroppedTables != nil {
						return false
					}
					if expectedDDLEvent.NeedDroppedTables != nil {
						if actualDDLEvent.NeedDroppedTables == nil {
							return false
						}
						sort.Slice(expectedDDLEvent.NeedDroppedTables.TableIDs, func(i, j int) bool {
							return expectedDDLEvent.NeedDroppedTables.TableIDs[i] < expectedDDLEvent.NeedDroppedTables.TableIDs[j]
						})
						sort.Slice(actualDDLEvent.NeedDroppedTables.TableIDs, func(i, j int) bool {
							return actualDDLEvent.NeedDroppedTables.TableIDs[i] < actualDDLEvent.NeedDroppedTables.TableIDs[j]
						})
						if !reflect.DeepEqual(expectedDDLEvent.NeedDroppedTables, actualDDLEvent.NeedDroppedTables) {
							return false
						}
					}
					// check NeedAddedTables
					sort.Slice(expectedDDLEvent.NeedAddedTables, func(i, j int) bool {
						// TableID should be unique, so it is enough
						return expectedDDLEvent.NeedAddedTables[i].TableID < expectedDDLEvent.NeedAddedTables[j].TableID
					})
					if !reflect.DeepEqual(expectedDDLEvent.NeedAddedTables, actualDDLEvent.NeedAddedTables) {
						return false
					}
					// check TableNameChange
					if expectedDDLEvent.TableNameChange == nil && actualDDLEvent.TableNameChange != nil {
						return false
					}
					if expectedDDLEvent.TableNameChange != nil {
						if actualDDLEvent.TableNameChange == nil {
							return false
						}
						sort.Slice(expectedDDLEvent.TableNameChange.AddName, func(i, j int) bool {
							if expectedDDLEvent.TableNameChange.AddName[i].TableName < expectedDDLEvent.TableNameChange.AddName[j].TableName {
								return true
							} else if expectedDDLEvent.TableNameChange.AddName[i].TableName > expectedDDLEvent.TableNameChange.AddName[j].TableName {
								return false
							} else {
								return expectedDDLEvent.TableNameChange.AddName[i].SchemaName < expectedDDLEvent.TableNameChange.AddName[j].SchemaName
							}
						})
						sort.Slice(expectedDDLEvent.TableNameChange.DropName, func(i, j int) bool {
							if expectedDDLEvent.TableNameChange.DropName[i].TableName < expectedDDLEvent.TableNameChange.DropName[j].TableName {
								return true
							} else if expectedDDLEvent.TableNameChange.DropName[i].TableName > expectedDDLEvent.TableNameChange.DropName[j].TableName {
								return false
							} else {
								return expectedDDLEvent.TableNameChange.DropName[i].SchemaName < expectedDDLEvent.TableNameChange.DropName[j].SchemaName
							}
						})
						if !reflect.DeepEqual(expectedDDLEvent.TableNameChange, actualDDLEvent.TableNameChange) {
							return false
						}
					}
				}
				return true
			}
			for _, testCase := range tt.fetchTableDDLEventsTestCase {
				events, err := pStorage.fetchTableDDLEvents(testCase.tableID, testCase.tableFilter, testCase.startTs, testCase.endTs)
				require.Nil(t, err)
				if !checkDDLEvents(testCase.result, events) {
					log.Warn("fetchTableDDLEvents result wrong",
						zap.Int("testIndex", index),
						zap.Int64("tableID", testCase.tableID),
						zap.Any("tableFilter", testCase.tableFilter),
						zap.Uint64("startTs", testCase.startTs),
						zap.Uint64("endTs", testCase.endTs),
						zap.String("ddlJobs", formatDDLJobsForTest(tt.ddlJobs)),
						zap.String("expectedEvents", formatDDLEventsForTest(testCase.result)),
						zap.String("actualEvents", formatDDLEventsForTest(events)))
					t.Fatalf("fetchTableDDLEvents result wrong")
				}
			}
			for _, testCase := range tt.fetchTableTriggerDDLEvents {
				events, err := pStorage.fetchTableTriggerDDLEvents(testCase.tableFilter, testCase.startTs, testCase.limit)
				require.Nil(t, err)
				if !checkDDLEvents(testCase.result, events) {
					log.Warn("fetchTableTriggerDDLEvents result wrong",
						zap.Int("testIndex", index),
						zap.Any("tableFilter", testCase.tableFilter),
						zap.Uint64("startTs", testCase.startTs),
						zap.Int("limit", testCase.limit),
						zap.String("ddlJobs", formatDDLJobsForTest(tt.ddlJobs)),
						zap.String("expectedEvents", formatDDLEventsForTest(testCase.result)),
						zap.String("actualEvents", formatDDLEventsForTest(events)))
					t.Fatalf("fetchTableTriggerDDLEvents result wrong")
				}
			}
		}
		for _, job := range tt.ddlJobs {
			err := pStorage.handleDDLJob(job)
			require.Nil(t, err)
		}
		checkState(false)
		pStorage.close()
		// load from disk and check again
		pStorage = loadPersistentStorageFromPathForTest(dbPath, math.MaxUint64)
		checkState(true)
		pStorage.close()
	}
}

func TestReadWriteMeta(t *testing.T) {
	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
	err := os.RemoveAll(dbPath)
	require.Nil(t, err)
	db, err := pebble.Open(dbPath, &pebble.Options{})
	require.Nil(t, err)
	defer db.Close()

	{
		gcTS := uint64(1000)
		upperBound := UpperBoundMeta{
			FinishedDDLTs: 3000,
			SchemaVersion: 4000,
			ResolvedTs:    1000,
		}

		writeGcTs(db, gcTS)
		writeUpperBoundMeta(db, upperBound)

		gcTSRead, err := readGcTs(db)
		require.Nil(t, err)
		require.Equal(t, gcTS, gcTSRead)

		upperBoundRead, err := readUpperBoundMeta(db)
		require.Nil(t, err)
		require.Equal(t, upperBound, upperBoundRead)
	}

	// update gcTs
	{
		gcTS := uint64(2000)

		writeGcTs(db, gcTS)

		gcTSRead, err := readGcTs(db)
		require.Nil(t, err)
		require.Equal(t, gcTS, gcTSRead)
	}

	// update upperbound
	{
		upperBound := UpperBoundMeta{
			FinishedDDLTs: 5000,
			SchemaVersion: 5000,
			ResolvedTs:    1000,
		}

		writeUpperBoundMeta(db, upperBound)

		upperBoundRead, err := readUpperBoundMeta(db)
		require.Nil(t, err)
		require.Equal(t, upperBound, upperBoundRead)
	}
}

func TestRegisterTable(t *testing.T) {
	type QueryTableInfoTestCase struct {
		tableID int64
		snapTs  uint64
		name    string
	}
	testCases := []struct {
		initailDBInfos []mockDBInfo // tables in initailDBInfos will be registered before apply ddl
		ddlJobs        []*model.Job
		queryCases     []QueryTableInfoTestCase
	}{
		{
			initailDBInfos: []mockDBInfo{
				{
					dbInfo: &model.DBInfo{
						ID:   50,
						Name: pmodel.NewCIStr("test"),
					},
					tables: []*model.TableInfo{
						{
							ID:   99,
							Name: pmodel.NewCIStr("t1"),
						},
					},
				},
			},
			ddlJobs: func() []*model.Job {
				return []*model.Job{
					buildRenameTableJobForTest(50, 99, "t2", 1000, nil),                              // rename table 99 to t2
					buildCreateTableJobForTest(50, 100, "t3", 1010),                                  // create table 100
					buildTruncateTableJobForTest(50, 100, 101, "t3", 1020),                           // truncate table 100 to 101
					buildCreatePartitionTableJobForTest(50, 102, "t4", []int64{201, 202, 203}, 1030), // create partition table 102
				}
			}(),
			queryCases: []QueryTableInfoTestCase{
				{
					tableID: 99,
					snapTs:  990,
					name:    "t1",
				},
				{
					tableID: 99,
					snapTs:  1000,
					name:    "t2",
				},
				{
					tableID: 100,
					snapTs:  1010,
					name:    "t3",
				},
				{
					tableID: 101,
					snapTs:  1020,
					name:    "t3",
				},
				{
					tableID: 201,
					snapTs:  1030,
					name:    "t4",
				},
				{
					tableID: 202,
					snapTs:  1030,
					name:    "t4",
				},
				{
					tableID: 203,
					snapTs:  1030,
					name:    "t4",
				},
			},
		},
	}
	for _, tt := range testCases {
		dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
		pStorage := newPersistentStorageForTest(dbPath, tt.initailDBInfos)
		for _, db := range tt.initailDBInfos {
			for _, table := range db.tables {
				pStorage.registerTable(table.ID, 0) // second arguments is not important
			}
		}
		for _, job := range tt.ddlJobs {
			err := pStorage.handleDDLJob(job)
			require.Nil(t, err)
		}
		for _, testCase := range tt.queryCases {
			pStorage.registerTable(testCase.tableID, 0) // second arguments is not important
			tableInfo, err := pStorage.getTableInfo(testCase.tableID, testCase.snapTs)
			require.Nil(t, err)
			require.Equal(t, testCase.name, tableInfo.TableName.Table)
		}
		pStorage.close()
	}
}

func TestGCPersistStorage(t *testing.T) {
	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
	err := os.RemoveAll(dbPath)
	require.Nil(t, err)

	schemaID := int64(300)
	tableID1 := int64(100)
	tableID2 := int64(200)

	initialDBInfos := []mockDBInfo{
		{
			dbInfo: &model.DBInfo{
				ID:   schemaID,
				Name: pmodel.NewCIStr("test"),
			},
			tables: []*model.TableInfo{
				{
					ID:   tableID1,
					Name: pmodel.NewCIStr("t1"),
				},
				{
					ID:   tableID2,
					Name: pmodel.NewCIStr("t2"),
				},
			},
		},
	}

	pStorage := newPersistentStorageForTest(dbPath, initialDBInfos)

	// create table t3
	tableID3 := int64(500)
	{
		ddlEvent := &model.Job{
			Type:     model.ActionCreateTable,
			SchemaID: schemaID,
			TableID:  tableID3,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 501,
				TableInfo: &model.TableInfo{
					ID:   tableID3,
					Name: pmodel.NewCIStr("t3"),
				},
				FinishedTS: 602,
			},
		}
		pStorage.handleDDLJob(ddlEvent)
	}

	// drop table t2
	{
		ddlEvent := &model.Job{
			Type:     model.ActionDropTable,
			SchemaID: schemaID,
			TableID:  tableID2,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 503,
				TableInfo:     nil,
				FinishedTS:    603,
			},
		}
		pStorage.handleDDLJob(ddlEvent)
	}

	// rename table t1
	{
		ddlEvent := &model.Job{
			Type:     model.ActionRenameTable,
			SchemaID: schemaID,
			TableID:  tableID1,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 505,
				TableInfo: &model.TableInfo{
					ID:   tableID1,
					Name: pmodel.NewCIStr("t1_r"),
				},
				FinishedTS: 605,
			},
		}
		pStorage.handleDDLJob(ddlEvent)
	}

	// write upper bound
	newUpperBound := UpperBoundMeta{
		FinishedDDLTs: 700,
		SchemaVersion: 509,
		ResolvedTs:    705,
	}
	{
		writeUpperBoundMeta(pStorage.db, newUpperBound)
	}

	// mock gc
	newGcTs1 := uint64(601)
	{
		databaseInfo := []mockDBInfo{
			{
				dbInfo: &model.DBInfo{
					ID:   schemaID,
					Name: pmodel.NewCIStr("test"),
				},
				tables: []*model.TableInfo{
					{
						ID:   tableID1,
						Name: pmodel.NewCIStr("t1"),
					},
					{
						ID:   tableID2,
						Name: pmodel.NewCIStr("t2"),
					},
				},
			},
		}
		mockWriteKVSnapOnDisk(pStorage.db, newGcTs1, databaseInfo)

		require.Equal(t, 3, len(pStorage.tableTriggerDDLHistory))
		require.Equal(t, 3, len(pStorage.tablesDDLHistory))
		pStorage.cleanObsoleteDataInMemory(newGcTs1)
		require.Equal(t, 3, len(pStorage.tableTriggerDDLHistory))
		require.Equal(t, 3, len(pStorage.tablesDDLHistory))
	}

	// mock gc again with a register table
	pStorage.registerTable(tableID1, newGcTs1+1)
	newGcTs2 := uint64(603)
	{
		databaseInfo := []mockDBInfo{
			{
				dbInfo: &model.DBInfo{
					ID:   schemaID,
					Name: pmodel.NewCIStr("test"),
				},
				tables: []*model.TableInfo{
					{
						ID:   tableID1,
						Name: pmodel.NewCIStr("t1"),
					},
					{
						ID:   tableID2,
						Name: pmodel.NewCIStr("t3"),
					},
				},
			},
		}
		mockWriteKVSnapOnDisk(pStorage.db, newGcTs2, databaseInfo)

		require.Equal(t, 3, len(pStorage.tableTriggerDDLHistory))
		require.Equal(t, 3, len(pStorage.tablesDDLHistory))
		pStorage.cleanObsoleteDataInMemory(newGcTs2)
		require.Equal(t, 1, len(pStorage.tableTriggerDDLHistory))
		require.Equal(t, uint64(605), pStorage.tableTriggerDDLHistory[0])
		require.Equal(t, 1, len(pStorage.tablesDDLHistory))
		require.Equal(t, 1, len(pStorage.tablesDDLHistory[tableID1]))
		tableInfoT1, err := pStorage.getTableInfo(tableID1, newGcTs2)
		require.Nil(t, err)
		require.Equal(t, "t1", tableInfoT1.TableName.Table)
		tableInfoT1, err = pStorage.getTableInfo(tableID1, 606)
		require.Nil(t, err)
		require.Equal(t, "t1_r", tableInfoT1.TableName.Table)
	}

	pStorage = loadPersistentStorageForTest(pStorage.db, newGcTs2, newUpperBound)
	{
		require.Equal(t, newGcTs2, pStorage.gcTs)
		require.Equal(t, newUpperBound, pStorage.upperBound)
		require.Equal(t, 1, len(pStorage.tableTriggerDDLHistory))
		require.Equal(t, uint64(605), pStorage.tableTriggerDDLHistory[0])
		require.Equal(t, 1, len(pStorage.tablesDDLHistory))
		require.Equal(t, 1, len(pStorage.tablesDDLHistory[tableID1]))
	}

	// TODO: test obsolete data can be removed
}
