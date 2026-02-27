// Copyright 2025 PingCAP, Inc.
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

package event

import (
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestTableSchemaStoreWhenMysqlSink(t *testing.T) {
	schemaInfos := make([]*heartbeatpb.SchemaInfo, 0)
	schemaInfos = append(schemaInfos, &heartbeatpb.SchemaInfo{
		SchemaID: 1,
		Tables: []*heartbeatpb.TableInfo{
			{
				TableID: 1,
			},
			{
				TableID: 2,
			},
		},
	})
	schemaInfos = append(schemaInfos, &heartbeatpb.SchemaInfo{
		SchemaID: 2,
		Tables: []*heartbeatpb.TableInfo{
			{
				TableID: 3,
			},
			{
				TableID: 4,
			},
		},
	})

	tableSchemaStore := NewTableSchemaStore(schemaInfos, common.MysqlSinkType, false)
	tableIds := tableSchemaStore.GetAllTableIds()
	require.Equal(t, 5, len(tableIds))
	tableIds = tableSchemaStore.GetTableIdsByDB(1)
	require.Equal(t, 3, len(tableIds))
	tableIds = tableSchemaStore.GetTableIdsByDB(2)
	require.Equal(t, 3, len(tableIds))

	// add table event
	event1 := &DDLEvent{
		FinishedTs: 3,
		NeedAddedTables: []Table{
			{
				SchemaID: 1,
				TableID:  5,
			},
			{
				SchemaID: 2,
				TableID:  6,
			},
		},
	}

	tableSchemaStore.AddEvent(event1)

	tableIds = tableSchemaStore.GetAllTableIds()
	require.Equal(t, 7, len(tableIds))
	tableIds = tableSchemaStore.GetTableIdsByDB(1)
	require.Equal(t, 4, len(tableIds))
	tableIds = tableSchemaStore.GetTableIdsByDB(2)
	require.Equal(t, 4, len(tableIds))

	// drop databases
	event2 := &DDLEvent{
		FinishedTs: 5,
		NeedDroppedTables: &InfluencedTables{
			InfluenceType: InfluenceTypeDB,
			SchemaID:      1,
		},
	}
	tableSchemaStore.AddEvent(event2)
	tableIds = tableSchemaStore.GetAllTableIds()
	require.Equal(t, 4, len(tableIds))
	tableIds = tableSchemaStore.GetTableIdsByDB(1)
	require.Equal(t, 1, len(tableIds))
	tableIds = tableSchemaStore.GetTableIdsByDB(2)
	require.Equal(t, 4, len(tableIds))

	// rename
	event3 := &DDLEvent{
		FinishedTs: 7,
		UpdatedSchemas: []SchemaIDChange{
			{
				TableID:     6,
				OldSchemaID: 2,
				NewSchemaID: 3,
			},
		},
	}
	tableSchemaStore.AddEvent(event3)
	tableIds = tableSchemaStore.GetAllTableIds()
	require.Equal(t, 4, len(tableIds))
	tableIds = tableSchemaStore.GetTableIdsByDB(2)
	require.Equal(t, 3, len(tableIds))
	tableIds = tableSchemaStore.GetTableIdsByDB(3)
	require.Equal(t, 2, len(tableIds))
}

func TestTableSchemaStoreActiveActiveMetadata(t *testing.T) {
	schemaInfos := []*heartbeatpb.SchemaInfo{
		{
			SchemaID:   1,
			SchemaName: "db1",
			Tables: []*heartbeatpb.TableInfo{
				{TableID: 1, TableName: "t1"},
			},
		},
	}

	store := NewTableSchemaStore(schemaInfos, common.MysqlSinkType, true)
	names := store.GetAllTableNames(0, false)
	require.Equal(t, 1, len(names))
	require.Equal(t, "db1", names[0].SchemaName)
	require.Equal(t, "t1", names[0].TableName)
}

func TestTableSchemaStoreWhenNonMysqlSink(t *testing.T) {
	schemaInfos := make([]*heartbeatpb.SchemaInfo, 0)
	schemaInfos = append(schemaInfos, &heartbeatpb.SchemaInfo{
		SchemaName: "test1",
		Tables: []*heartbeatpb.TableInfo{
			{
				TableName: "table1",
			},
			{
				TableName: "table2",
			},
		},
	})
	schemaInfos = append(schemaInfos, &heartbeatpb.SchemaInfo{
		SchemaName: "test2",
		Tables: []*heartbeatpb.TableInfo{
			{
				TableName: "table3",
			},
			{
				TableName: "table4",
			},
		},
	})

	tableSchemaStore := NewTableSchemaStore(schemaInfos, common.KafkaSinkType, false)
	tableNames := tableSchemaStore.GetAllTableNames(1, true)
	require.Equal(t, 4, len(tableNames))

	// add table event
	event1 := &DDLEvent{
		FinishedTs: 3,
		TableNameChange: &TableNameChange{
			AddName: []SchemaTableName{
				{
					SchemaName: "test1",
					TableName:  "table5",
				},
				{
					SchemaName: "test2",
					TableName:  "table6",
				},
			},
		},
	}

	tableSchemaStore.AddEvent(event1)

	tableNames = tableSchemaStore.GetAllTableNames(2, true)
	require.Equal(t, 4, len(tableNames))
	tableNames = tableSchemaStore.GetAllTableNames(3, true)
	require.Equal(t, 6, len(tableNames))

	// drop databases
	event2 := &DDLEvent{
		FinishedTs: 5,
		TableNameChange: &TableNameChange{
			DropDatabaseName: "test1",
		},
	}
	tableSchemaStore.AddEvent(event2)

	tableNames = tableSchemaStore.GetAllTableNames(5, true)
	require.Equal(t, 4, len(tableNames))

	// rename
	event3 := &DDLEvent{
		FinishedTs: 7,
		TableNameChange: &TableNameChange{
			AddName: []SchemaTableName{
				{
					SchemaName: "test3",
					TableName:  "table7",
				},
			},
			DropName: []SchemaTableName{
				{
					SchemaName: "test2",
					TableName:  "table6",
				},
			},
		},
	}
	tableSchemaStore.AddEvent(event3)
	tableNames = tableSchemaStore.GetAllTableNames(7, true)
	require.Equal(t, 4, len(tableNames))
}

func TestTableSchemaStoreNonMysqlTableIDsBootstrapOnly(t *testing.T) {
	schemaInfos := []*heartbeatpb.SchemaInfo{
		{
			SchemaID:   1,
			SchemaName: "db1",
			Tables: []*heartbeatpb.TableInfo{
				{TableID: 10, TableName: "t1"},
				{TableID: 20, TableName: "t2"},
			},
		},
	}

	store := NewTableSchemaStore(schemaInfos, common.KafkaSinkType, false)
	require.ElementsMatch(t, []int64{10, 20}, store.GetAllNormalTableIds())

	// For non-MySQL sinks, table IDs are used for bootstrap only and do not need
	// to be updated by DDL events after initialization.
	store.AddEvent(&DDLEvent{
		FinishedTs: 100,
		NeedAddedTables: []Table{
			{SchemaID: 1, TableID: 30},
		},
		NeedDroppedTables: &InfluencedTables{
			InfluenceType: InfluenceTypeNormal,
			TableIDs:      []int64{10},
		},
		UpdatedSchemas: []SchemaIDChange{
			{TableID: 20, OldSchemaID: 1, NewSchemaID: 2},
		},
	})

	require.ElementsMatch(t, []int64{10, 20}, store.GetAllNormalTableIds())
}

func TestTableSchemaStoreWhenCloudStorageSink(t *testing.T) {
	schemaInfos := []*heartbeatpb.SchemaInfo{
		{
			SchemaID: 1,
			Tables: []*heartbeatpb.TableInfo{
				{TableID: 10},
				{TableID: 20},
			},
		},
	}

	store := NewTableSchemaStore(schemaInfos, common.CloudStorageSinkType, false)
	require.ElementsMatch(t, []int64{10, 20}, store.GetAllNormalTableIds())

	store.AddEvent(&DDLEvent{
		FinishedTs: 100,
		NeedAddedTables: []Table{
			{SchemaID: 1, TableID: 30},
		},
	})
	require.ElementsMatch(t, []int64{10, 20, 30}, store.GetAllNormalTableIds())

	store.AddEvent(&DDLEvent{
		FinishedTs: 101,
		NeedDroppedTables: &InfluencedTables{
			InfluenceType: InfluenceTypeNormal,
			TableIDs:      []int64{20},
		},
	})
	require.ElementsMatch(t, []int64{10, 30}, store.GetAllNormalTableIds())
}
