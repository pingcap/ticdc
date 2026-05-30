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

package maintainer

import (
	"testing"

	"github.com/pingcap/ticdc/downstreamadapter/routing"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/eventservice"
	"github.com/stretchr/testify/require"
)

func TestRouteConflictDetectorPrecheckReportsConflict(t *testing.T) {
	detector := newTestRouteConflictDetector(t, "target", routing.TablePlaceholder)

	var reportedErr error
	detector.reportError = func(err error) {
		reportedErr = err
	}

	info := routeDDLInfo{
		key:      getEventKey(10, false),
		commitTs: 10,
		addedTables: []*heartbeatpb.Table{
			{SchemaID: 2, TableID: 2},
		},
	}
	ready, err := detector.precheck(info)
	require.Error(t, err)
	require.False(t, ready)
	require.Same(t, err, reportedErr)
	require.Contains(t, err.Error(), "table route conflict")
	require.Contains(t, err.Error(), "source `db1`.`t`")
	require.Contains(t, err.Error(), "source `db2`.`t`")
	require.Contains(t, err.Error(), "target `target`.`t`")

	_, ok := detector.tables[2]
	require.False(t, ok)
}

func TestRouteConflictDetectorApplyUpdatesRegistry(t *testing.T) {
	detector := newTestRouteConflictDetector(t, routing.SchemaPlaceholder+"_target", routing.TablePlaceholder)

	info := routeDDLInfo{
		key:      getEventKey(10, false),
		commitTs: 10,
		addedTables: []*heartbeatpb.Table{
			{SchemaID: 2, TableID: 2},
		},
	}
	ready, err := detector.precheck(info)
	require.NoError(t, err)
	require.True(t, ready)

	_, ok := detector.tables[2]
	require.False(t, ok)

	require.NoError(t, detector.apply(info))

	binding, ok := detector.tables[2]
	require.True(t, ok)
	require.Equal(t, routing.TableKey{Schema: "db2_target", Table: "t"}, binding.binding.Target)
}

func TestRouteConflictDetectorReadsNewTableBeforeDispatcherRegistration(t *testing.T) {
	detector := newTestRouteConflictDetector(t, routing.SchemaPlaceholder+"_target", routing.TablePlaceholder)
	store := testRouteSchemaStore(t, detector)
	store.AppendDDLEvent(3, routeTableInfoDDL(3, "db3", "t"))
	store.RequireRegisteredTablesForGetTableInfo()

	info := routeDDLInfo{
		key:      getEventKey(10, false),
		commitTs: 10,
		addedTables: []*heartbeatpb.Table{
			{SchemaID: 3, TableID: 3},
		},
	}
	ready, err := detector.precheck(info)
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, 1, store.ForceGetTableInfoCount(3))
}

func TestRouteConflictDetectorIgnoresDDLSpanInBlockTables(t *testing.T) {
	detector := newTestRouteConflictDetector(t, routing.SchemaPlaceholder+"_target", routing.TablePlaceholder)

	info := routeDDLInfo{
		key:      getEventKey(10, false),
		commitTs: 10,
		blockTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_Normal,
			TableIDs:      []int64{common.DDLSpanTableID, 1},
		},
		addedTables: []*heartbeatpb.Table{
			{SchemaID: 2, TableID: 2},
		},
	}
	ready, err := detector.precheck(info)
	require.NoError(t, err)
	require.True(t, ready)
	require.NoError(t, detector.apply(info))

	_, ok := detector.tables[2]
	require.True(t, ok)
}

func TestRouteConflictDetectorDropReleasesBootstrapBinding(t *testing.T) {
	detector := newTestRouteConflictDetector(t, "target", routing.TablePlaceholder)

	dropInfo := routeDDLInfo{
		key:      getEventKey(10, false),
		commitTs: 10,
		droppedTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_Normal,
			TableIDs:      []int64{1},
		},
	}
	ready, err := detector.precheck(dropInfo)
	require.NoError(t, err)
	require.True(t, ready)
	require.NoError(t, detector.apply(dropInfo))

	_, ok := detector.tables[1]
	require.False(t, ok)

	addInfo := routeDDLInfo{
		key:      getEventKey(20, false),
		commitTs: 20,
		addedTables: []*heartbeatpb.Table{
			{SchemaID: 2, TableID: 2},
		},
	}
	ready, err = detector.precheck(addInfo)
	require.NoError(t, err)
	require.True(t, ready)
	require.NoError(t, detector.apply(addInfo))
}

func newTestRouteConflictDetector(t *testing.T, targetSchema, targetTable string) *routeConflictDetector {
	t.Helper()

	cfID := common.NewChangeFeedIDWithName("test-changefeed", common.DefaultKeyspaceName)
	store := eventservice.NewMockSchemaStore()
	store.AppendDDLEvent(
		1,
		routeTableInfoDDL(1, "db1", "t"),
	)
	store.AppendDDLEvent(
		2,
		routeTableInfoDDL(2, "db2", "t"),
	)
	appcontext.SetService[schemastore.SchemaStore](appcontext.SchemaStore, store)

	detector, err := newRouteConflictDetector(
		cfID,
		common.KeyspaceMeta{},
		&config.ReplicaConfig{
			Sink: &config.SinkConfig{
				DispatchRules: []*config.DispatchRule{
					{
						Matcher:      []string{"*.*"},
						TargetSchema: targetSchema,
						TargetTable:  targetTable,
					},
				},
			},
		},
		nil,
		[]commonEvent.Table{
			{
				SchemaID: 1,
				TableID:  1,
				SchemaTableName: &commonEvent.SchemaTableName{
					SchemaName: "db1",
					TableName:  "t",
				},
			},
		},
	)
	require.NoError(t, err)
	return detector
}

type routeSchemaStoreMock interface {
	AppendDDLEvent(id common.TableID, ddls ...commonEvent.DDLEvent)
	RequireRegisteredTablesForGetTableInfo()
	ForceGetTableInfoCount(tableID common.TableID) int
}

func testRouteSchemaStore(t *testing.T, detector *routeConflictDetector) routeSchemaStoreMock {
	t.Helper()

	store, ok := detector.schemaStore.(routeSchemaStoreMock)
	require.True(t, ok)
	return store
}

func routeTableInfoDDL(tableID int64, schema, table string) commonEvent.DDLEvent {
	return commonEvent.DDLEvent{
		TableInfo: newTestRouteTableInfo(tableID, schema, table),
	}
}

func newTestRouteTableInfo(tableID int64, schema, table string) *common.TableInfo {
	return &common.TableInfo{
		TableName: common.TableName{
			Schema:  schema,
			Table:   table,
			TableID: tableID,
		},
	}
}
