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

func TestRouteAdminPrecheckReportsConflict(t *testing.T) {
	admin := newTestRouteAdmin(t, "target", routing.TablePlaceholder)

	var reportedErr error
	admin.reportError = func(err error) {
		reportedErr = err
	}

	info := routeAdmissionInfo{
		key:      getEventKey(10, false),
		commitTs: 10,
		addedTables: []*heartbeatpb.Table{
			{SchemaID: 2, TableID: 2},
		},
	}
	ready, err := admin.precheck(info)
	require.Error(t, err)
	require.False(t, ready)
	require.Same(t, err, reportedErr)
	require.Contains(t, err.Error(), "table route conflict")
	require.Contains(t, err.Error(), "source `db1`.`t`")
	require.Contains(t, err.Error(), "source `db2`.`t`")
	require.Contains(t, err.Error(), "target `target`.`t`")

	_, ok := admin.tableSources[2]
	require.False(t, ok)
}

func TestRouteAdminApplyUpdatesRegistry(t *testing.T) {
	admin := newTestRouteAdmin(t, routing.SchemaPlaceholder+"_target", routing.TablePlaceholder)

	info := routeAdmissionInfo{
		key:      getEventKey(10, false),
		commitTs: 10,
		addedTables: []*heartbeatpb.Table{
			{SchemaID: 2, TableID: 2},
		},
	}
	ready, err := admin.precheck(info)
	require.NoError(t, err)
	require.True(t, ready)

	_, ok := admin.tableSources[2]
	require.False(t, ok)

	require.NoError(t, admin.apply(info))

	binding, ok := admin.tableSources[2]
	require.True(t, ok)
	require.Equal(t, routing.TableKey{Schema: "db2_target", Table: "t"}, binding.binding.Target)
}

func TestRouteAdminApplyBuildsRecoveredTransition(t *testing.T) {
	admin := newTestRouteAdmin(t, "target", routing.TablePlaceholder)
	store := testRouteSchemaStore(t, admin)
	store.AppendDDLEvent(1, routeTableInfoDDL(1, "db1", "u"))

	info := routeAdmissionInfo{
		key:      getEventKey(10, false),
		commitTs: 10,
		blockTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_Normal,
			TableIDs:      []int64{1},
		},
	}

	require.Empty(t, admin.pendingEvents)
	require.NoError(t, admin.apply(info))

	binding, ok := admin.tableSources[1]
	require.True(t, ok)
	require.Equal(t, routing.TableKey{Schema: "db1", Table: "u"}, binding.binding.Source)
	require.Equal(t, routing.TableKey{Schema: "target", Table: "u"}, binding.binding.Target)
	require.Empty(t, admin.pendingEvents)
	require.Empty(t, admin.pendingQueue)
}

func TestRouteAdminReadsNewTableBeforeDispatcherRegistration(t *testing.T) {
	admin := newTestRouteAdmin(t, routing.SchemaPlaceholder+"_target", routing.TablePlaceholder)
	store := testRouteSchemaStore(t, admin)
	store.AppendDDLEvent(3, routeTableInfoDDL(3, "db3", "t"))
	store.RequireRegisteredTablesForGetTableInfo()

	info := routeAdmissionInfo{
		key:      getEventKey(10, false),
		commitTs: 10,
		addedTables: []*heartbeatpb.Table{
			{SchemaID: 3, TableID: 3},
		},
	}
	ready, err := admin.precheck(info)
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, 1, store.ForceGetTableInfoCount(3))
}

func TestRouteAdminIgnoresDDLSpanInBlockTables(t *testing.T) {
	admin := newTestRouteAdmin(t, routing.SchemaPlaceholder+"_target", routing.TablePlaceholder)

	info := routeAdmissionInfo{
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
	ready, err := admin.precheck(info)
	require.NoError(t, err)
	require.True(t, ready)
	require.NoError(t, admin.apply(info))

	_, ok := admin.tableSources[2]
	require.True(t, ok)
}

func TestRouteAdminCachesRouteNeutralBlockEvents(t *testing.T) {
	admin := newTestRouteAdmin(t, routing.SchemaPlaceholder+"_target", routing.TablePlaceholder)
	store := testRouteSchemaStore(t, admin)

	// Schema-only DDLs such as ADD COLUMN and RENAME COLUMN still report BlockTables,
	// but they do not change table route admission. Keep repeated handling low-cost.
	info := routeAdmissionInfo{
		key:      getEventKey(10, false),
		commitTs: 10,
		blockTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_Normal,
			TableIDs:      []int64{1},
		},
	}

	ready, err := admin.precheck(info)
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, 1, store.ForceGetTableInfoCount(1))
	require.Len(t, admin.routeNeutralEventCache, 1)
	require.Empty(t, admin.pendingEvents)
	require.Empty(t, admin.pendingQueue)

	ready, err = admin.precheck(info)
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, 1, store.ForceGetTableInfoCount(1))
	require.Len(t, admin.routeNeutralEventCache, 1)

	require.NoError(t, admin.apply(info))
	require.Equal(t, 1, store.ForceGetTableInfoCount(1))
	require.Empty(t, admin.routeNeutralEventCache)
	require.Empty(t, admin.pendingEvents)
	require.Empty(t, admin.pendingQueue)
	require.Equal(t, 1, admin.sourceRefs[routing.TableKey{Schema: "db1", Table: "t"}])
}

func TestRouteAdminDropReleasesBootstrapBinding(t *testing.T) {
	admin := newTestRouteAdmin(t, "target", routing.TablePlaceholder)

	dropInfo := routeAdmissionInfo{
		key:      getEventKey(10, false),
		commitTs: 10,
		droppedTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_Normal,
			TableIDs:      []int64{1},
		},
	}
	ready, err := admin.precheck(dropInfo)
	require.NoError(t, err)
	require.True(t, ready)
	require.NoError(t, admin.apply(dropInfo))

	_, ok := admin.tableSources[1]
	require.False(t, ok)

	addInfo := routeAdmissionInfo{
		key:      getEventKey(20, false),
		commitTs: 20,
		addedTables: []*heartbeatpb.Table{
			{SchemaID: 2, TableID: 2},
		},
	}
	ready, err = admin.precheck(addInfo)
	require.NoError(t, err)
	require.True(t, ready)
	require.NoError(t, admin.apply(addInfo))
}

func TestRouteAdminTracksSourceAdmissionNotTableID(t *testing.T) {
	admin := newTestRouteAdmin(t, "target", routing.TablePlaceholder)
	store := testRouteSchemaStore(t, admin)
	store.AppendDDLEvent(3, routeTableInfoDDL(3, "db1", "t"))
	store.AppendDDLEvent(4, routeTableInfoDDL(4, "db1", "t"))

	source := routing.TableKey{Schema: "db1", Table: "t"}

	addSameSourceInfo := routeAdmissionInfo{
		key:      getEventKey(10, false),
		commitTs: 10,
		addedTables: []*heartbeatpb.Table{
			{SchemaID: 1, TableID: 3},
		},
	}
	ready, err := admin.precheck(addSameSourceInfo)
	require.NoError(t, err)
	require.True(t, ready)
	require.NoError(t, admin.apply(addSameSourceInfo))
	require.Equal(t, 2, admin.sourceRefs[source])

	dropOnePhysicalIDInfo := routeAdmissionInfo{
		key:      getEventKey(20, false),
		commitTs: 20,
		droppedTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_Normal,
			TableIDs:      []int64{1},
		},
	}
	ready, err = admin.precheck(dropOnePhysicalIDInfo)
	require.NoError(t, err)
	require.True(t, ready)
	require.NoError(t, admin.apply(dropOnePhysicalIDInfo))
	require.Equal(t, 1, admin.sourceRefs[source])
	_, ok := admin.tableSources[1]
	require.False(t, ok)
	_, ok = admin.tableSources[3]
	require.True(t, ok)

	truncateInfo := routeAdmissionInfo{
		key:      getEventKey(30, false),
		commitTs: 30,
		droppedTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_Normal,
			TableIDs:      []int64{3},
		},
		addedTables: []*heartbeatpb.Table{
			{SchemaID: 1, TableID: 4},
		},
	}
	ready, err = admin.precheck(truncateInfo)
	require.NoError(t, err)
	require.True(t, ready)
	require.NoError(t, admin.apply(truncateInfo))
	require.Equal(t, 1, admin.sourceRefs[source])
	_, ok = admin.tableSources[3]
	require.False(t, ok)
	_, ok = admin.tableSources[4]
	require.True(t, ok)

	conflictInfo := routeAdmissionInfo{
		key:      getEventKey(40, false),
		commitTs: 40,
		addedTables: []*heartbeatpb.Table{
			{SchemaID: 2, TableID: 2},
		},
	}
	ready, err = admin.precheck(conflictInfo)
	require.Error(t, err)
	require.False(t, ready)
	require.Contains(t, err.Error(), "table route conflict")
}

func newTestRouteAdmin(t *testing.T, targetSchema, targetTable string) *routeAdmin {
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

	admin, err := newRouteAdmin(
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
	return admin
}

type routeSchemaStoreMock interface {
	AppendDDLEvent(id common.TableID, ddls ...commonEvent.DDLEvent)
	RequireRegisteredTablesForGetTableInfo()
	ForceGetTableInfoCount(tableID common.TableID) int
}

func testRouteSchemaStore(t *testing.T, admin *routeAdmin) routeSchemaStoreMock {
	t.Helper()

	store, ok := admin.schemaStore.(routeSchemaStoreMock)
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
