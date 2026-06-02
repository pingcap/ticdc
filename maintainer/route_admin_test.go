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
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestRouteAdminPrecheckReportsConflict(t *testing.T) {
	admin, _ := newTestRouteAdmin(t, routeAllTo("target", "t"))

	var reportedErr error
	admin.reportError = func(err error) {
		reportedErr = err
	}

	info := routeAdmission{
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

func TestRouteAdminAdmitsAddedTables(t *testing.T) {
	admin, tableNames := newTestRouteAdmin(t, routeBySource())

	info := routeAdmission{
		key:      getEventKey(10, false),
		commitTs: 10,
		addedTables: []*heartbeatpb.Table{
			{SchemaID: 2, TableID: 2},
		},
		routeTables: []*heartbeatpb.RouteTableAdmission{
			{
				SchemaID:         2,
				TableID:          2,
				SourceSchemaName: "db2",
				SourceTableName:  "t",
				TargetSchemaName: "db2_target",
				TargetTableName:  "t",
			},
		},
	}
	ready, err := admin.precheck(info)
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, 0, tableNames.count(2))

	_, ok := admin.tableSources[2]
	require.False(t, ok)

	require.NoError(t, admin.apply(info))

	binding, ok := admin.tableSources[2]
	require.True(t, ok)
	require.Equal(t, routing.TableKey{Schema: "db2_target", Table: "t"}, binding.binding.Target)

	tableNames.set(3, "db3", "t")
	info = routeAdmission{
		key:      getEventKey(20, false),
		commitTs: 20,
		addedTables: []*heartbeatpb.Table{
			{SchemaID: 3, TableID: 3},
		},
	}
	ready, err = admin.precheck(info)
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, 1, tableNames.count(3))
	require.NoError(t, admin.apply(info))

	tableNames.set(4, "db4", "t")
	info = routeAdmission{
		key:      getEventKey(30, false),
		commitTs: 30,
		blockTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_Normal,
			TableIDs:      []int64{common.DDLSpanTableID, 1},
		},
		addedTables: []*heartbeatpb.Table{
			{SchemaID: 4, TableID: 4},
		},
	}
	ready, err = admin.precheck(info)
	require.NoError(t, err)
	require.True(t, ready)
	require.NoError(t, admin.apply(info))

	_, ok = admin.tableSources[4]
	require.True(t, ok)
	require.Equal(t, 0, tableNames.count(common.DDLSpanTableID))
}

func TestRouteAdminCachesRouteNeutralBlockEvents(t *testing.T) {
	admin, tableNames := newTestRouteAdmin(t, routeBySource())

	// Schema-only DDLs such as ADD COLUMN and RENAME COLUMN still report BlockTables,
	// but they do not change table route admission. Keep repeated handling low-cost.
	info := routeAdmission{
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
	require.Equal(t, 1, tableNames.count(1))
	require.Len(t, admin.routeNeutralEventCache, 1)
	require.Empty(t, admin.pendingEvents)
	require.Empty(t, admin.pendingQueue)

	ready, err = admin.precheck(info)
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, 1, tableNames.count(1))
	require.Len(t, admin.routeNeutralEventCache, 1)

	require.NoError(t, admin.apply(info))
	require.Equal(t, 1, tableNames.count(1))
	require.Empty(t, admin.routeNeutralEventCache)
	require.Empty(t, admin.pendingEvents)
	require.Empty(t, admin.pendingQueue)
	require.Equal(t, 1, admin.sourceRefs[routing.TableKey{Schema: "db1", Table: "t"}])

	tableNames.set(1, "db1", "u")
	recoveredInfo := routeAdmission{
		key:      getEventKey(20, false),
		commitTs: 20,
		blockTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_Normal,
			TableIDs:      []int64{1},
		},
	}
	require.NoError(t, admin.apply(recoveredInfo))
	binding, ok := admin.tableSources[1]
	require.True(t, ok)
	require.Equal(t, routing.TableKey{Schema: "db1", Table: "u"}, binding.binding.Source)
	require.Equal(t, routing.TableKey{Schema: "db1_target", Table: "u"}, binding.binding.Target)
	require.Empty(t, admin.pendingEvents)
	require.Empty(t, admin.pendingQueue)
}

func TestRouteAdminTracksSourceAdmissionNotTableID(t *testing.T) {
	admin, tableNames := newTestRouteAdmin(t, routeAllTo("target", "t"))
	tableNames.set(3, "db1", "t")
	tableNames.set(4, "db1", "t")

	source := routing.TableKey{Schema: "db1", Table: "t"}

	addSameSourceInfo := routeAdmission{
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

	dropOnePhysicalIDInfo := routeAdmission{
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

	truncateInfo := routeAdmission{
		key:      getEventKey(30, false),
		commitTs: 30,
		blockTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_Normal,
			TableIDs:      []int64{3, 4},
		},
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

	// Replayed partition DDL can still mention the old physical partition ID in
	// BlockTables after its admission state has been replaced.
	ready, err = admin.precheck(truncateInfo)
	require.NoError(t, err)
	require.True(t, ready)
	require.NoError(t, admin.apply(truncateInfo))
	require.Equal(t, 1, admin.sourceRefs[source])

	dropLastSourceInfo := routeAdmission{
		key:      getEventKey(40, false),
		commitTs: 40,
		droppedTables: &heartbeatpb.InfluencedTables{
			InfluenceType: heartbeatpb.InfluenceType_Normal,
			TableIDs:      []int64{4},
		},
	}
	ready, err = admin.precheck(dropLastSourceInfo)
	require.NoError(t, err)
	require.True(t, ready)
	require.NoError(t, admin.apply(dropLastSourceInfo))
	_, ok = admin.sourceRefs[source]
	require.False(t, ok)
	_, ok = admin.tableSources[4]
	require.False(t, ok)

	addOtherSourceInfo := routeAdmission{
		key:      getEventKey(50, false),
		commitTs: 50,
		addedTables: []*heartbeatpb.Table{
			{SchemaID: 2, TableID: 2},
		},
	}
	ready, err = admin.precheck(addOtherSourceInfo)
	require.NoError(t, err)
	require.True(t, ready)
	require.NoError(t, admin.apply(addOtherSourceInfo))
	_, ok = admin.tableSources[2]
	require.True(t, ok)
}

func newTestRouteAdmin(t *testing.T, rules []*config.DispatchRule) (*routeAdmin, *routeTableNames) {
	t.Helper()

	cfID := common.NewChangeFeedIDWithName("test-changefeed", common.DefaultKeyspaceName)
	return newTestRouteAdminWithChangefeed(t, cfID, rules)
}

func newTestRouteAdminWithChangefeed(
	t *testing.T,
	cfID common.ChangeFeedID,
	rules []*config.DispatchRule,
) (*routeAdmin, *routeTableNames) {
	t.Helper()

	tableNames := newRouteTableNames()
	tableNames.set(1, "db1", "t")
	tableNames.set(2, "db2", "t")

	admin, err := newRouteAdmin(
		cfID,
		common.KeyspaceMeta{},
		&config.ReplicaConfig{
			Sink: &config.SinkConfig{
				DispatchRules: rules,
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
		tableNames.get,
	)
	require.NoError(t, err)
	return admin, tableNames
}

type routeTableNames struct {
	names       map[int64]common.TableName
	lookupCount map[int64]int
}

func newRouteTableNames() *routeTableNames {
	return &routeTableNames{
		names:       make(map[int64]common.TableName),
		lookupCount: make(map[int64]int),
	}
}

func (s *routeTableNames) set(tableID int64, schema, table string) {
	s.names[tableID] = common.TableName{
		Schema:  schema,
		Table:   table,
		TableID: tableID,
	}
}

func (s *routeTableNames) get(
	_ common.KeyspaceMeta,
	tableID int64,
	_ uint64,
) (common.TableName, error) {
	s.lookupCount[tableID]++
	tableName, ok := s.names[tableID]
	if !ok {
		return common.TableName{}, errors.New("table name not found")
	}
	return tableName, nil
}

func (s *routeTableNames) count(tableID int64) int {
	return s.lookupCount[tableID]
}

func routeAllTo(targetSchema, targetTable string) []*config.DispatchRule {
	return []*config.DispatchRule{{
		Matcher:      []string{"*.*"},
		TargetSchema: targetSchema,
		TargetTable:  targetTable,
	}}
}

func routeBySource() []*config.DispatchRule {
	return []*config.DispatchRule{
		routeExact("db1", "t", "db1_target", "t"),
		routeExact("db1", "u", "db1_target", "u"),
		routeExact("db2", "t", "db2_target", "t"),
		routeExact("db3", "t", "db3_target", "t"),
		routeExact("db4", "t", "db4_target", "t"),
	}
}

func routeForRename() []*config.DispatchRule {
	return []*config.DispatchRule{
		routeExact("db1", "t", "target", "t"),
		routeExact("db1", "u", "target", "u"),
	}
}

func routeExact(sourceSchema, sourceTable, targetSchema, targetTable string) *config.DispatchRule {
	return &config.DispatchRule{
		Matcher:      []string{sourceSchema + "." + sourceTable},
		TargetSchema: targetSchema,
		TargetTable:  targetTable,
	}
}
