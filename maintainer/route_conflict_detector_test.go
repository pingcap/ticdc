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
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/ticdc/downstreamadapter/routing"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/filter"
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
	store := &fakeRouteSchemaStore{
		tables: map[int64]*common.TableInfo{
			1: newTestRouteTableInfo(1, "db1", "t"),
			2: newTestRouteTableInfo(2, "db2", "t"),
		},
	}
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

func newTestRouteTableInfo(tableID int64, schema, table string) *common.TableInfo {
	return &common.TableInfo{
		TableName: common.TableName{
			Schema:  schema,
			Table:   table,
			TableID: tableID,
		},
	}
}

type fakeRouteSchemaStore struct {
	tables map[int64]*common.TableInfo
}

func (s *fakeRouteSchemaStore) Name() string {
	return "fake-route-schema-store"
}

func (s *fakeRouteSchemaStore) Run(ctx context.Context) error {
	return nil
}

func (s *fakeRouteSchemaStore) Close(ctx context.Context) error {
	return nil
}

func (s *fakeRouteSchemaStore) GetAllPhysicalTables(
	keyspaceMeta common.KeyspaceMeta,
	snapTs uint64,
	filter filter.Filter,
) ([]commonEvent.Table, error) {
	return nil, nil
}

func (s *fakeRouteSchemaStore) RegisterTable(
	keyspaceMeta common.KeyspaceMeta,
	tableID int64,
	startTs uint64,
) error {
	return nil
}

func (s *fakeRouteSchemaStore) UnregisterTable(
	keyspaceMeta common.KeyspaceMeta,
	tableID int64,
) error {
	return nil
}

func (s *fakeRouteSchemaStore) GetTableInfo(
	keyspaceMeta common.KeyspaceMeta,
	tableID int64,
	ts uint64,
) (*common.TableInfo, error) {
	tableInfo, ok := s.tables[tableID]
	if !ok {
		return nil, fmt.Errorf("table %d not found", tableID)
	}
	return tableInfo, nil
}

func (s *fakeRouteSchemaStore) GetTableDDLEventState(
	keyspaceMeta common.KeyspaceMeta,
	tableID int64,
) (schemastore.DDLEventState, error) {
	return schemastore.DDLEventState{}, nil
}

func (s *fakeRouteSchemaStore) FetchTableDDLEvents(
	keyspaceMeta common.KeyspaceMeta,
	dispatcherID common.DispatcherID,
	tableID int64,
	tableFilter filter.Filter,
	start uint64,
	end uint64,
) ([]commonEvent.DDLEvent, error) {
	return nil, nil
}

func (s *fakeRouteSchemaStore) FetchTableTriggerDDLEvents(
	keyspaceMeta common.KeyspaceMeta,
	dispatcherID common.DispatcherID,
	tableFilter filter.Filter,
	start uint64,
	limit int,
) ([]commonEvent.DDLEvent, uint64, error) {
	return nil, 0, nil
}

func (s *fakeRouteSchemaStore) RegisterKeyspace(
	ctx context.Context,
	keyspaceMeta common.KeyspaceMeta,
) error {
	return nil
}
