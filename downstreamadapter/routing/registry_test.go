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

package routing

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
)

func makeBinding(
	logicalID, replicaID, schemaID int64,
	sourceSchema, sourceTable, targetSchema, targetTable string,
	ruleIndex int,
	matcher []string,
) RouteBinding {
	return RouteBinding{
		Source: SourceKey{
			LogicalTableID: logicalID,
			Schema:         sourceSchema,
			Table:          sourceTable,
		},
		ReplicaTableID: replicaID,
		SourceSchemaID: schemaID,
		Target: TargetKey{
			Schema: targetSchema,
			Table:  targetTable,
		},
		RuleIndex: ruleIndex,
		Matcher:   matcher,
	}
}

func TestNewTargetTableRegistry(t *testing.T) {
	t.Parallel()

	t.Run("empty bindings", func(t *testing.T) {
		t.Parallel()
		r, err := NewTargetTableRegistry(nil)
		require.NoError(t, err)
		require.NotNil(t, r)
		require.Empty(t, r.Snapshot())
	})

	t.Run("non-conflicting bindings", func(t *testing.T) {
		t.Parallel()
		bindings := []RouteBinding{
			makeBinding(1, 1, 10, "db1", "t1", "db1", "t1", -1, nil),
			makeBinding(2, 2, 10, "db1", "t2", "archive", "t2", 0, []string{"db1.*"}),
		}
		r, err := NewTargetTableRegistry(bindings)
		require.NoError(t, err)
		require.Len(t, r.Snapshot(), 2)
	})

	t.Run("conflicting bindings fail", func(t *testing.T) {
		t.Parallel()
		bindings := []RouteBinding{
			makeBinding(1, 1, 10, "db1", "t1", "archive", "orders", 0, []string{"db1.*"}),
			makeBinding(2, 2, 20, "db2", "t1", "archive", "orders", 0, []string{"db2.*"}),
		}
		_, err := NewTargetTableRegistry(bindings)
		require.Error(t, err)
		require.True(t, errors.ErrTableRouteConflict.Equal(err))
	})

	t.Run("same source multi-replica allowed", func(t *testing.T) {
		t.Parallel()
		bindings := []RouteBinding{
			makeBinding(1, 100, 10, "db1", "t1", "db1", "t1", -1, nil),
			makeBinding(1, 101, 10, "db1", "t1", "db1", "t1", -1, nil),
		}
		r, err := NewTargetTableRegistry(bindings)
		require.NoError(t, err)
		require.Len(t, r.Snapshot(), 2)
	})
}

func TestTargetTableRegistryValidateAdd(t *testing.T) {
	t.Parallel()

	r, err := NewTargetTableRegistry([]RouteBinding{
		makeBinding(1, 1, 10, "db1", "t1", "archive", "orders", 0, []string{"db1.*"}),
	})
	require.NoError(t, err)

	err = r.ValidateAdd(makeBinding(2, 2, 20, "db2", "t2", "archive", "customers", 0, []string{"db2.*"}))
	require.NoError(t, err)

	err = r.ValidateAdd(makeBinding(1, 3, 10, "db1", "t1", "archive", "orders", 0, []string{"db1.*"}))
	require.NoError(t, err)

	err = r.ValidateAdd(makeBinding(2, 2, 20, "db2", "t2", "archive", "orders", 0, []string{"db2.*"}))
	require.Error(t, err)
	require.True(t, errors.ErrTableRouteConflict.Equal(err))
	require.Contains(t, err.Error(), "target `archive`.`orders`")
	require.Contains(t, err.Error(), "source `db1`.`t1`")
	require.Contains(t, err.Error(), "source `db2`.`t2`")
	require.Contains(t, err.Error(), "matcher=[db1.*]")
	require.Contains(t, err.Error(), "matcher=[db2.*]")
}

func TestValidateNoStaticRouteConflict(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangeFeedIDWithName("test-changefeed", common.DefaultKeyspaceName)
	rules := []*config.DispatchRule{
		{Matcher: []string{"db1.*"}, TargetSchema: "archive", TargetTable: "{table}"},
		{Matcher: []string{"db2.*"}, TargetSchema: "archive", TargetTable: "{table}"},
	}

	err := ValidateNoStaticRouteConflict(changefeedID, false, rules, []*common.TableInfo{
		newRouteConflictTableInfo(1, "db1", "orders"),
		newRouteConflictTableInfo(2, "db2", "orders"),
	})
	require.Error(t, err)
	require.True(t, errors.ErrTableRouteConflict.Equal(err))
	require.Contains(t, err.Error(), "test-changefeed")
	require.Contains(t, err.Error(), "target `archive`.`orders`")
	require.Contains(t, err.Error(), "rule=0")
	require.Contains(t, err.Error(), "rule=1")

	err = ValidateNoStaticRouteConflict(changefeedID, false, rules, []*common.TableInfo{
		newRouteConflictTableInfo(1, "db1", "orders"),
		newRouteConflictTableInfo(2, "db2", "customers"),
	})
	require.NoError(t, err)
}

func newRouteConflictTableInfo(tableID int64, schema, table string) *common.TableInfo {
	return &common.TableInfo{
		TableName: common.TableName{
			Schema:  schema,
			Table:   table,
			TableID: tableID,
		},
	}
}
