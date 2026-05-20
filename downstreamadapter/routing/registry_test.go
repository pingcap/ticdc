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

func TestTargetTableRegistryAdd(t *testing.T) {
	t.Parallel()

	t.Run("non-conflicting bindings", func(t *testing.T) {
		t.Parallel()
		r := NewTargetTableRegistry()
		require.NotNil(t, r)

		require.NoError(t, r.Add(newRouteBinding(1, "db1", "t1", "db1", "t1")))
		require.NoError(t, r.Add(newRouteBinding(2, "db1", "t2", "archive", "t2")))
	})

	t.Run("conflicting bindings fail", func(t *testing.T) {
		t.Parallel()
		r := NewTargetTableRegistry()

		require.NoError(t, r.Add(newRouteBinding(1, "db1", "t1", "archive", "orders")))
		err := r.Add(newRouteBinding(2, "db2", "t1", "archive", "orders"))
		require.Error(t, err)
		require.True(t, errors.ErrTableRouteConflict.Equal(err))
		require.Contains(t, err.Error(), "target `archive`.`orders`")
		require.Contains(t, err.Error(), "source `db1`.`t1`")
		require.Contains(t, err.Error(), "source `db2`.`t1`")
	})

	t.Run("same source multi-replica allowed", func(t *testing.T) {
		t.Parallel()
		r := NewTargetTableRegistry()

		require.NoError(t, r.Add(newRouteBinding(1, "db1", "t1", "db1", "t1")))
		require.NoError(t, r.Add(newRouteBinding(1, "db1", "t1", "db1", "t1")))
	})
}

func TestValidateNoStaticRouteConflict(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangeFeedIDWithName("test-changefeed", common.DefaultKeyspaceName)
	rules := []*config.DispatchRule{
		{Matcher: []string{"db1.*"}, TargetSchema: "archive", TargetTable: "{table}"},
		{Matcher: []string{"db2.*"}, TargetSchema: "archive", TargetTable: "{table}"},
	}

	err := ValidateNoStaticRouteConflict(
		changefeedID,
		false,
		rules,
		[]common.TableName{newRouteConflictTableName(1, "db1", "orders")},
		[]common.TableName{newRouteConflictTableName(2, "db2", "orders")},
	)
	require.Error(t, err)
	require.True(t, errors.ErrTableRouteConflict.Equal(err))
	require.Contains(t, err.Error(), "target `archive`.`orders`")
	require.Contains(t, err.Error(), "source `db1`.`orders`")
	require.Contains(t, err.Error(), "source `db2`.`orders`")

	err = ValidateNoStaticRouteConflict(changefeedID, false, rules, []common.TableName{
		newRouteConflictTableName(1, "db1", "orders"),
		newRouteConflictTableName(2, "db2", "customers"),
	})
	require.NoError(t, err)
}

func newRouteConflictTableName(tableID int64, schema, table string) common.TableName {
	return common.TableName{
		Schema:  schema,
		Table:   table,
		TableID: tableID,
	}
}
