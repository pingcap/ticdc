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

func TestTargetTableRegistry(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangeFeedIDWithName("test-changefeed", common.DefaultKeyspaceName)
	r := NewTargetTableRegistry(changefeedID, false, 0)
	require.NotNil(t, r)

	require.NoError(t, r.ApplyTransition(nil, []RouteBinding{
		NewRouteBinding("db1", "t1", "db1", "t1"),
		NewRouteBinding("db1", "t2", "archive", "t2"),
		NewRouteBinding("db1", "t3", "archive", "orders"),
	}, true))

	err := r.ApplyTransition(nil, []RouteBinding{NewRouteBinding("db2", "t1", "archive", "orders")}, true)
	require.Error(t, err)
	require.True(t, errors.ErrTableRouteConflict.Equal(err))
	require.Contains(t, err.Error(), "target `archive`.`orders`")
	require.Contains(t, err.Error(), "source `db1`.`t3`")
	require.Contains(t, err.Error(), "source `db2`.`t1`")

	require.NoError(t, r.ApplyTransition(nil, []RouteBinding{NewRouteBinding("db1", "t1", "db1", "t1")}, true))

	require.NoError(t, r.ApplyTransition([]TableKey{{Schema: "db1", Table: "t3"}}, nil, true))
	require.Len(t, r.target2Source, 2)
	require.NoError(t, r.ApplyTransition(nil, []RouteBinding{NewRouteBinding("db2", "t1", "archive", "orders")}, true))
	require.NoError(t, r.ApplyTransition([]TableKey{{Schema: "db1", Table: "t3"}}, nil, true))
	require.Len(t, r.target2Source, 3)

	require.NoError(t, r.ApplyTransition(
		[]TableKey{{Schema: "db2", Table: "t1"}},
		[]RouteBinding{NewRouteBinding("db2", "t1_new", "archive", "orders")},
		true,
	))
	require.Len(t, r.target2Source, 3)
	err = r.ApplyTransition(nil, []RouteBinding{NewRouteBinding("db3", "t3", "archive", "orders")}, true)
	require.Error(t, err)
	require.True(t, errors.ErrTableRouteConflict.Equal(err))

	err = r.ApplyTransition(
		[]TableKey{{Schema: "db2", Table: "t1_new"}},
		[]RouteBinding{NewRouteBinding("db3", "t3", "archive", "t2")},
		true,
	)
	require.Error(t, err)
	require.True(t, errors.ErrTableRouteConflict.Equal(err))
	require.Len(t, r.target2Source, 3)
	require.NoError(t, r.ApplyTransition(nil, []RouteBinding{NewRouteBinding("db2", "t1_new", "archive", "orders")}, true))

	err = r.ApplyTransition(nil, []RouteBinding{
		NewRouteBinding("db4", "t4", "archive", "invoices"),
	}, false)
	require.NoError(t, err)
	require.Len(t, r.target2Source, 3)

	require.NoError(t, r.ApplyTransition(nil, []RouteBinding{NewRouteBinding("db4", "t4", "archive", "invoices")}, true))
	err = r.ApplyTransition(nil, []RouteBinding{
		NewRouteBinding("db5", "t5", "archive", "invoices"),
	}, false)
	require.Error(t, err)
	require.True(t, errors.ErrTableRouteConflict.Equal(err))
	require.Len(t, r.target2Source, 4)

	err = r.ApplyTransition(nil, []RouteBinding{
		NewRouteBinding("db6", "t6", "archive", "payments"),
		NewRouteBinding("db7", "t7", "archive", "payments"),
	}, true)
	require.Error(t, err)
	require.True(t, errors.ErrTableRouteConflict.Equal(err))
	require.Len(t, r.target2Source, 4)
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
		[]common.TableName{{Schema: "db1", Table: "orders"}},
		[]common.TableName{{Schema: "db2", Table: "orders"}},
	)
	require.Error(t, err)
	require.True(t, errors.ErrTableRouteConflict.Equal(err))
	require.Contains(t, err.Error(), "target `archive`.`orders`")
	require.Contains(t, err.Error(), "source `db1`.`orders`")
	require.Contains(t, err.Error(), "source `db2`.`orders`")

	err = ValidateNoStaticRouteConflict(changefeedID, false, rules, []common.TableName{
		{Schema: "db1", Table: "orders"},
		{Schema: "db2", Table: "customers"},
	})
	require.NoError(t, err)

	err = ValidateNoStaticRouteConflict(
		changefeedID,
		false,
		[]*config.DispatchRule{
			{Matcher: []string{"db2.*"}, TargetSchema: "db1", TargetTable: "{table}"},
		},
		[]common.TableName{
			{Schema: "db1", Table: "orders"},
			{Schema: "db2", Table: "orders"},
		},
	)
	require.Error(t, err)
	require.True(t, errors.ErrTableRouteConflict.Equal(err))
	require.Contains(t, err.Error(), "target `db1`.`orders`")
	require.Contains(t, err.Error(), "source `db1`.`orders`")
	require.Contains(t, err.Error(), "source `db2`.`orders`")
}

// TestValidateNoStaticRouteConflictCaseSensitivity checks that target identity
// follows the changefeed's case sensitivity: a case-insensitive changefeed routes
// both sources into one table, so the rules conflict.
func TestValidateNoStaticRouteConflictCaseSensitivity(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangeFeedIDWithName("test-changefeed", common.DefaultKeyspaceName)
	rules := []*config.DispatchRule{
		{Matcher: []string{"db1.*"}, TargetSchema: "archive", TargetTable: "Orders"},
		{Matcher: []string{"db2.*"}, TargetSchema: "archive", TargetTable: "orders"},
	}
	tables := []common.TableName{
		{Schema: "db1", Table: "t1"},
		{Schema: "db2", Table: "t2"},
	}

	err := ValidateNoStaticRouteConflict(changefeedID, false, rules, tables)
	require.Error(t, err)
	require.True(t, errors.ErrTableRouteConflict.Equal(err))

	require.NoError(t, ValidateNoStaticRouteConflict(changefeedID, true, rules, tables))
}

// TestTargetTableRegistryCaseSensitivity covers table identity in the registry.
func TestTargetTableRegistryCaseSensitivity(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangeFeedIDWithName("test-changefeed", common.DefaultKeyspaceName)

	t.Run("case insensitive targets conflict", func(t *testing.T) {
		t.Parallel()
		r := NewTargetTableRegistry(changefeedID, false, 0)
		require.NoError(t, r.ApplyTransition(nil, []RouteBinding{
			NewRouteBinding("db1", "t1", "archive", "Orders"),
		}, true))
		err := r.ApplyTransition(nil, []RouteBinding{
			NewRouteBinding("db2", "t2", "archive", "orders"),
		}, true)
		require.Error(t, err)
		require.True(t, errors.ErrTableRouteConflict.Equal(err))
	})

	t.Run("case sensitive targets stay distinct", func(t *testing.T) {
		t.Parallel()
		r := NewTargetTableRegistry(changefeedID, true, 0)
		require.NoError(t, r.ApplyTransition(nil, []RouteBinding{
			NewRouteBinding("db1", "t1", "archive", "Orders"),
		}, true))
		require.NoError(t, r.ApplyTransition(nil, []RouteBinding{
			NewRouteBinding("db2", "t2", "archive", "orders"),
		}, true))
		require.Len(t, r.target2Source, 2)
	})

	t.Run("case insensitive sources share one owner", func(t *testing.T) {
		t.Parallel()
		r := NewTargetTableRegistry(changefeedID, false, 0)
		require.NoError(t, r.ApplyTransition(nil, []RouteBinding{
			NewRouteBinding("db1", "t1", "archive", "orders"),
		}, true))
		// The same table spelled differently is idempotent, not a conflict.
		require.NoError(t, r.ApplyTransition(nil, []RouteBinding{
			NewRouteBinding("db1", "T1", "archive", "orders"),
		}, true))
		require.Len(t, r.source2Target, 1)

		// Releasing the differently spelled name releases the same entry.
		require.NoError(t, r.ApplyTransition([]TableKey{{Schema: "DB1", Table: "T1"}}, nil, true))
		require.Empty(t, r.source2Target)
		require.Empty(t, r.target2Source)
	})

	t.Run("case sensitive sources are different owners", func(t *testing.T) {
		t.Parallel()
		r := NewTargetTableRegistry(changefeedID, true, 0)
		require.NoError(t, r.ApplyTransition(nil, []RouteBinding{
			NewRouteBinding("db1", "t1", "archive", "orders"),
		}, true))
		err := r.ApplyTransition(nil, []RouteBinding{
			NewRouteBinding("db1", "T1", "archive", "orders"),
		}, true)
		require.Error(t, err)
		require.True(t, errors.ErrTableRouteConflict.Equal(err))
	})
}
