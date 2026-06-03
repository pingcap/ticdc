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
	r := NewTargetTableRegistry(changefeedID, 0)
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

	err = r.ApplyTransition(nil, []RouteBinding{NewRouteBinding("db1", "t1", "archive", "orders_new")}, true)
	require.Error(t, err)
	require.True(t, errors.ErrInternalCheckFailed.Equal(err))

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

	err = r.ApplyTransition(nil, []RouteBinding{
		NewRouteBinding("db6", "t6", "archive", "payments"),
		NewRouteBinding("db6", "t6", "archive", "payments_new"),
	}, true)
	require.Error(t, err)
	require.True(t, errors.ErrInternalCheckFailed.Equal(err))
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
