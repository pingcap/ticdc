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

	changefeedID := common.NewChangeFeedIDWithName("test-changefeed", common.DefaultKeyspaceName)
	t.Run("non-conflicting bindings", func(t *testing.T) {
		t.Parallel()
		r := NewTargetTableRegistry(changefeedID)
		require.NotNil(t, r)

		require.NoError(t, r.Add(newRouteBinding("db1", "t1", "db1", "t1")))
		require.NoError(t, r.Add(newRouteBinding("db1", "t2", "archive", "t2")))
	})

	t.Run("conflicting bindings fail", func(t *testing.T) {
		t.Parallel()
		r := NewTargetTableRegistry(changefeedID)

		require.NoError(t, r.Add(newRouteBinding("db1", "t1", "archive", "orders")))
		err := r.Add(newRouteBinding("db2", "t1", "archive", "orders"))
		require.Error(t, err)
		require.True(t, errors.ErrTableRouteConflict.Equal(err))
		require.Contains(t, err.Error(), "target `archive`.`orders`")
		require.Contains(t, err.Error(), "source `db1`.`t1`")
		require.Contains(t, err.Error(), "source `db2`.`t1`")
	})

	t.Run("same source mapping is idempotent", func(t *testing.T) {
		t.Parallel()
		r := NewTargetTableRegistry(changefeedID)

		require.NoError(t, r.Add(newRouteBinding("db1", "t1", "db1", "t1")))
		require.NoError(t, r.Add(newRouteBinding("db1", "t1", "db1", "t1")))
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
