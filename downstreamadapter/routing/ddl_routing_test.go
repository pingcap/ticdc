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
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestRewriteDDLQueryWithRoutingReturnsOriginalQueryForNoopCases(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test-changefeed")

	noRouterDDL := &commonEvent.DDLEvent{
		Query: "CREATE TABLE `source_db`.`test_table` (id INT PRIMARY KEY)",
		TableInfo: &common.TableInfo{
			TableName: common.TableName{Schema: "source_db", Table: "test_table"},
		},
	}
	newQuery, changed, err := rewriteDDLQueryWithRouting(nil, noRouterDDL, changefeedID)
	require.NoError(t, err)
	require.False(t, changed)
	require.Equal(t, noRouterDDL.Query, newQuery)

	emptyQueryDDL := &commonEvent.DDLEvent{}
	newQuery, changed, err = rewriteDDLQueryWithRouting(nil, emptyQueryDDL, changefeedID)
	require.NoError(t, err)
	require.False(t, changed)
	require.Empty(t, newQuery)

	router, err := NewRouter(false, []*config.DispatchRule{
		{
			Matcher:      []string{"source_db.*"},
			TargetSchema: "target_db",
			TargetTable:  TablePlaceholder,
		},
	})
	require.NoError(t, err)

	noMatchDDL := &commonEvent.DDLEvent{
		Query: "CREATE TABLE `other_db`.`test_table` (id INT PRIMARY KEY)",
		TableInfo: &common.TableInfo{
			TableName: common.TableName{Schema: "other_db", Table: "test_table"},
		},
	}
	newQuery, changed, err = rewriteDDLQueryWithRouting(router, noMatchDDL, changefeedID)
	require.NoError(t, err)
	require.False(t, changed)
	require.Equal(t, noMatchDDL.Query, newQuery)
}

func TestRewriteDDLQueryWithRoutingRewritesMatchedTableDDL(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(false, []*config.DispatchRule{
		{
			Matcher:      []string{"source_db.*"},
			TargetSchema: "target_db",
			TargetTable:  "{table}_routed",
		},
	})
	require.NoError(t, err)

	ddl := &commonEvent.DDLEvent{
		Query: "ALTER TABLE `source_db`.`test_table` ADD COLUMN c INT",
		TableInfo: &common.TableInfo{
			TableName: common.TableName{Schema: "source_db", Table: "test_table"},
		},
	}

	newQuery, changed, err := rewriteDDLQueryWithRouting(
		router, ddl, common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test-changefeed"),
	)
	require.NoError(t, err)
	require.True(t, changed)
	require.Contains(t, newQuery, "`target_db`.`test_table_routed`")
	require.NotContains(t, newQuery, "`source_db`.`test_table`")
}

func TestRewriteDDLQueryWithRoutingRewritesRenameDDL(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(false, []*config.DispatchRule{
		{
			Matcher:      []string{"db1.*"},
			TargetSchema: "target1",
			TargetTable:  TablePlaceholder,
		},
		{
			Matcher:      []string{"db2.*"},
			TargetSchema: "target2",
			TargetTable:  TablePlaceholder,
		},
	})
	require.NoError(t, err)

	ddl := &commonEvent.DDLEvent{
		Query: "RENAME TABLE `db1`.`t1` TO `db2`.`t2`",
		TableInfo: &common.TableInfo{
			TableName: common.TableName{Schema: "db2", Table: "t2"},
		},
		MultipleTableInfos: []*common.TableInfo{
			{TableName: common.TableName{Schema: "db2", Table: "t2"}},
			{TableName: common.TableName{Schema: "db1", Table: "t1"}},
		},
	}

	newQuery, changed, err := rewriteDDLQueryWithRouting(
		router, ddl, common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test-changefeed"),
	)
	require.NoError(t, err)
	require.True(t, changed)
	require.Contains(t, newQuery, "`target1`.`t1`")
	require.Contains(t, newQuery, "`target2`.`t2`")
}

func TestRewriteDDLQueryWithRoutingRewritesDatabaseDDL(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(false, []*config.DispatchRule{
		{
			Matcher:      []string{"source_db.*"},
			TargetSchema: "target_db",
		},
	})
	require.NoError(t, err)

	ddl := &commonEvent.DDLEvent{
		Query: "CREATE DATABASE `source_db`",
	}

	newQuery, changed, err := rewriteDDLQueryWithRouting(
		router, ddl, common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test-changefeed"),
	)
	require.NoError(t, err)
	require.True(t, changed)
	require.Contains(t, newQuery, "`target_db`")
	require.NotContains(t, newQuery, "`source_db`")
}

func TestRewriteDDLQueryWithRoutingReturnsTypedParseError(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(false, []*config.DispatchRule{
		{
			Matcher:      []string{"source_db.*"},
			TargetSchema: "target_db",
			TargetTable:  TablePlaceholder,
		},
	})
	require.NoError(t, err)

	ddl := &commonEvent.DDLEvent{Query: "INVALID DDL"}

	_, _, err = rewriteDDLQueryWithRouting(
		router, ddl, common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test-changefeed"),
	)
	require.Error(t, err)
	code, ok := cerror.RFCCode(err)
	require.True(t, ok)
	require.Equal(t, cerror.ErrTableRoutingFailed.RFCCode(), code)
}
