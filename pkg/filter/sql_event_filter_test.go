// Copyright 2025 PingCAP, Inc.
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

package filter

import (
	"testing"

	"github.com/pingcap/errors"
	bf "github.com/pingcap/ticdc/pkg/binlog-filter"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestSQLEventFilterDML(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name            string
		cfg             *config.FilterConfig
		schema          string
		table           string
		dmlType         common.RowType
		shouldBeSkipped bool
	}{
		{
			name: "ignore insert event",
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{
						Matcher:     []string{"test.*"},
						IgnoreEvent: []bf.EventType{bf.InsertEvent},
					},
				},
			},
			schema:          "test",
			table:           "t1",
			dmlType:         common.RowTypeInsert,
			shouldBeSkipped: true,
		},
		{
			name: "don't ignore update event",
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{
						Matcher:     []string{"test.*"},
						IgnoreEvent: []bf.EventType{bf.InsertEvent},
					},
				},
			},
			schema:          "test",
			table:           "t1",
			dmlType:         common.RowTypeUpdate,
			shouldBeSkipped: false,
		},
		{
			name: "ignore all dml events",
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{
						Matcher:     []string{"test.*"},
						IgnoreEvent: []bf.EventType{bf.AllDML},
					},
				},
			},
			schema:          "test",
			table:           "t1",
			dmlType:         common.RowTypeDelete,
			shouldBeSkipped: true,
		},
		{
			name: "matcher does not match",
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{
						Matcher:     []string{"otherdb.*"},
						IgnoreEvent: []bf.EventType{bf.AllDML},
					},
				},
			},
			schema:          "test",
			table:           "t1",
			dmlType:         common.RowTypeInsert,
			shouldBeSkipped: false,
		},
	}

	for _, cs := range cases {
		t.Run(cs.name, func(t *testing.T) {
			filter, err := newSQLEventFilter(cs.cfg)
			require.NoError(t, err)
			skip, err := filter.shouldSkipDML(cs.schema, cs.table, cs.dmlType)
			require.NoError(t, err)
			require.Equal(t, cs.shouldBeSkipped, skip)
		})
	}
}

func TestSQLEventFilterDDL(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name            string
		cfg             *config.FilterConfig
		schema          string
		table           string
		ddlType         model.ActionType
		query           string
		shouldBeSkipped bool
	}{
		{
			name: "ignore create table event",
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{
						Matcher:     []string{"test.*"},
						IgnoreEvent: []bf.EventType{bf.CreateTable},
					},
				},
			},
			schema: "test", table: "t1", ddlType: model.ActionCreateTable, query: "create table t1(id int)",
			shouldBeSkipped: true,
		},
		{
			name: "ignore create database by alias",
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{
						Matcher:     []string{"test.*"},
						IgnoreEvent: []bf.EventType{bf.CreateDatabase},
					},
				},
			},
			schema: "test", table: "", ddlType: model.ActionCreateSchema, query: "create database test",
			shouldBeSkipped: true,
		},
		{
			name: "ignore add column by ignoring alter table",
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{
						Matcher:     []string{"test.*"},
						IgnoreEvent: []bf.EventType{bf.AlterTable},
					},
				},
			},
			schema: "test", table: "t1", ddlType: model.ActionAddColumn, query: "alter table t1 add column c1 int",
			shouldBeSkipped: true,
		},
		{
			name: "ignore rename index specifically, not all alter table",
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{
						Matcher:     []string{"test.*"},
						IgnoreEvent: []bf.EventType{bf.RenameIndex},
					},
				},
			},
			schema: "test", table: "t1", ddlType: model.ActionRenameIndex, query: "alter table t1 rename index i1 to i2",
			shouldBeSkipped: true,
		},
		{
			name: "don't ignore add column when only rename index is ignored",
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{
						Matcher:     []string{"test.*"},
						IgnoreEvent: []bf.EventType{bf.RenameIndex},
					},
				},
			},
			schema: "test", table: "t1", ddlType: model.ActionAddColumn, query: "alter table t1 add column c1 int",
			shouldBeSkipped: false,
		},
		{
			name: "ignore ddl by sql regex",
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{
						Matcher:   []string{"test.*"},
						IgnoreSQL: []string{"^DROP TABLE", "add column"},
					},
				},
			},
			schema: "test", table: "t1", ddlType: model.ActionDropTable, query: "DROP TABLE t1",
			shouldBeSkipped: true,
		},
		{
			name: "ignore ddl by sql regex (case 2)",
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{
						Matcher:   []string{"test.*"},
						IgnoreSQL: []string{"^DROP TABLE", "add column"},
					},
				},
			},
			schema: "test", table: "t1", ddlType: model.ActionAddColumn, query: "alter table t1 add column c1 int",
			shouldBeSkipped: true,
		},
		{
			name: "don't ignore ddl by sql regex",
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{
						Matcher:   []string{"test.*"},
						IgnoreSQL: []string{"^DROP TABLE"},
					},
				},
			},
			schema: "test", table: "t1", ddlType: model.ActionCreateTable, query: "CREATE TABLE t1(id int)",
			shouldBeSkipped: false,
		},
		{
			name: "ignore all ddl",
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{
						Matcher:     []string{"test.*"},
						IgnoreEvent: []bf.EventType{bf.AllDDL},
					},
				},
			},
			schema: "test", table: "t1", ddlType: model.ActionCreateTable, query: "CREATE TABLE t1(id int)",
			shouldBeSkipped: true,
		},
	}

	for _, cs := range cases {
		t.Run(cs.name, func(t *testing.T) {
			filter, err := newSQLEventFilter(cs.cfg)
			require.NoError(t, err)
			skip, err := filter.shouldSkipDDL(cs.schema, cs.table, cs.query, cs.ddlType)
			require.NoError(t, err)
			require.Equal(t, cs.shouldBeSkipped, skip)
		})
	}
}

func TestSQLEventFilterInvalidConfig(t *testing.T) {
	t.Parallel()
	// invalid event type
	cfg := &config.FilterConfig{
		EventFilters: []*config.EventFilterRule{
			{
				Matcher:     []string{"test.*"},
				IgnoreEvent: []bf.EventType{"invalid-event"},
			},
		},
	}
	_, err := newSQLEventFilter(cfg)
	require.Error(t, err)
	require.Regexp(t, "invalid ignore event type", err.Error())

	// invalid matcher
	cfg = &config.FilterConfig{
		EventFilters: []*config.EventFilterRule{
			{
				Matcher: []string{"test.t[1"}, // invalid regex in matcher
			},
		},
	}
	_, err = newSQLEventFilter(cfg)
	require.Error(t, err)
	require.Regexp(t, "ErrFilterRuleInvalid", err.Error())

	// invalid sql regex
	cfg = &config.FilterConfig{
		EventFilters: []*config.EventFilterRule{
			{
				Matcher:   []string{"test.t1"},
				IgnoreSQL: []string{"^["},
			},
		},
	}
	_, err = newSQLEventFilter(cfg)
	require.Error(t, err)
	require.Regexp(t, "ErrFilterRuleInvalid", err.Error())
}

func TestShouldSkipDDL(t *testing.T) {
	t.Parallel()
	type innerCase struct {
		schema  string
		table   string
		query   string
		ddlType timodel.ActionType
		skip    bool
	}

	type testCase struct {
		cfg   *config.FilterConfig
		cases []innerCase
		err   error
	}

	// filter all ddl
	case1 := testCase{
		cfg: &config.FilterConfig{
			EventFilters: []*config.EventFilterRule{
				{
					Matcher:     []string{"test.t1"},
					IgnoreEvent: []bf.EventType{bf.AllDDL},
				},
			},
		},
		cases: []innerCase{
			{
				schema:  "test",
				table:   "t1",
				query:   "alter table t1 modify column age int",
				ddlType: timodel.ActionModifyColumn,
				skip:    true,
			},
			{
				schema:  "test",
				table:   "t1",
				query:   "create table t1(id int primary key)",
				ddlType: timodel.ActionCreateTable,
				skip:    true,
			},
			{
				schema:  "test",
				table:   "t2", // table name not match
				query:   "alter table t2 modify column age int",
				ddlType: timodel.ActionModifyColumn,
				skip:    false,
			},
			{
				schema:  "test2", // schema name not match
				table:   "t1",
				query:   "alter table t1 modify column age int",
				ddlType: timodel.ActionModifyColumn,
				skip:    false,
			},
		},
	}
	f, err := newSQLEventFilter(case1.cfg)
	require.True(t, errors.ErrorEqual(err, case1.err), "case: %+s", err)
	for _, c := range case1.cases {
		skip, err := f.shouldSkipDDL(c.schema, c.table, c.query, c.ddlType)
		require.NoError(t, err)
		require.Equal(t, c.skip, skip, "case: %+v", c)
	}

	// filter some ddl
	case2 := testCase{
		cfg: &config.FilterConfig{
			EventFilters: []*config.EventFilterRule{
				{
					Matcher:     []string{"*.t1"},
					IgnoreEvent: []bf.EventType{bf.DropDatabase, bf.DropSchema},
				},
			},
		},
		cases: []innerCase{
			{
				schema:  "test",
				table:   "t1",
				query:   "alter table t1 modify column age int",
				ddlType: timodel.ActionModifyColumn,
				skip:    false,
			},
			{
				schema:  "test",
				table:   "t1",
				query:   "alter table t1 drop column age",
				ddlType: timodel.ActionDropColumn,
				skip:    false,
			},
			{
				schema:  "test2",
				table:   "t1",
				query:   "drop database test2",
				ddlType: timodel.ActionDropSchema,
				skip:    true,
			},
			{
				schema:  "test3",
				table:   "t1",
				query:   "drop index i3 on t1",
				ddlType: timodel.ActionDropIndex,
				skip:    false,
			},
		},
	}
	f, err = newSQLEventFilter(case2.cfg)
	require.True(t, errors.ErrorEqual(err, case2.err), "case: %+s", err)
	for _, c := range case2.cases {
		skip, err := f.shouldSkipDDL(c.schema, c.table, c.query, c.ddlType)
		require.NoError(t, err)
		require.Equal(t, c.skip, skip, "case: %+v", c)
	}

	// filter ddl by IgnoreSQL
	case3 := testCase{
		cfg: &config.FilterConfig{
			EventFilters: []*config.EventFilterRule{
				{
					Matcher:   []string{"*.t1"},
					IgnoreSQL: []string{"MODIFY COLUMN", "DROP COLUMN", "^DROP DATABASE"},
				},
			},
		},
		cases: []innerCase{
			{
				schema:  "test",
				table:   "t1",
				query:   "ALTER TABLE t1 MODIFY COLUMN age int(11) NOT NULL",
				ddlType: timodel.ActionModifyColumn,
				skip:    true,
			},
			{
				schema:  "test",
				table:   "t1",
				query:   "ALTER TABLE t1 DROP COLUMN age",
				ddlType: timodel.ActionDropColumn,
				skip:    true,
			},
			{ // no table name
				schema:  "test2",
				query:   "DROP DATABASE test",
				ddlType: timodel.ActionDropSchema,
				skip:    true,
			},
			{
				schema:  "test3",
				table:   "t1",
				query:   "Drop Index i1 on test3.t1",
				ddlType: timodel.ActionDropIndex,
				skip:    false,
			},
		},
	}
	f, err = newSQLEventFilter(case3.cfg)
	require.True(t, errors.ErrorEqual(err, case3.err), "case: %+s", err)
	for _, c := range case3.cases {
		skip, err := f.shouldSkipDDL(c.schema, c.table, c.query, c.ddlType)
		require.NoError(t, err)
		require.Equal(t, c.skip, skip, "case: %+v", c)
	}

	// config error
	case4 := testCase{
		cfg: &config.FilterConfig{
			EventFilters: []*config.EventFilterRule{
				{
					Matcher:     []string{"*.t1"},
					IgnoreEvent: []bf.EventType{bf.EventType("aa")},
				},
			},
		},
		err: cerror.ErrInvalidIgnoreEventType,
	}
	_, err = newSQLEventFilter(case4.cfg)
	require.True(t, errors.ErrorEqual(err, case4.err), "case: %+s", err)

	// config error
	case5 := testCase{
		cfg: &config.FilterConfig{
			EventFilters: []*config.EventFilterRule{
				{
					Matcher:   []string{"*.t1"},
					IgnoreSQL: []string{"--6"}, // this is a valid regx
				},
			},
		},
	}
	_, err = newSQLEventFilter(case5.cfg)
	require.True(t, errors.ErrorEqual(err, case5.err), "case: %+s", err)
}

func TestVerifyIgnoreEvents(t *testing.T) {
	t.Parallel()
	type testCase struct {
		ignoreEvent []bf.EventType
		err         error
	}

	cases := make([]testCase, len(SupportedEventTypes()))
	for i, eventType := range SupportedEventTypes() {
		cases[i] = testCase{
			ignoreEvent: []bf.EventType{eventType},
			err:         nil,
		}
	}

	cases = append(cases, testCase{
		ignoreEvent: []bf.EventType{bf.EventType("unknown")},
		err:         cerror.ErrInvalidIgnoreEventType,
	})

	cases = append(cases, testCase{
		ignoreEvent: []bf.EventType{bf.AlterTable},
		err:         nil,
	})

	for _, tc := range cases {
		require.True(t, errors.ErrorEqual(tc.err, verifyIgnoreEvents(tc.ignoreEvent)))
	}
}
