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

package columnselector

import (
	"testing"

	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	parser_model "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestNewColumnSelector(t *testing.T) {
	// the column selector is not set
	replicaConfig := config.GetDefaultReplicaConfig()
	selectors, err := New(replicaConfig.Sink, false)
	require.NoError(t, err)
	require.NotNil(t, selectors)
	require.Len(t, selectors.selectors, 0)

	replicaConfig.Sink.ColumnSelectors = []*config.ColumnSelector{
		{
			Matcher: []string{"test.*"},
			Columns: []string{"a", "b"},
		},
		{
			Matcher: []string{"test1.*"},
			Columns: []string{"*", "!a"},
		},
		{
			Matcher: []string{"test2.*"},
			Columns: []string{"co*", "!col2"},
		},
		{
			Matcher: []string{"test3.*"},
			Columns: []string{"co?1"},
		},
	}
	selectors, err = New(replicaConfig.Sink, false)
	require.NoError(t, err)
	require.Len(t, selectors.selectors, 4)
}

func TestColumnSelectorCaseSensitive(t *testing.T) {
	sinkConfig := &config.SinkConfig{
		ColumnSelectors: []*config.ColumnSelector{{
			Matcher: []string{"Sales.Orders"},
			Columns: []string{"*", "!payload"},
		}},
	}
	payload := &model.ColumnInfo{Name: parser_model.NewCIStr("payload")}
	id := &model.ColumnInfo{Name: parser_model.NewCIStr("id")}
	for _, caseSensitive := range []bool{false, true} {
		selectors, err := New(sinkConfig, caseSensitive)
		require.NoError(t, err)
		require.False(t, selectors.Get("Sales", "Orders").Select(payload))
		for _, table := range [][2]string{{"sales", "Orders"}, {"Sales", "orders"}} {
			selector := selectors.Get(table[0], table[1])
			require.Equal(t, caseSensitive, selector.Select(payload))
			require.True(t, selector.Select(id))
		}
	}
}

func TestColumnSelectorGetSelector(t *testing.T) {
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.ColumnSelectors = []*config.ColumnSelector{
		{
			Matcher: []string{"test.*"},
			Columns: []string{"a", "b"},
		},
		{
			Matcher: []string{"test1.*"},
			Columns: []string{"*", "!a"},
		},
		{
			Matcher: []string{"test2.*"},
			Columns: []string{"co*", "!col2"},
		},
		{
			Matcher: []string{"test3.*"},
			Columns: []string{"co?1"},
		},
	}
	selectors, err := New(replicaConfig.Sink, false)
	require.NoError(t, err)

	{
		selector := selectors.Get("test", "t1")
		columns := []*model.ColumnInfo{
			{
				Name: parser_model.NewCIStr("a"),
			},
			{
				Name: parser_model.NewCIStr("b"),
			},
			{
				Name: parser_model.NewCIStr("c"),
			},
		}
		for _, col := range columns {
			if col.Name.O != "c" {
				require.True(t, selector.Select(col))
			} else {
				require.False(t, selector.Select(col))
			}
		}
	}

	{
		selector := selectors.Get("test1", "aaa")
		columns := []*model.ColumnInfo{
			{
				Name: parser_model.NewCIStr("a"),
			},
			{
				Name: parser_model.NewCIStr("b"),
			},
			{
				Name: parser_model.NewCIStr("c"),
			},
		}
		for _, col := range columns {
			if col.Name.O != "a" {
				require.True(t, selector.Select(col))
			} else {
				require.False(t, selector.Select(col))
			}
		}
	}

	{
		selector := selectors.Get("test2", "t2")
		columns := []*model.ColumnInfo{
			{
				Name: parser_model.NewCIStr("a"),
			},
			{
				Name: parser_model.NewCIStr("col2"),
			},
			{
				Name: parser_model.NewCIStr("col1"),
			},
		}
		for _, col := range columns {
			if col.Name.O == "col1" {
				require.True(t, selector.Select(col))
			} else {
				require.False(t, selector.Select(col))
			}
		}
	}

	{
		selector := selectors.Get("test3", "t3")
		columns := []*model.ColumnInfo{
			{
				Name: parser_model.NewCIStr("a"),
			},
			{
				Name: parser_model.NewCIStr("col2"),
			},
			{
				Name: parser_model.NewCIStr("col1"),
			},
		}
		for _, col := range columns {
			if col.Name.O == "col1" {
				require.True(t, selector.Select(col))
			} else {
				require.False(t, selector.Select(col))
			}
		}
	}

	{
		selector := selectors.Get("test4", "t4")
		columns := []*model.ColumnInfo{
			{
				Name: parser_model.NewCIStr("a"),
			},
			{
				Name: parser_model.NewCIStr("col2"),
			},
			{
				Name: parser_model.NewCIStr("col1"),
			},
		}
		for _, col := range columns {
			require.True(t, selector.Select(col))
		}
	}
}

func TestVerifyTablesUsesFirstMatchingSelector(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name             string
		firstColumns     []string
		fallbackColumns  []string
		partitionColumns []string
		tables           []string
		wantError        string
	}{
		{
			name:            "later rule filters primary key",
			firstColumns:    []string{"id", "payload"},
			fallbackColumns: []string{"payload"},
			tables:          []string{"t"},
		},
		{
			name:             "later rule filters partition column",
			firstColumns:     []string{"id", "payload"},
			fallbackColumns:  []string{"id"},
			partitionColumns: []string{"payload"},
			tables:           []string{"t"},
		},
		{
			name:            "first rule filters primary key",
			firstColumns:    []string{"payload"},
			fallbackColumns: []string{"id", "payload"},
			tables:          []string{"t"},
			wantError:       "no primary key columns or unique key columns obtained after filter out",
		},
		{
			name:             "first rule filters partition column",
			firstColumns:     []string{"id"},
			fallbackColumns:  []string{"id", "payload"},
			partitionColumns: []string{"payload"},
			tables:           []string{"t"},
			wantError:        "the filtered out column is used in the column dispatcher",
		},
		{
			name:            "next table uses fallback rule",
			firstColumns:    []string{"id", "payload"},
			fallbackColumns: []string{"payload"},
			tables:          []string{"t", "t2"},
			wantError:       "test.t2",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sinkConfig := &config.SinkConfig{
				ColumnSelectors: []*config.ColumnSelector{
					{Matcher: []string{"test.t"}, Columns: tc.firstColumns},
					{Matcher: []string{"test.*"}, Columns: tc.fallbackColumns},
				},
			}
			selectors, err := New(sinkConfig, false)
			require.NoError(t, err)

			var router *eventrouter.EventRouter
			if len(tc.partitionColumns) > 0 {
				sinkConfig.DispatchRules = []*config.DispatchRule{{
					Matcher:       []string{"test.*"},
					PartitionRule: "columns",
					Columns:       tc.partitionColumns,
				}}
				router, err = eventrouter.NewEventRouter(sinkConfig, false, "topic", false, false)
				require.NoError(t, err)
			}

			tables := make([]*commonType.TableInfo, 0, len(tc.tables))
			for _, name := range tc.tables {
				tables = append(tables, commonType.WrapTableInfo("test", &model.TableInfo{
					Name:       parser_model.NewCIStr(name),
					PKIsHandle: true,
					Columns: []*model.ColumnInfo{
						newColumnInfoForSelectorTest(1, "id", mysql.PriKeyFlag|mysql.NotNullFlag),
						newColumnInfoForSelectorTest(2, "payload", 0),
					},
				}))
			}

			err = selectors.VerifyTables(tables, router)
			if tc.wantError == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.True(t, errors.ErrColumnSelectorFailed.Equal(err))
				require.ErrorContains(t, err, tc.wantError)
			}
		})
	}
}

func TestVerifyTablesRequiresFullUniqueKey(t *testing.T) {
	t.Parallel()

	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.ColumnSelectors = []*config.ColumnSelector{
		{
			Matcher: []string{"test.t"},
			Columns: []string{"a"},
		},
	}
	selectors, err := New(replicaConfig.Sink, false)
	require.NoError(t, err)

	tableInfo := commonType.WrapTableInfo("test", &model.TableInfo{
		Name: parser_model.NewCIStr("t"),
		Columns: []*model.ColumnInfo{
			newColumnInfoForSelectorTest(1, "a", mysql.NotNullFlag),
			newColumnInfoForSelectorTest(2, "b", mysql.NotNullFlag),
		},
		Indices: []*model.IndexInfo{
			{
				Name: parser_model.NewCIStr("uk_ab"),
				Columns: []*model.IndexColumn{
					{Name: parser_model.NewCIStr("a"), Offset: 0},
					{Name: parser_model.NewCIStr("b"), Offset: 1},
				},
				Unique: true,
				State:  model.StatePublic,
			},
		},
	})

	err = selectors.VerifyTables([]*commonType.TableInfo{tableInfo}, nil)
	require.Error(t, err)
	require.True(t, errors.ErrColumnSelectorFailed.Equal(err))

	replicaConfig.Sink.ColumnSelectors[0].Columns = []string{"a", "b"}
	selectors, err = New(replicaConfig.Sink, false)
	require.NoError(t, err)
	require.NoError(t, selectors.VerifyTables([]*commonType.TableInfo{tableInfo}, nil))
}

func newColumnInfoForSelectorTest(id int64, name string, flag uint) *model.ColumnInfo {
	ft := types.NewFieldType(mysql.TypeLong)
	ft.AddFlag(flag)
	return &model.ColumnInfo{
		ID:        id,
		Name:      parser_model.NewCIStr(name),
		FieldType: *ft,
		State:     model.StatePublic,
	}
}
