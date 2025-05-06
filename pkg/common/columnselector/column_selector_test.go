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

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

func TestNewColumnSelector(t *testing.T) {
	// the column selector is not set
	replicaConfig := config.GetDefaultReplicaConfig()
	selectors, err := New(replicaConfig.Sink)
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
	selectors, err = New(replicaConfig.Sink)
	require.NoError(t, err)
	require.Len(t, selectors.selectors, 4)
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
	selectors, err := New(replicaConfig.Sink)
	require.NoError(t, err)

	{
		selector := selectors.Get("test", "t1")
		columns := []*model.ColumnInfo{
			{
				Name: pmodel.NewCIStr("a"),
			},
			{
				Name: pmodel.NewCIStr("b"),
			},
			{
				Name: pmodel.NewCIStr("c"),
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
				Name: pmodel.NewCIStr("a"),
			},
			{
				Name: pmodel.NewCIStr("b"),
			},
			{
				Name: pmodel.NewCIStr("c"),
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
				Name: pmodel.NewCIStr("a"),
			},
			{
				Name: pmodel.NewCIStr("col2"),
			},
			{
				Name: pmodel.NewCIStr("col1"),
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
				Name: pmodel.NewCIStr("a"),
			},
			{
				Name: pmodel.NewCIStr("col2"),
			},
			{
				Name: pmodel.NewCIStr("col1"),
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
				Name: pmodel.NewCIStr("a"),
			},
			{
				Name: pmodel.NewCIStr("col2"),
			},
			{
				Name: pmodel.NewCIStr("col1"),
			},
		}
		for _, col := range columns {
			require.True(t, selector.Select(col))
		}
	}
}
