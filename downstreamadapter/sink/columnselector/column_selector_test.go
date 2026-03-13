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
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
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
				Name: ast.NewCIStr("a"),
			},
			{
				Name: ast.NewCIStr("b"),
			},
			{
				Name: ast.NewCIStr("c"),
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
				Name: ast.NewCIStr("a"),
			},
			{
				Name: ast.NewCIStr("b"),
			},
			{
				Name: ast.NewCIStr("c"),
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
				Name: ast.NewCIStr("a"),
			},
			{
				Name: ast.NewCIStr("col2"),
			},
			{
				Name: ast.NewCIStr("col1"),
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
				Name: ast.NewCIStr("a"),
			},
			{
				Name: ast.NewCIStr("col2"),
			},
			{
				Name: ast.NewCIStr("col1"),
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
				Name: ast.NewCIStr("a"),
			},
			{
				Name: ast.NewCIStr("col2"),
			},
			{
				Name: ast.NewCIStr("col1"),
			},
		}
		for _, col := range columns {
			require.True(t, selector.Select(col))
		}
	}
}

func TestVerifyTablesRejectFilteredTopicDispatchColumn(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("create database column_selector_topic")
	helper.Tk().MustExec("use column_selector_topic")
	helper.DDL2Job("create table t (id int primary key, topic_key varchar(64), payload varchar(64))")

	dmlEvent := helper.DML2Event("column_selector_topic", "t",
		"insert into t values (1, 'topic-a', 'payload')")
	tableInfo := dmlEvent.TableInfo

	sinkConfig := &config.SinkConfig{
		DispatchRules: []*config.DispatchRule{
			{
				Matcher:   []string{"column_selector_topic.*"},
				TopicRule: "topic_{column:topic_key}",
			},
		},
		ColumnSelectors: []*config.ColumnSelector{
			{
				Matcher: []string{"column_selector_topic.*"},
				Columns: []string{"id", "payload"},
			},
		},
	}

	selectors, err := New(sinkConfig)
	require.NoError(t, err)
	router, err := eventrouter.NewEventRouter(sinkConfig, "default_topic", false, false)
	require.NoError(t, err)

	err = selectors.VerifyTables([]*common.TableInfo{tableInfo}, router)
	require.ErrorContains(t, err, "used in the topic dispatcher")
}

func TestVerifyTablesRejectFilteredOutboxColumn(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("create database column_selector_outbox")
	helper.Tk().MustExec("use column_selector_outbox")
	helper.DDL2Job("create table t (" +
		"id int primary key, " +
		"aggregate_id varchar(64), " +
		"payload varchar(64), " +
		"event_type varchar(64))")

	dmlEvent := helper.DML2Event("column_selector_outbox", "t",
		"insert into t values (1, 'agg-1', 'payload', 'created')")
	tableInfo := dmlEvent.TableInfo

	protocol := "outbox-json"
	sinkConfig := &config.SinkConfig{
		Protocol: &protocol,
		Outbox: &config.OutboxConfig{
			IDColumn:      "id",
			KeyColumn:     "aggregate_id",
			ValueColumn:   "payload",
			HeaderColumns: map[string]string{"event_type": "event_type"},
		},
		ColumnSelectors: []*config.ColumnSelector{
			{
				Matcher: []string{"column_selector_outbox.*"},
				Columns: []string{"id", "aggregate_id"},
			},
		},
	}

	selectors, err := New(sinkConfig)
	require.NoError(t, err)
	router, err := eventrouter.NewEventRouter(sinkConfig, "default_topic", false, false)
	require.NoError(t, err)

	err = selectors.VerifyTables([]*common.TableInfo{tableInfo}, router)
	require.ErrorContains(t, err, "required by outbox config")
}

func TestVerifyTablesAllowsRetainedTopicAndOutboxColumns(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("create database column_selector_allow")
	helper.Tk().MustExec("use column_selector_allow")
	helper.DDL2Job("create table t (" +
		"id int primary key, " +
		"aggregate_id varchar(64), " +
		"payload varchar(64), " +
		"event_type varchar(64), " +
		"topic_key varchar(64))")

	dmlEvent := helper.DML2Event("column_selector_allow", "t",
		"insert into t values (1, 'agg-1', 'payload', 'created', 'route-a')")
	tableInfo := dmlEvent.TableInfo

	protocol := "outbox-json"
	sinkConfig := &config.SinkConfig{
		Protocol: &protocol,
		Outbox: &config.OutboxConfig{
			IDColumn:      "id",
			KeyColumn:     "aggregate_id",
			ValueColumn:   "payload",
			HeaderColumns: map[string]string{"event_type": "event_type"},
		},
		DispatchRules: []*config.DispatchRule{
			{
				Matcher:   []string{"column_selector_allow.*"},
				TopicRule: "topic_{column:topic_key}",
			},
		},
		ColumnSelectors: []*config.ColumnSelector{
			{
				Matcher: []string{"column_selector_allow.*"},
				Columns: []string{"id", "aggregate_id", "payload", "event_type", "topic_key"},
			},
		},
	}

	selectors, err := New(sinkConfig)
	require.NoError(t, err)
	router, err := eventrouter.NewEventRouter(sinkConfig, "default_topic", false, false)
	require.NoError(t, err)

	require.NoError(t, selectors.VerifyTables([]*common.TableInfo{tableInfo}, router))
}
