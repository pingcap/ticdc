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
	"github.com/stretchr/testify/require"
)

func TestAdminReportsRouteConflict(t *testing.T) {
	admin := newAdminForTest(t, routeAllTo("target", "t"))

	var reportedErr error
	admin.SetErrorReporter(func(err error) {
		reportedErr = err
	})

	info := routeEvent(10, admit("db2", "t", "target", "t"))
	ready, err := admin.Precheck(info)
	require.Error(t, err)
	require.False(t, ready)
	require.Same(t, err, reportedErr)
	require.Contains(t, err.Error(), "table route conflict")
	require.Contains(t, err.Error(), "source `db1`.`t`")
	require.Contains(t, err.Error(), "source `db2`.`t`")
	require.Contains(t, err.Error(), "target `target`.`t`")

	_, ok := admin.activeRoutes[TableKey{Schema: "db2", Table: "t"}]
	require.False(t, ok)
}

func TestAdminMaintainsNameLevelLifecycle(t *testing.T) {
	admin := newAdminForTest(t, routeAllTo("target", "t"))

	source := TableKey{Schema: "db1", Table: "t"}
	binding, ok := admin.activeRoutes[source]
	require.True(t, ok)
	require.Equal(t, TableKey{Schema: "target", Table: "t"}, binding.Target)

	neutral := routeEvent(10)
	ready, err := admin.Precheck(neutral)
	require.NoError(t, err)
	require.True(t, ready)
	require.NoError(t, admin.Apply(neutral))
	_, ok = admin.activeRoutes[source]
	require.True(t, ok)

	rename := routeEvent(20,
		release("db1", "t"),
		admit("db2", "t", "target", "t"),
	)
	ready, err = admin.Precheck(rename)
	require.NoError(t, err)
	require.True(t, ready)
	require.NoError(t, admin.Apply(rename))
	_, ok = admin.activeRoutes[source]
	require.False(t, ok)
	binding, ok = admin.activeRoutes[TableKey{Schema: "db2", Table: "t"}]
	require.True(t, ok)
	require.Equal(t, TableKey{Schema: "target", Table: "t"}, binding.Target)

	dropSchema := routeEvent(30, releaseSchema("db2"))
	ready, err = admin.Precheck(dropSchema)
	require.NoError(t, err)
	require.True(t, ready)
	require.NoError(t, admin.Apply(dropSchema))
	_, ok = admin.activeRoutes[TableKey{Schema: "db2", Table: "t"}]
	require.False(t, ok)

	recreate := routeEvent(40, admit("db1", "t", "target", "t"))
	ready, err = admin.Precheck(recreate)
	require.NoError(t, err)
	require.True(t, ready)
	require.NoError(t, admin.Apply(recreate))
	_, ok = admin.activeRoutes[source]
	require.True(t, ok)
}

func TestAdminSerializesPendingTransitions(t *testing.T) {
	admin := newAdminForTest(t, routeBySource())

	first := routeEvent(10, admit("db2", "t", "db2_target", "t"))
	ready, err := admin.Precheck(first)
	require.NoError(t, err)
	require.True(t, ready)

	second := routeEvent(20, admit("db3", "t", "db3_target", "t"))
	ready, err = admin.Precheck(second)
	require.NoError(t, err)
	require.False(t, ready)

	require.NoError(t, admin.Apply(first))
	ready, err = admin.Precheck(second)
	require.NoError(t, err)
	require.True(t, ready)
	require.NoError(t, admin.Apply(second))

	binding, ok := admin.activeRoutes[TableKey{Schema: "db3", Table: "t"}]
	require.True(t, ok)
	require.Equal(t, TableKey{Schema: "db3_target", Table: "t"}, binding.Target)
}

func newAdminForTest(t *testing.T, rules []*config.DispatchRule) *Admin {
	t.Helper()

	admin, err := NewAdmin(
		common.NewChangeFeedIDWithName("test-changefeed", common.DefaultKeyspaceName),
		&config.ReplicaConfig{
			Sink: &config.SinkConfig{
				DispatchRules: rules,
			},
		},
		nil,
		[]commonEvent.Table{
			{
				SchemaID: 1,
				TableID:  1,
				SchemaTableName: &commonEvent.SchemaTableName{
					SchemaName: "db1",
					TableName:  "t",
				},
			},
		},
	)
	require.NoError(t, err)
	return admin
}

func routeEvent(commitTs uint64, tables ...Admission) AdmissionEvent {
	return AdmissionEvent{CommitTs: commitTs, Tables: tables}
}

func admit(schema, table, targetSchema, targetTable string) Admission {
	return Admission{
		Action:  Admit,
		Binding: NewRouteBinding(schema, table, targetSchema, targetTable),
	}
}

func release(schema, table string) Admission {
	return Admission{
		Action: Release,
		Source: TableKey{Schema: schema, Table: table},
	}
}

func releaseSchema(schema string) Admission {
	return Admission{
		Action: ReleaseSchema,
		Source: TableKey{Schema: schema},
	}
}

func routeAllTo(targetSchema, targetTable string) []*config.DispatchRule {
	return []*config.DispatchRule{{
		Matcher:      []string{"*.*"},
		TargetSchema: targetSchema,
		TargetTable:  targetTable,
	}}
}

func routeBySource() []*config.DispatchRule {
	return []*config.DispatchRule{
		routeExact("db1", "t", "db1_target", "t"),
		routeExact("db2", "t", "db2_target", "t"),
		routeExact("db3", "t", "db3_target", "t"),
	}
}

func routeExact(sourceSchema, sourceTable, targetSchema, targetTable string) *config.DispatchRule {
	return &config.DispatchRule{
		Matcher:      []string{sourceSchema + "." + sourceTable},
		TargetSchema: targetSchema,
		TargetTable:  targetTable,
	}
}
