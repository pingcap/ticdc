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

	ready, err := admin.Precheck(10, []Admission{admit("db2", "t", "target", "t")})
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

	ready, err := admin.Precheck(10, nil)
	require.NoError(t, err)
	require.True(t, ready)
	require.NoError(t, admin.Apply(10, nil))
	_, ok = admin.activeRoutes[source]
	require.True(t, ok)

	rename := []Admission{
		release("db1", "t"),
		admit("db2", "t", "target", "t"),
	}
	ready, err = admin.Precheck(20, rename)
	require.NoError(t, err)
	require.True(t, ready)
	require.NoError(t, admin.Apply(20, rename))
	_, ok = admin.activeRoutes[source]
	require.False(t, ok)
	binding, ok = admin.activeRoutes[TableKey{Schema: "db2", Table: "t"}]
	require.True(t, ok)
	require.Equal(t, TableKey{Schema: "target", Table: "t"}, binding.Target)

	ready, err = admin.Precheck(30, []Admission{releaseSchema("db2")})
	require.NoError(t, err)
	require.True(t, ready)
	require.NoError(t, admin.Apply(30, []Admission{releaseSchema("db2")}))
	_, ok = admin.activeRoutes[TableKey{Schema: "db2", Table: "t"}]
	require.False(t, ok)

	ready, err = admin.Precheck(40, []Admission{admit("db1", "t", "target", "t")})
	require.NoError(t, err)
	require.True(t, ready)
	require.NoError(t, admin.Apply(40, []Admission{admit("db1", "t", "target", "t")}))
	_, ok = admin.activeRoutes[source]
	require.True(t, ok)
}

func TestAdminSerializesPendingTransitions(t *testing.T) {
	admin := newAdminForTest(t, routeBySource())

	first := admit("db2", "t", "db2_target", "t")
	ready, err := admin.Precheck(10, []Admission{first})
	require.NoError(t, err)
	require.True(t, ready)

	second := admit("db3", "t", "db3_target", "t")
	ready, err = admin.Precheck(20, []Admission{second})
	require.NoError(t, err)
	require.False(t, ready)

	require.NoError(t, admin.Apply(10, []Admission{first}))
	ready, err = admin.Precheck(20, []Admission{second})
	require.NoError(t, err)
	require.True(t, ready)
	require.NoError(t, admin.Apply(20, []Admission{second}))

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
