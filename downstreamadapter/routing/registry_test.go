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

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
)

func makeBinding(logicalID, replicaID, schemaID int64, sourceSchema, sourceTable, targetSchema, targetTable string, ruleIndex int, matcher []string) RouteBinding {
	return RouteBinding{
		Source: SourceKey{
			LogicalTableID: logicalID,
			Schema:         sourceSchema,
			Table:          sourceTable,
		},
		ReplicaTableID: replicaID,
		SourceSchemaID: schemaID,
		Target: TargetKey{
			Schema: targetSchema,
			Table:  targetTable,
		},
		RuleIndex: ruleIndex,
		Matcher:   matcher,
	}
}

func TestNewTargetTableRegistry(t *testing.T) {
	t.Parallel()

	t.Run("empty bindings", func(t *testing.T) {
		t.Parallel()
		r, err := NewTargetTableRegistry(nil)
		require.NoError(t, err)
		require.NotNil(t, r)
		require.Empty(t, r.Snapshot())
	})

	t.Run("non-conflicting bindings", func(t *testing.T) {
		t.Parallel()
		bindings := []RouteBinding{
			makeBinding(1, 1, 10, "db1", "t1", "db1", "t1", -1, nil),
			makeBinding(2, 2, 10, "db1", "t2", "archive", "t2", 0, []string{"db1.*"}),
		}
		r, err := NewTargetTableRegistry(bindings)
		require.NoError(t, err)
		require.Len(t, r.Snapshot(), 2)
	})

	t.Run("conflicting bindings fail", func(t *testing.T) {
		t.Parallel()
		bindings := []RouteBinding{
			makeBinding(1, 1, 10, "db1", "t1", "archive", "orders", 0, []string{"db1.*"}),
			makeBinding(2, 2, 20, "db2", "t1", "archive", "orders", 0, []string{"db2.*"}),
		}
		_, err := NewTargetTableRegistry(bindings)
		require.Error(t, err)
		require.True(t, errors.ErrTableRouteConflict.Equal(err))
	})

	t.Run("same source multi-replica allowed", func(t *testing.T) {
		t.Parallel()
		// Same logical table split into multiple physical table spans
		bindings := []RouteBinding{
			makeBinding(1, 100, 10, "db1", "t1", "db1", "t1", -1, nil),
			makeBinding(1, 101, 10, "db1", "t1", "db1", "t1", -1, nil),
		}
		r, err := NewTargetTableRegistry(bindings)
		require.NoError(t, err)
		require.Len(t, r.Snapshot(), 2)
	})
}

func TestTargetTableRegistryValidateAdd(t *testing.T) {
	t.Parallel()

	r, err := NewTargetTableRegistry([]RouteBinding{
		makeBinding(1, 1, 10, "db1", "t1", "archive", "orders", 0, []string{"db1.*"}),
	})
	require.NoError(t, err)

	t.Run("add new target succeeds", func(t *testing.T) {
		t.Parallel()
		err := r.ValidateAdd(makeBinding(2, 2, 20, "db2", "t2", "archive", "customers", 0, []string{"db2.*"}))
		require.NoError(t, err)
	})

	t.Run("add same source to same target succeeds", func(t *testing.T) {
		t.Parallel()
		err := r.ValidateAdd(makeBinding(1, 3, 10, "db1", "t1", "archive", "orders", 0, []string{"db1.*"}))
		require.NoError(t, err)
	})

	t.Run("add different source to existing target fails", func(t *testing.T) {
		t.Parallel()
		err := r.ValidateAdd(makeBinding(2, 2, 20, "db2", "t2", "archive", "orders", 0, []string{"db2.*"}))
		require.Error(t, err)
		require.True(t, errors.ErrTableRouteConflict.Equal(err))
	})
}

func TestTargetTableRegistryUpsert(t *testing.T) {
	t.Parallel()

	t.Run("insert new binding", func(t *testing.T) {
		t.Parallel()
		r, _ := NewTargetTableRegistry(nil)
		err := r.Upsert(makeBinding(1, 1, 10, "db1", "t1", "archive", "orders", 0, nil))
		require.NoError(t, err)
		require.Len(t, r.Snapshot(), 1)
	})

	t.Run("upsert same replica replaces old", func(t *testing.T) {
		t.Parallel()
		r, _ := NewTargetTableRegistry([]RouteBinding{
			makeBinding(1, 1, 10, "db1", "t1", "archive", "orders", 0, nil),
		})
		err := r.Upsert(makeBinding(1, 1, 10, "db1", "t1", "archive", "orders_v2", 1, []string{"db1.*"}))
		require.NoError(t, err)
		snapshot := r.Snapshot()
		require.Len(t, snapshot, 1)
		require.Equal(t, "orders_v2", snapshot[0].Target.Table)
	})

	t.Run("upsert different source to same target fails", func(t *testing.T) {
		t.Parallel()
		r, _ := NewTargetTableRegistry([]RouteBinding{
			makeBinding(1, 1, 10, "db1", "t1", "archive", "orders", 0, nil),
		})
		err := r.Upsert(makeBinding(2, 2, 20, "db2", "t1", "archive", "orders", 0, nil))
		require.Error(t, err)
		require.True(t, errors.ErrTableRouteConflict.Equal(err))
	})
}

func TestTargetTableRegistryRemoveByReplicaID(t *testing.T) {
	t.Parallel()

	r, _ := NewTargetTableRegistry([]RouteBinding{
		makeBinding(1, 1, 10, "db1", "t1", "archive", "orders", 0, nil),
		makeBinding(2, 2, 10, "db1", "t2", "archive", "customers", 0, nil),
	})

	r.RemoveByReplicaID(1)
	require.Len(t, r.Snapshot(), 1)
	require.Equal(t, int64(2), r.Snapshot()[0].ReplicaTableID)

	// Removing again is a no-op.
	r.RemoveByReplicaID(1)
	require.Len(t, r.Snapshot(), 1)
}

func TestTargetTableRegistryRemoveBySchemaID(t *testing.T) {
	t.Parallel()

	r, _ := NewTargetTableRegistry([]RouteBinding{
		makeBinding(1, 1, 10, "db1", "t1", "archive", "orders", 0, nil),
		makeBinding(2, 2, 10, "db1", "t2", "archive", "customers", 0, nil),
		makeBinding(3, 3, 20, "db2", "t3", "db2", "t3", -1, nil),
	})

	r.RemoveBySchemaID(10)
	snapshot := r.Snapshot()
	require.Len(t, snapshot, 1)
	require.Equal(t, int64(20), snapshot[0].SourceSchemaID)
}

func TestTargetTableRegistryReplace(t *testing.T) {
	t.Parallel()

	t.Run("remove and add succeeds", func(t *testing.T) {
		t.Parallel()
		r, _ := NewTargetTableRegistry([]RouteBinding{
			makeBinding(1, 1, 10, "db1", "t1", "archive", "orders", 0, nil),
		})
		err := r.Replace([]int64{1}, []RouteBinding{
			makeBinding(2, 2, 20, "db2", "t2", "archive", "orders", 0, nil),
		})
		require.NoError(t, err)
		snapshot := r.Snapshot()
		require.Len(t, snapshot, 1)
		require.Equal(t, int64(2), snapshot[0].Source.LogicalTableID)
	})

	t.Run("remove same source add same target succeeds", func(t *testing.T) {
		t.Parallel()
		r, _ := NewTargetTableRegistry([]RouteBinding{
			makeBinding(1, 1, 10, "db1", "t1", "archive", "orders", 0, nil),
		})
		err := r.Replace([]int64{1}, []RouteBinding{
			makeBinding(1, 2, 10, "db1", "t1", "archive", "orders", 0, nil),
		})
		require.NoError(t, err)
		require.Len(t, r.Snapshot(), 1)
	})

	t.Run("add conflict fails and registry is unchanged", func(t *testing.T) {
		t.Parallel()
		r, _ := NewTargetTableRegistry([]RouteBinding{
			makeBinding(1, 1, 10, "db1", "t1", "archive", "orders", 0, nil),
			makeBinding(2, 2, 10, "db1", "t2", "archive", "orders_v2", 0, nil),
		})
		original := r.Snapshot()
		err := r.Replace([]int64{1}, []RouteBinding{
			makeBinding(3, 3, 20, "db2", "t3", "archive", "orders_v2", 0, nil),
		})
		require.Error(t, err)
		require.True(t, errors.ErrTableRouteConflict.Equal(err))
		require.Equal(t, original, r.Snapshot())
	})

	t.Run("remove non-existing id is no-op", func(t *testing.T) {
		t.Parallel()
		r, _ := NewTargetTableRegistry([]RouteBinding{
			makeBinding(1, 1, 10, "db1", "t1", "archive", "orders", 0, nil),
		})
		err := r.Replace([]int64{999}, []RouteBinding{
			makeBinding(2, 2, 20, "db2", "t2", "archive", "customers", 0, nil),
		})
		require.NoError(t, err)
		require.Len(t, r.Snapshot(), 2)
	})
}

func TestTargetTableRegistryValidateReplace(t *testing.T) {
	t.Parallel()

	r, _ := NewTargetTableRegistry([]RouteBinding{
		makeBinding(1, 1, 10, "db1", "t1", "archive", "orders", 0, nil),
	})

	// ValidateReplace must not mutate the registry.
	original := r.Snapshot()

	err := r.ValidateReplace([]int64{1}, []RouteBinding{
		makeBinding(2, 2, 20, "db2", "t2", "archive", "orders", 0, nil),
	})
	require.NoError(t, err)
	require.Equal(t, original, r.Snapshot())

	// Conflict via ValidateReplace also does not mutate.
	err = r.ValidateReplace(nil, []RouteBinding{
		makeBinding(2, 2, 20, "db2", "t2", "archive", "orders", 0, nil),
	})
	require.Error(t, err)
	require.True(t, errors.ErrTableRouteConflict.Equal(err))
	require.Equal(t, original, r.Snapshot())
}

func TestTargetTableRegistrySnapshot(t *testing.T) {
	t.Parallel()

	r, _ := NewTargetTableRegistry([]RouteBinding{
		makeBinding(2, 2, 20, "db2", "t2", "archive", "customers", 0, nil),
		makeBinding(1, 1, 10, "db1", "t1", "archive", "orders", 0, nil),
	})

	snapshot := r.Snapshot()
	require.Len(t, snapshot, 2)
	// Snapshot should be sorted by target.
	require.Equal(t, "archive", snapshot[0].Target.Schema)
	// "customers" < "orders" lexicographically
	require.Equal(t, "customers", snapshot[0].Target.Table)
	require.Equal(t, "orders", snapshot[1].Target.Table)
}
