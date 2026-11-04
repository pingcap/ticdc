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

package schemastore

import (
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/routing"
	"github.com/pingcap/ticdc/pkg/sqlname"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestCreateViewCanonicalSourceRouting(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	tk := helper.Tk()
	storage := newPersistentStorageForTest(t.TempDir(), nil)
	defer storage.close()
	storage.kvStorage = helper.Storage()
	for _, query := range []string{
		"CREATE DATABASE source_db",
		"CREATE TABLE source_db.orders (id INT PRIMARY KEY)",
		"CREATE VIEW source_db.base_view AS SELECT id FROM source_db.orders",
	} {
		require.NoError(t, storage.handleDDLJob(helper.DDL2Job(query)))
	}
	dml := helper.DML2Event("source_db", "orders", "INSERT INTO source_db.orders VALUES (7)")
	tk.MustExec("CREATE DATABASE target_db")
	tk.MustExec("CREATE TABLE target_db.orders_r (id INT PRIMARY KEY)")
	tk.MustExec("INSERT INTO target_db.orders_r VALUES (7)")
	tk.MustExec("CREATE VIEW target_db.base_view_r AS SELECT id FROM target_db.orders_r")
	for _, tc := range []struct{ name, selectSQL string }{
		{"column", "SELECT ORDERS.id FROM SOURCE_DB.ORDERS"},
		{"qualified column", "SELECT SOURCE_DB.ORDERS.id FROM source_db.ORDERS"},
		{"wildcard", "SELECT SOURCE_DB.ORDERS.* FROM SOURCE_DB.ORDERS"},
		{"alias", "SELECT o.id FROM SOURCE_DB.ORDERS AS o"},
		{"CTE", "WITH ORDERS AS (SELECT ORDERS.id FROM SOURCE_DB.ORDERS) SELECT ORDERS.id FROM ORDERS"},
		{"CTE absent from catalog", "WITH selected_users AS (SELECT ORDERS.id FROM SOURCE_DB.ORDERS) SELECT id FROM selected_users"},
		{"view dependency", "SELECT BASE_VIEW.id FROM SOURCE_DB.BASE_VIEW"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := helper.DDL2Job("CREATE VIEW source_db.v AS " + tc.selectSQL)
			tk.MustQuery("SELECT * FROM source_db.v").Check(testkit.Rows("7"))
			require.NoError(t, storage.handleDDLJob(job))
			snap := storage.db.NewSnapshot()
			raw := readPersistedDDLEvent(snap, job.BinlogInfo.FinishedTS)
			require.NoError(t, snap.Close())
			ddl, ok, err := buildDDLEvent(&raw, nil, common.DDLSpanTableID)
			require.NoError(t, err)
			require.True(t, ok)
			// The persisted normalization is useful with routing disabled too.
			tk.MustExec("DROP VIEW source_db.v")
			tk.MustExec(ddl.Query)
			tk.MustQuery("SELECT * FROM source_db.v").Check(testkit.Rows("7"))
			for _, sensitive := range []bool{true, false} {
				router, err := routing.NewRouter(common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName), sensitive, []*config.DispatchRule{{
					Matcher: []string{"source_db.orders", "source_db.base_view", "source_db.v"}, TargetSchema: "target_db", TargetTable: "{table}_r",
				}})
				require.NoError(t, err)
				table, err := router.ApplyToTableInfo(dml.TableInfo)
				require.NoError(t, err)
				require.Equal(t, "target_db", table.GetTargetSchemaName())
				require.Equal(t, "orders_r", table.GetTargetTableName())
				routed, err := router.ApplyToDDLEvent(&ddl)
				require.NoError(t, err)
				require.NotContains(t, routed.Query, "`source_db`.")
				tk.MustExec(routed.Query)
				tk.MustQuery("SELECT * FROM target_db.v_r").Check(testkit.Rows("7"))
				tk.MustExec("DROP VIEW target_db.v_r")
			}
			tk.MustExec("DROP VIEW source_db.v")
		})
	}
}

func TestViewSourceUsesDDLSnapshot(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	path := t.TempDir()
	storage := newPersistentStorageForTest(path, nil)
	storage.kvStorage = helper.Storage()
	for _, query := range []string{"CREATE DATABASE source_db", "CREATE TABLE source_db.orders (id INT PRIMARY KEY)"} {
		require.NoError(t, storage.handleDDLJob(helper.DDL2Job(query)))
	}
	job := helper.DDL2Job("CREATE VIEW source_db.v AS SELECT ORDERS.id FROM SOURCE_DB.ORDERS")
	// The latest catalog no longer has either the view or its dependency.
	// Persisting this older DDL must still resolve its original names.
	helper.DDL2Job("DROP VIEW source_db.v")
	helper.DDL2Job("DROP TABLE source_db.orders")
	require.NoError(t, storage.handleDDLJob(job))
	snap := storage.db.NewSnapshot()
	raw := readPersistedDDLEvent(snap, job.BinlogInfo.FinishedTS)
	require.NoError(t, snap.Close())
	require.Contains(t, raw.Query, "SELECT `source_db`.`orders`.`id`")
	require.Contains(t, raw.Query, "FROM `source_db`.`orders`")
	require.NoError(t, storage.close())
	// Replay consumes the persisted canonical SQL without any upstream lookup.
	reopened := loadPersistentStorageFromPathForTest(path, job.BinlogInfo.FinishedTS)
	defer reopened.close()
	snap = reopened.db.NewSnapshot()
	replayed := readPersistedDDLEvent(snap, job.BinlogInfo.FinishedTS)
	require.NoError(t, snap.Close())
	require.Equal(t, raw.Query, replayed.Query)
}

func TestViewTableResolverMissingSource(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	helper.DDL2Job("CREATE DATABASE source_db")
	resolve := newViewTableResolver(helper.GetCurrentMeta())
	_, err := resolve(sqlname.Name{Schema: "source_db", Table: "missing"})
	require.ErrorContains(t, err, "absent from the DDL snapshot")
	_, err = resolve(sqlname.Name{Schema: "missing", Table: "t"})
	require.ErrorContains(t, err, "absent from the DDL snapshot")
}

func TestViewNormalizationFailureDoesNotPersist(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	storage := newPersistentStorageForTest(t.TempDir(), nil)
	defer storage.close()
	storage.kvStorage = helper.Storage()
	for _, query := range []string{"CREATE DATABASE source_db", "CREATE TABLE source_db.orders (id INT PRIMARY KEY)"} {
		require.NoError(t, storage.handleDDLJob(helper.DDL2Job(query)))
	}
	job := helper.DDL2Job("CREATE VIEW source_db.v AS SELECT id FROM source_db.orders")
	originalSelect := job.BinlogInfo.TableInfo.View.SelectStmt
	job.BinlogInfo.TableInfo.View.SelectStmt = "SELECT id FROM source_db.missing"
	historyLen := len(storage.tableTriggerDDLHistory)
	require.ErrorContains(t, storage.handleDDLJob(job), "absent from the DDL snapshot")
	require.Len(t, storage.tableTriggerDDLHistory, historyLen)
	key, err := ddlJobKey(job.BinlogInfo.FinishedTS)
	require.NoError(t, err)
	_, closer, err := storage.db.Get(key)
	if closer != nil {
		require.NoError(t, closer.Close())
	}
	require.ErrorIs(t, err, pebble.ErrNotFound)
	// Retry the same DDL once the input is corrected. No partial state remains.
	job.BinlogInfo.TableInfo.View.SelectStmt = originalSelect
	require.NoError(t, storage.handleDDLJob(job))
	require.Len(t, storage.tableTriggerDDLHistory, historyLen+1)
}
