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

package snowflake

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type procedureSpec struct {
	Name string
	File string
}

func TestProcedureBundleLayout(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	require.True(t, ok)

	baseDir := filepath.Dir(currentFile)
	proceduresDir := filepath.Join(baseDir, "procedures")
	deployPath := filepath.Join(baseDir, "deploy", "deploy.sql")

	expected := []procedureSpec{
		{Name: "SP_DISCOVER_CONTROL_FILES", File: "sp_discover_control_files.sql"},
		{Name: "SP_LOAD_DDL_MANIFESTS", File: "sp_load_ddl_manifests.sql"},
		{Name: "SP_REGISTER_SNAPSHOT_TABLES", File: "sp_register_snapshot_tables.sql"},
		{Name: "SP_BOOTSTRAP_ONE_TABLE", File: "sp_bootstrap_one_table.sql"},
		{Name: "SP_BOOTSTRAP_ALL_TABLES", File: "sp_bootstrap_all_tables.sql"},
		{Name: "SP_APPLY_DDL_UP_TO", File: "sp_apply_ddl_up_to.sql"},
		{Name: "SP_APPLY_ONE_DDL", File: "sp_apply_one_ddl.sql"},
		{Name: "SP_ENSURE_RAW_CHANGE_TABLE", File: "sp_ensure_raw_change_table.sql"},
		{Name: "SP_ENSURE_TARGET_TABLE", File: "sp_ensure_target_table.sql"},
		{Name: "SP_SYNC_ONE_TABLE", File: "sp_sync_one_table.sql"},
		{Name: "SP_SYNC_ALL_TABLES", File: "sp_sync_all_tables.sql"},
		{Name: "SP_REBUILD_ONE_TABLE", File: "sp_rebuild_one_table.sql"},
		{Name: "SP_PROCESS_REBUILD_QUEUE", File: "sp_process_rebuild_queue.sql"},
		{Name: "SP_ORCHESTRATE", File: "sp_orchestrate.sql"},
	}

	for _, spec := range expected {
		procedurePath := filepath.Join(proceduresDir, spec.File)
		content, err := os.ReadFile(procedurePath)
		require.NoErrorf(t, err, "missing procedure file: %s", spec.File)

		text := strings.ToUpper(string(content))
		require.Contains(t, text, "CREATE OR REPLACE PROCEDURE TICDC_META."+spec.Name+"(")
		require.Contains(t, text, "LANGUAGE SQL")
		require.Contains(t, text, "EXECUTE AS OWNER")
		require.Contains(t, text, "EXCEPTION")
		require.Contains(t, text, "PROCEDURE_ERROR_LOG")
	}

	deployBytes, err := os.ReadFile(deployPath)
	require.NoError(t, err)
	deployText := string(deployBytes)

	lastIndex := -1
	for _, spec := range expected {
		entry := fmt.Sprintf("!source ../procedures/%s", spec.File)
		index := strings.Index(deployText, entry)
		require.NotEqualf(t, -1, index, "deploy entry missing for %s", spec.Name)
		require.Greaterf(t, index, lastIndex, "deploy order mismatch for %s", spec.Name)
		lastIndex = index
	}
}

func TestControlProceduresConsumeS3Manifests(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	require.True(t, ok)

	baseDir := filepath.Dir(currentFile)
	discoverPath := filepath.Join(baseDir, "procedures", "sp_discover_control_files.sql")
	loadPath := filepath.Join(baseDir, "procedures", "sp_load_ddl_manifests.sql")

	discoverBytes, err := os.ReadFile(discoverPath)
	require.NoError(t, err)
	discoverSQL := strings.ToUpper(string(discoverBytes))
	require.NotContains(t, discoverSQL, "TODO")
	require.Contains(t, discoverSQL, "P_INTEGRATION_ID STRING")
	require.Contains(t, discoverSQL, "DIRECTORY(@TICDC_META.CTL_STAGE)")
	require.Contains(t, discoverSQL, "CREATE TABLE IF NOT EXISTS TICDC_META.INTEGRATION_REGISTRY")
	require.Contains(t, discoverSQL, "MERGE INTO TICDC_META.CONTROL_FILE_REGISTRY")
	require.Contains(t, discoverSQL, "INTEGRATION_ID STRING")
	require.Contains(t, discoverSQL, "ON T.INTEGRATION_ID = S.INTEGRATION_ID AND T.FILE_PATH = S.FILE_PATH")
	require.Contains(t, discoverSQL, "FROM TICDC_META.INTEGRATION_REGISTRY")
	require.Contains(t, discoverSQL, "REGEXP_LIKE")
	require.Contains(t, discoverSQL, "LOAD_STATUS")

	loadBytes, err := os.ReadFile(loadPath)
	require.NoError(t, err)
	loadSQL := strings.ToUpper(string(loadBytes))
	require.NotContains(t, loadSQL, "TODO")
	require.Contains(t, loadSQL, "P_INTEGRATION_ID STRING")
	require.Contains(t, loadSQL, "MERGE INTO TICDC_META.DDL_EVENT_QUEUE")
	require.Contains(t, loadSQL, "INTEGRATION_ID STRING")
	require.Contains(t, loadSQL, "ON T.INTEGRATION_ID = S.INTEGRATION_ID AND T.EVENT_ID = S.EVENT_ID")
	require.Contains(t, loadSQL, "TICDC_META.GLOBAL_CHECKPOINT_STATE")
	require.Contains(t, loadSQL, "ON T.INTEGRATION_ID = S.INTEGRATION_ID")
	require.Contains(t, loadSQL, "METADATA$FILENAME")
	require.Contains(t, loadSQL, "REGEXP_LIKE(METADATA$FILENAME")
	require.Contains(t, loadSQL, "PATTERN => '.*\\\\/CONTROL\\\\/DDL\\\\/.*\\\\.JSON'")
	require.Contains(t, loadSQL, "PATTERN => '.*\\\\/CONTROL\\\\/CHECKPOINT\\\\/GLOBAL\\\\/.*\\\\.JSON'")
}

func TestAllProceduresAreTenantScoped(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	require.True(t, ok)

	baseDir := filepath.Dir(currentFile)
	files, err := filepath.Glob(filepath.Join(baseDir, "procedures", "*.sql"))
	require.NoError(t, err)
	require.NotEmpty(t, files)

	for _, file := range files {
		content, readErr := os.ReadFile(file)
		require.NoError(t, readErr)

		text := strings.ToUpper(string(content))
		require.Containsf(t, text, "P_INTEGRATION_ID", "procedure should be tenant scoped: %s", filepath.Base(file))
		require.NotContainsf(t, text, "TODO", "procedure should be implemented: %s", filepath.Base(file))
	}
}

func TestOrchestratePropagatesIntegrationID(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	require.True(t, ok)

	baseDir := filepath.Dir(currentFile)
	orchestratePath := filepath.Join(baseDir, "procedures", "sp_orchestrate.sql")
	orchestrateBytes, err := os.ReadFile(orchestratePath)
	require.NoError(t, err)

	orchestrateSQL := strings.ToUpper(string(orchestrateBytes))
	require.Contains(t, orchestrateSQL, "SP_ORCHESTRATE(P_INTEGRATION_ID STRING)")
	require.Contains(t, orchestrateSQL, "CALL TICDC_META.SP_DISCOVER_CONTROL_FILES(:P_INTEGRATION_ID)")
	require.Contains(t, orchestrateSQL, "CALL TICDC_META.SP_LOAD_DDL_MANIFESTS(:P_INTEGRATION_ID)")
	require.Contains(t, orchestrateSQL, "CALL TICDC_META.SP_APPLY_DDL_UP_TO(:P_INTEGRATION_ID, :V_UPPER_TS)")
	require.Contains(t, orchestrateSQL, "CALL TICDC_META.SP_SYNC_ALL_TABLES(:P_INTEGRATION_ID, :V_UPPER_TS)")
	require.Contains(t, orchestrateSQL, "CALL TICDC_META.SP_PROCESS_REBUILD_QUEUE(:P_INTEGRATION_ID)")
	require.Contains(t, orchestrateSQL, "WHERE INTEGRATION_ID = :P_INTEGRATION_ID")
}

func TestTenantScopedProceduresFailClosed(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	require.True(t, ok)

	baseDir := filepath.Dir(currentFile)
	for _, file := range []string{
		"sp_discover_control_files.sql",
		"sp_load_ddl_manifests.sql",
	} {
		content, err := os.ReadFile(filepath.Join(baseDir, "procedures", file))
		require.NoError(t, err)
		text := strings.ToUpper(string(content))

		require.Contains(t, text, "IF (V_CONTROL_PREFIX IS NULL OR V_CONTROL_PREFIX = '') THEN")
		require.Contains(t, text, "STARTSWITH")
		require.Contains(t, text, ":V_CONTROL_PREFIX")
		require.NotContains(t, text, "V_CONTROL_PREFIX = ''\n        OR")
		require.NotContains(t, text, "WHERE INTEGRATION_ID = P_INTEGRATION_ID")
	}
}

func TestProcedureBundleCreatesDesignMetadataTables(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	require.True(t, ok)

	baseDir := filepath.Dir(currentFile)
	files, err := filepath.Glob(filepath.Join(baseDir, "procedures", "*.sql"))
	require.NoError(t, err)

	var bundle strings.Builder
	for _, file := range files {
		content, readErr := os.ReadFile(file)
		require.NoError(t, readErr)
		bundle.Write(content)
		bundle.WriteByte('\n')
	}

	text := strings.ToUpper(bundle.String())
	for _, table := range []string{
		"INTEGRATION_REGISTRY",
		"OBJECT_REGISTRY",
		"COLUMN_REGISTRY",
		"INDEX_REGISTRY",
		"CONTROL_FILE_REGISTRY",
		"DDL_EVENT_RAW",
		"DDL_EVENT_QUEUE",
		"GLOBAL_CHECKPOINT_STATE",
		"TABLE_SYNC_STATE",
		"REBUILD_QUEUE",
		"TASK_RUN_LOG",
		"DDL_APPLY_LOG",
		"RESYNC_LOG",
		"PROCEDURE_ERROR_LOG",
	} {
		require.Contains(t, text, "CREATE TABLE IF NOT EXISTS TICDC_META."+table)
	}
}

func TestCoreProceduresImplementBootstrapDDLAndDML(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	require.True(t, ok)

	baseDir := filepath.Dir(currentFile)
	readProcedure := func(name string) string {
		content, err := os.ReadFile(filepath.Join(baseDir, "procedures", name))
		require.NoError(t, err)
		return strings.ToUpper(string(content))
	}

	applyUpToSQL := readProcedure("sp_apply_ddl_up_to.sql")
	require.Contains(t, applyUpToSQL, "CURSOR FOR")
	require.Contains(t, applyUpToSQL, "OPEN C_DDL USING")
	require.Contains(t, applyUpToSQL, "CALL TICDC_META.SP_APPLY_ONE_DDL(:P_INTEGRATION_ID, :V_EVENT_ID)")

	applyOneSQL := readProcedure("sp_apply_one_ddl.sql")
	require.Contains(t, applyOneSQL, "TICDC_META.REBUILD_QUEUE")
	require.Contains(t, applyOneSQL, "TICDC_META.DDL_APPLY_LOG")
	require.Contains(t, applyOneSQL, "EXECUTE IMMEDIATE V_DDL_SQL")
	require.Contains(t, applyOneSQL, "APPLY_STATUS = 'REBUILD_REQUIRED'")
	require.Contains(t, applyOneSQL, "APPLY_STATUS = 'APPLIED'")

	bootstrapSQL := readProcedure("sp_bootstrap_one_table.sql")
	require.Contains(t, bootstrapSQL, "INSERT INTO")
	require.Contains(t, bootstrapSQL, "FROM ' || V_SNAPSHOT_EXTERNAL_TABLE")
	require.Contains(t, bootstrapSQL, "MERGE INTO TICDC_META.TABLE_SYNC_STATE")
	require.Contains(t, bootstrapSQL, "LAST_APPLIED_COMMIT_TS")

	syncSQL := readProcedure("sp_sync_one_table.sql")
	require.Contains(t, syncSQL, "DELETE FROM")
	require.Contains(t, syncSQL, "_TIDB_OLD_ROW_IDENTITY")
	require.Contains(t, syncSQL, "MERGE INTO")
	require.Contains(t, syncSQL, "_TIDB_ROW_IDENTITY")
	require.Contains(t, syncSQL, "TICDC_META.TABLE_SYNC_STATE")

	rebuildSQL := readProcedure("sp_rebuild_one_table.sql")
	require.Contains(t, rebuildSQL, "PAUSED_FOR_REBUILD")
	require.Contains(t, rebuildSQL, "CALL TICDC_META.SP_BOOTSTRAP_ONE_TABLE")
	require.Contains(t, rebuildSQL, "CALL TICDC_META.SP_SYNC_ONE_TABLE")
	require.Contains(t, rebuildSQL, "REBUILD_QUEUE")
}

func TestExceptionLoggingUsesSnowflakeBindings(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	require.True(t, ok)

	baseDir := filepath.Dir(currentFile)
	files, err := filepath.Glob(filepath.Join(baseDir, "procedures", "*.sql"))
	require.NoError(t, err)
	require.NotEmpty(t, files)

	for _, file := range files {
		content, readErr := os.ReadFile(file)
		require.NoError(t, readErr)

		text := strings.ToUpper(string(content))
		require.NotContainsf(t, text, ", SQLSTATE", "exception SQLSTATE should be bound: %s", filepath.Base(file))
		require.NotContainsf(t, text, ", SQLCODE", "exception SQLCODE should be bound: %s", filepath.Base(file))
		require.NotContainsf(t, text, ", SQLERRM", "exception SQLERRM should be bound: %s", filepath.Base(file))
		require.NotContainsf(t, text, "= SQLERRM", "exception SQLERRM should be bound: %s", filepath.Base(file))
		require.NotContainsf(t, text, "\n      SQLERRM;", "exception SQLERRM should be bound: %s", filepath.Base(file))
		require.NotContainsf(t, text, "), V_", "SQL statement fallback variables should be bound: %s", filepath.Base(file))
		require.Containsf(t, text, ":SQLSTATE", "exception SQLSTATE should be logged: %s", filepath.Base(file))
		require.Containsf(t, text, ":SQLCODE", "exception SQLCODE should be logged: %s", filepath.Base(file))
		require.Containsf(t, text, ":SQLERRM", "exception SQLERRM should be logged: %s", filepath.Base(file))
	}
}
