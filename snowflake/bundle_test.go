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
