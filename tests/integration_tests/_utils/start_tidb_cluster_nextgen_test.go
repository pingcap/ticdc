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

package utils_test

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTiDBServersUseUniqueSlowQueryLogs(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	require.True(t, ok)

	scriptPath := filepath.Join(filepath.Dir(currentFile), "start_tidb_cluster_nextgen")
	content, err := os.ReadFile(scriptPath)
	require.NoError(t, err)

	lines := strings.Split(string(content), "\n")
	commands := make([]string, 0, 5)
	for i := 0; i < len(lines); i++ {
		if strings.TrimSpace(lines[i]) != `tidb-server \` {
			continue
		}

		var command strings.Builder
		for ; i < len(lines); i++ {
			command.WriteString(lines[i])
			command.WriteByte('\n')
			if strings.HasSuffix(strings.TrimSpace(lines[i]), "&") {
				break
			}
		}
		commands = append(commands, command.String())
	}
	require.Len(t, commands, 5)

	slowQueryLogPattern := regexp.MustCompile(`--log-slow-query\s+"([^"]+)"`)
	slowQueryLogs := make(map[string]struct{}, len(commands))
	for _, command := range commands {
		matches := slowQueryLogPattern.FindStringSubmatch(command)
		require.Len(t, matches, 2, "TiDB command must configure a slow-query log:\n%s", command)
		slowQueryLogs[matches[1]] = struct{}{}
	}
	require.Len(t, slowQueryLogs, len(commands), "each TiDB server must use a unique slow-query log")
	require.Equal(t, map[string]struct{}{
		"$OUT_DIR/log/upstream/tidb-system/tidb-slow.log":               {},
		"$OUT_DIR/log/upstream/tidb-$KEYSPACE_NAME/tidb-slow.log":       {},
		"$OUT_DIR/log/upstream/tidb-$KEYSPACE_NAME-other/tidb-slow.log": {},
		"$OUT_DIR/log/downstream/tidb-system/tidb-slow.log":             {},
		"$OUT_DIR/log/downstream/tidb-$KEYSPACE_NAME/tidb-slow.log":     {},
	}, slowQueryLogs)
}
