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

package scripts

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateNextGenMetricsRequiresExpectedMarkers(t *testing.T) {
	t.Run("fails when user dashboard misses keyspace suffix", func(t *testing.T) {
		sedCmd := writeSedWrapper(t, "user")
		_, _, output, err := runGenerateNextGenMetrics(t, sedCmd)
		require.Error(t, err)
		require.Contains(t, string(output), "must contain '-KeyspaceName'")
	})

	t.Run("fails when shared dashboard misses sharedpool label", func(t *testing.T) {
		sedCmd := writeSedWrapper(t, "shared")
		_, _, output, err := runGenerateNextGenMetrics(t, sedCmd)
		require.Error(t, err)
		require.Contains(t, string(output), "must contain 'sharedpool_id'")
	})

	t.Run("succeeds with generated markers present", func(t *testing.T) {
		sharedFile, userFile, output, err := runGenerateNextGenMetrics(t, "")
		require.NoError(t, err, string(output))

		userContent, err := os.ReadFile(userFile)
		require.NoError(t, err)
		require.Contains(t, string(userContent), "-KeyspaceName")

		sharedContent, err := os.ReadFile(sharedFile)
		require.NoError(t, err)
		require.Contains(t, string(sharedContent), "sharedpool_id")
	})
}

func runGenerateNextGenMetrics(t *testing.T, sedCmd string) (string, string, []byte, error) {
	t.Helper()

	tempDir := t.TempDir()
	sharedFile := filepath.Join(tempDir, "ticdc_new_arch_next_gen.json")
	userFile := filepath.Join(tempDir, "ticdc_new_arch_with_keyspace_name.json")

	cmd := exec.Command("bash", "./scripts/generate-next-gen-metrics.sh", sharedFile, userFile)
	cmd.Dir = repoRoot(t)
	if sedCmd != "" {
		cmd.Env = append(os.Environ(), "SED_CMD="+sedCmd)
	}

	output, err := cmd.CombinedOutput()
	return sharedFile, userFile, output, err
}

func repoRoot(t *testing.T) string {
	t.Helper()

	wd, err := os.Getwd()
	require.NoError(t, err)

	if _, err := os.Stat(filepath.Join(wd, "scripts", "generate-next-gen-metrics.sh")); err == nil {
		return wd
	}

	if filepath.Base(wd) == "scripts" {
		root := filepath.Dir(wd)
		_, err = os.Stat(filepath.Join(root, "scripts", "generate-next-gen-metrics.sh"))
		require.NoError(t, err)
		return root
	}

	t.Fatalf("failed to locate repository root from %s", wd)
	return ""
}

func writeSedWrapper(t *testing.T, skipMode string) string {
	t.Helper()

	realSed, err := exec.LookPath("sed")
	require.NoError(t, err)

	wrapperPath := filepath.Join(t.TempDir(), "sed-wrapper.sh")
	wrapperContent := fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail

real_sed=%q
skip_mode=%q

if [[ "${1:-}" == "--version" ]]; then
  exec "$real_sed" "$@"
fi

for arg in "$@"; do
  if [[ "$skip_mode" == "user" && "$arg" == *"-KeyspaceName/"* ]]; then
    exit 0
  fi
  if [[ "$skip_mode" == "shared" && "$arg" == *"sharedpool_id"* ]]; then
    exit 0
  fi
done

exec "$real_sed" "$@"
`, realSed, skipMode)
	err = os.WriteFile(wrapperPath, []byte(wrapperContent), 0o755)
	require.NoError(t, err)
	return wrapperPath
}
