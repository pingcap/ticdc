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
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
)

func TestRunCDCServerConfig(t *testing.T) {
	t.Parallel()

	scriptPath, err := filepath.Abs("run_cdc_server")
	require.NoError(t, err)

	for _, test := range []struct {
		name            string
		config          string
		args            []string
		legacySafePoint string
		expected        string
	}{
		{
			name:     "default",
			expected: "[debug]\nenable-kafka-franz-go = true\n",
		},
		{
			name:     "root configuration",
			config:   "tz = 'Asia/Shanghai'\n",
			expected: "tz = 'Asia/Shanghai'\n[debug]\nenable-kafka-franz-go = true\n",
		},
		{
			name:     "existing debug section",
			config:   "[debug] # test settings\nenable-db-sorter = true\n[security]\nredact-info-log = 'marker'\n",
			expected: "[debug]\nenable-db-sorter = true\nenable-kafka-franz-go = true\n[security]\nredact-info-log = 'marker'\n",
		},
		{
			name:     "nested debug section",
			config:   "[debug.event-service]\nlarge-txn-threshold-in-bytes = 1048576\n",
			expected: "[debug.event-service]\nlarge-txn-threshold-in-bytes = 1048576\n[debug]\nenable-kafka-franz-go = true\n",
		},
		{
			name:     "explicitly enabled",
			config:   "[debug]\nenable-kafka-franz-go = true\n",
			expected: "[debug]\nenable-kafka-franz-go = true\n",
		},
		{
			name:     "explicitly disabled",
			config:   "[debug]\nenable-kafka-franz-go = false\n",
			expected: "[debug]\nenable-kafka-franz-go = false\n",
		},
		{
			name:            "redaction and legacy safepoint",
			args:            []string{"--redact-info-log", "marker"},
			legacySafePoint: "1",
			expected:        "enable-legacy-safepoint = true\n[security]\nredact-info-log = 'marker'\n[debug]\nenable-kafka-franz-go = true\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			workdir := t.TempDir()
			binary := filepath.Join(workdir, "fake-cdc")
			require.NoError(t, os.WriteFile(binary, []byte(`#!/bin/bash
while [ "$#" -gt 0 ]; do
	if [ "$1" = "--config" ]; then
		cp "$2" "${CAPTURED_CONFIG}.tmp"
		mv "${CAPTURED_CONFIG}.tmp" "$CAPTURED_CONFIG"
		exit 0
	fi
	shift
done
exit 1
`), 0o755))
			args := []string{scriptPath, "--workdir", workdir, "--binary", binary, "--skip-ready-check"}
			configPath := filepath.Join(workdir, "custom.toml")
			if test.config != "" {
				require.NoError(t, os.WriteFile(configPath, []byte(test.config), 0o644))
				args = append(args, "--config", configPath)
			}
			args = append(args, test.args...)
			capturedConfig := filepath.Join(workdir, "captured.toml")
			cmd := exec.CommandContext(t.Context(), "bash", args...)
			cmd.Env = append(os.Environ(),
				"TEST_NAME="+t.Name(),
				"OUT_DIR="+workdir,
				"LEGACY_SAFEPOINT="+test.legacySafePoint,
				"CAPTURED_CONFIG="+capturedConfig,
			)
			output, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", output)
			require.Eventually(t, func() bool {
				_, err := os.Stat(capturedConfig)
				return err == nil
			}, 5*time.Second, 10*time.Millisecond, "%s", output)

			var actual, expected map[string]any
			_, err = toml.DecodeFile(capturedConfig, &actual)
			require.NoError(t, err)
			_, err = toml.Decode(test.expected, &expected)
			require.NoError(t, err)
			require.Equal(t, expected, actual)
			if test.config != "" {
				original, err := os.ReadFile(configPath)
				require.NoError(t, err)
				require.Equal(t, test.config, string(original))
			}
		})
	}
}
