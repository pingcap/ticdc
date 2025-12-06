// Copyright 2024 PingCAP, Inc.
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

package server

import (
	"testing"

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

// TestServerRedactOptions tests the server options structure for log redaction.
// This validates that the server correctly initializes with default config.
func TestServerRedactOptions(t *testing.T) {
	o := newOptions()
	require.NotNil(t, o, "newOptions() should not return nil")
	require.NotNil(t, o.serverConfig, "serverConfig should be initialized")
	require.NotNil(t, o.serverConfig.Security, "Security config should be initialized")
	// RedactInfoLog in Security config defaults to empty string (OFF mode)
	require.Equal(t, "", o.serverConfig.Security.RedactInfoLog, "RedactInfoLog should be empty by default")
}

// TestServerRedactBackwardCompatibility tests that the server maintains
// backward compatibility when log redaction is not configured.
func TestServerRedactBackwardCompatibility(t *testing.T) {
	// Test that nil/unset config defaults to OFF mode
	o := newOptions()
	require.Equal(t, "", o.serverConfig.Security.RedactInfoLog, "Default RedactInfoLog should be empty string")

	// When empty string is parsed, it should result in OFF mode
	parsedMode := util.ParseRedactMode(o.serverConfig.Security.RedactInfoLog)
	require.Equal(t, perrors.RedactLogDisable, parsedMode, "Empty RedactInfoLog should parse to OFF mode")

	// Verify OFF mode doesn't redact (backward compatible behavior)
	perrors.RedactLogEnabled.Store(parsedMode)
	testData := "sensitive_data"
	result := util.RedactValue(testData)
	require.Equal(t, testData, result, "Default mode should not redact data")
}

// TestServerRedactModeInitialization tests that the server can initialize
// all three redaction modes correctly via the ServerConfig.Security.RedactInfoLog setting.
func TestServerRedactModeInitialization(t *testing.T) {
	tests := []struct {
		name     string
		cfgVal   string
		expected string
	}{
		{"off mode via config", "off", perrors.RedactLogDisable},
		{"on mode via config", "on", perrors.RedactLogEnable},
		{"marker mode via config", "marker", perrors.RedactLogMarker},
		{"empty config defaults to off", "", perrors.RedactLogDisable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create options with custom RedactInfoLog setting
			o := newOptions()
			o.serverConfig.Security.RedactInfoLog = tt.cfgVal

			// Simulate what server.go does when parsing the config
			mode := util.ParseRedactMode(o.serverConfig.Security.RedactInfoLog)
			require.Equal(t, tt.expected, mode)

			// Simulate storing the mode (as done in server initialization)
			perrors.RedactLogEnabled.Store(mode)
			require.Equal(t, tt.expected, perrors.RedactLogEnabled.Load())
		})
	}
}
