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
	"github.com/pingcap/ticdc/pkg/security"
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
	// RedactInfoLog is unset by default; the new-architecture server resolves it to ON.
	require.Equal(t, security.RedactInfoLogType(""), o.serverConfig.Security.RedactInfoLog, "RedactInfoLog should be empty by default")
}

// TestServerRedactDefault tests that the new-architecture server enables
// redaction when the setting is not configured.
func TestServerRedactDefault(t *testing.T) {
	o := newOptions()
	require.Equal(t, security.RedactInfoLogType(""), o.serverConfig.Security.RedactInfoLog, "Default RedactInfoLog should be empty string")

	mode, err := o.resolveRedactMode()
	require.NoError(t, err)
	require.Equal(t, perrors.RedactLogEnable, mode)
	originalMode := perrors.RedactLogEnabled.Load()
	t.Cleanup(func() { perrors.RedactLogEnabled.Store(originalMode) })
	perrors.RedactLogEnabled.Store(mode)
	testData := "sensitive_data"
	result := util.RedactValue(testData)
	require.Equal(t, "?", result, "Default mode should redact data")
}

// TestServerRedactModeInitialization tests that the server can initialize
// all three redaction modes correctly via the ServerConfig.Security.RedactInfoLog setting.
func TestServerRedactModeInitialization(t *testing.T) {
	tests := []struct {
		name     string
		cfgVal   security.RedactInfoLogType
		expected string
	}{
		// Canonical values (from TOML/JSON unmarshaling which now canonicalizes)
		{"OFF mode via config", "OFF", perrors.RedactLogDisable},
		{"ON mode via config", "ON", perrors.RedactLogEnable},
		{"MARKER mode via config", "MARKER", perrors.RedactLogMarker},
		{"empty config defaults to on", "", perrors.RedactLogEnable},
	}

	originalMode := perrors.RedactLogEnabled.Load()
	t.Cleanup(func() { perrors.RedactLogEnabled.Store(originalMode) })
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := newOptions()
			o.serverConfig.Security.RedactInfoLog = tt.cfgVal
			mode, err := o.resolveRedactMode()
			require.NoError(t, err)
			require.Equal(t, tt.expected, mode)

			perrors.RedactLogEnabled.Store(mode)
			require.Equal(t, tt.expected, perrors.RedactLogEnabled.Load())
		})
	}
}
