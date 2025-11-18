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
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/stretchr/testify/require"
)

func TestRedactModeInit(t *testing.T) {
	tests := []struct {
		name     string
		mode     string
		expected string
	}{
		{"Off mode", "off", perrors.RedactLogDisable},
		{"On mode", "on", perrors.RedactLogEnable},
		{"Marker mode", "marker", perrors.RedactLogMarker},
		{"Empty defaults to Off", "", perrors.RedactLogDisable},
		{"Invalid defaults to Off", "invalid", perrors.RedactLogDisable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the mode
			mode := util.ParseRedactMode(tt.mode)
			require.Equal(t, tt.expected, mode, "util.ParseRedactMode(%q) should return %v", tt.mode, tt.expected)

			// Set and verify
			perrors.RedactLogEnabled.Store(mode)
			require.Equal(t, tt.expected, perrors.RedactLogEnabled.Load(), "RedactLogEnabled.Load() should return %v after Store", tt.expected)
		})
	}
}

func TestRedactFunctions(t *testing.T) {
	// Test with ON mode
	perrors.RedactLogEnabled.Store(perrors.RedactLogEnable)

	sql := "SELECT * FROM users WHERE id = 123"
	redacted := redact.Value(sql)
	require.Equal(t, "?", redacted, "SQL should be redacted to '?'")

	value := "sensitive_data"
	redacted = redact.Value(value)
	require.Equal(t, "?", redacted, "Value should be redacted to '?'")

	key := []byte{0x12, 0x34, 0x56}
	redacted = redact.Key(key)
	require.Equal(t, "?", redacted, "Key should be redacted to '?'")

	// Test with OFF mode
	perrors.RedactLogEnabled.Store(perrors.RedactLogDisable)

	redacted = redact.Value(sql)
	require.Equal(t, sql, redacted, "SQL should not be redacted in OFF mode")

	redacted = redact.Value(value)
	require.Equal(t, value, redacted, "Value should not be redacted in OFF mode")

	redacted = redact.Key(key)
	require.Contains(t, redacted, "12", "Key should not be redacted in OFF mode")

	// Test MARKER mode with redact.String() which properly supports markers
	perrors.RedactLogEnabled.Store(perrors.RedactLogMarker)

	redacted = redact.String(perrors.RedactLogMarker, sql)
	require.Contains(t, redacted, "‹", "SQL should be wrapped with ‹› markers in MARKER mode")
	require.Contains(t, redacted, "›", "SQL should be wrapped with ‹› markers in MARKER mode")
	require.Contains(t, redacted, sql, "Original SQL should be present inside markers")

	redacted = redact.String(perrors.RedactLogMarker, value)
	require.Contains(t, redacted, "‹", "Value should be wrapped with ‹› markers in MARKER mode")
	require.Contains(t, redacted, "›", "Value should be wrapped with ‹› markers in MARKER mode")
	require.Contains(t, redacted, value, "Original value should be present inside markers")

	// Note: TiDB's simple Value() and Key() functions don't support MARKER mode,
	// they only check NeedRedact() and return "?" when enabled.
	// MARKER mode is only supported in String(mode, input) function.
}

func TestOptionsStructure(t *testing.T) {
	o := newOptions()
	require.NotNil(t, o, "newOptions() should not return nil")
	require.NotNil(t, o.serverConfig, "serverConfig should be initialized")
	require.Equal(t, "", o.redactInfoLog, "redactInfoLog should be empty by default")
}

func TestDynamicModeChanges(t *testing.T) {
	// Start with OFF mode
	perrors.RedactLogEnabled.Store(perrors.RedactLogDisable)
	require.Equal(t, perrors.RedactLogDisable, perrors.RedactLogEnabled.Load(), "Should start in OFF mode")

	testData := "sensitive_value"

	// Verify OFF mode behavior
	result := redact.Value(testData)
	require.Equal(t, testData, result, "Data should not be redacted in OFF mode")

	// Switch to ON mode at runtime
	perrors.RedactLogEnabled.Store(perrors.RedactLogEnable)
	require.Equal(t, perrors.RedactLogEnable, perrors.RedactLogEnabled.Load(), "Should switch to ON mode")

	// Verify ON mode behavior takes effect immediately
	result = redact.Value(testData)
	require.Equal(t, "?", result, "Data should be redacted after switching to ON mode")

	// Switch back to OFF mode
	perrors.RedactLogEnabled.Store(perrors.RedactLogDisable)
	require.Equal(t, perrors.RedactLogDisable, perrors.RedactLogEnabled.Load(), "Should switch back to OFF mode")

	result = redact.Value(testData)
	require.Equal(t, testData, result, "Data should not be redacted after switching back to OFF mode")
}

func TestThreadSafeModeChanges(t *testing.T) {
	// Set initial mode
	perrors.RedactLogEnabled.Store(perrors.RedactLogEnable)

	done := make(chan bool, 10)

	// Launch multiple goroutines that read and use the redaction mode
	for i := 0; i < 10; i++ {
		go func() {
			// Each goroutine should see the current mode
			mode := perrors.RedactLogEnabled.Load()
			result := redact.Value("test_data")

			// Verify the mode is consistent
			if mode == perrors.RedactLogEnable {
				require.Equal(t, "?", result, "Goroutine should see ON mode behavior")
			}
			done <- true
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestUtilRedactFunctions(t *testing.T) {
	testValue := "sensitive_data_123"
	testArgs := []interface{}{"arg1", 123, "secret"}

	// Test OFF mode
	perrors.RedactLogEnabled.Store(perrors.RedactLogDisable)
	result := util.RedactValue(testValue)
	require.Equal(t, testValue, result, "RedactValue should not redact in OFF mode")

	result = util.RedactAny(testValue)
	require.Equal(t, testValue, result, "RedactAny should not redact in OFF mode")

	argsResult := util.RedactArgs(testArgs)
	require.Contains(t, argsResult, "arg1", "RedactArgs should not redact in OFF mode")
	require.Contains(t, argsResult, "secret", "RedactArgs should not redact in OFF mode")

	// Test MARKER mode - should wrap with ‹›
	perrors.RedactLogEnabled.Store(perrors.RedactLogMarker)
	result = util.RedactValue(testValue)
	require.Contains(t, result, "‹", "RedactValue should wrap with ‹ in MARKER mode")
	require.Contains(t, result, "›", "RedactValue should wrap with › in MARKER mode")
	require.Contains(t, result, testValue, "RedactValue should contain original value in MARKER mode")

	result = util.RedactAny(testValue)
	require.Contains(t, result, "‹", "RedactAny should wrap with ‹ in MARKER mode")
	require.Contains(t, result, "›", "RedactAny should wrap with › in MARKER mode")

	argsResult = util.RedactArgs(testArgs)
	require.Contains(t, argsResult, "‹", "RedactArgs should wrap values with ‹ in MARKER mode")
	require.Contains(t, argsResult, "›", "RedactArgs should wrap values with › in MARKER mode")

	// Test ON mode - should return empty/placeholder
	perrors.RedactLogEnabled.Store(perrors.RedactLogEnable)
	result = util.RedactValue(testValue)
	require.Equal(t, "", result, "RedactValue should return empty string in ON mode")

	result = util.RedactAny(testValue)
	require.Equal(t, "", result, "RedactAny should return empty string in ON mode")

	argsResult = util.RedactArgs(testArgs)
	require.NotContains(t, argsResult, "arg1", "RedactArgs should fully redact in ON mode")
	require.NotContains(t, argsResult, "secret", "RedactArgs should fully redact in ON mode")
}

func TestBackwardCompatibility(t *testing.T) {
	// Test that empty string defaults to OFF mode (backward compatibility)
	mode := util.ParseRedactMode("")
	require.Equal(t, perrors.RedactLogDisable, mode, "Empty string should default to OFF mode")

	// Test that nil/unset config defaults to OFF mode
	o := newOptions()
	require.Equal(t, "", o.redactInfoLog, "Default redactInfoLog should be empty string")

	// When empty string is parsed, it should result in OFF mode
	parsedMode := util.ParseRedactMode(o.redactInfoLog)
	require.Equal(t, perrors.RedactLogDisable, parsedMode, "Empty redactInfoLog should parse to OFF mode")

	// Verify OFF mode doesn't redact
	perrors.RedactLogEnabled.Store(parsedMode)
	testData := "sensitive_data"
	result := redact.Value(testData)
	require.Equal(t, testData, result, "Default mode should not redact data")
}
