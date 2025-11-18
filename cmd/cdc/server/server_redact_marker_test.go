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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"strings"
	"testing"

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/stretchr/testify/require"
)

// TestMarkerModeExactFormat validates the EXACT format of MARKER mode output
func TestMarkerModeExactFormat(t *testing.T) {
	perrors.RedactLogEnabled.Store(perrors.RedactLogMarker)

	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple string",
			input:    "sensitive",
			expected: "‹sensitive›",
		},
		{
			name:     "credit card",
			input:    "4532-1000-1000-1000",
			expected: "‹4532-1000-1000-1000›",
		},
		{
			name:     "password",
			input:    "Password123!",
			expected: "‹Password123!›",
		},
		{
			name:     "SQL statement",
			input:    "SELECT * FROM users WHERE id = 123",
			expected: "‹SELECT * FROM users WHERE id = 123›",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "‹›",
		},
		{
			name:     "string with markers (should escape)",
			input:    "val‹ue",
			expected: "‹val‹‹ue›", // Inner ‹ should be doubled
		},
		{
			name:     "string with closing marker (should escape)",
			input:    "val›ue",
			expected: "‹val››ue›", // Inner › should be doubled
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test util.RedactValue()
			result := util.RedactValue(tc.input)
			require.Equal(t, tc.expected, result,
				"util.RedactValue() should produce exact format ‹value›")

			// Test util.RedactAny()
			result = util.RedactAny(tc.input)
			require.Equal(t, tc.expected, result,
				"util.RedactAny() should produce exact format ‹value›")

			// Test redact.String() directly
			result = redact.String(perrors.RedactLogMarker, tc.input)
			require.Equal(t, tc.expected, result,
				"redact.String() should produce exact format ‹value›")

			// Verify format properties
			if tc.input != "" && !strings.Contains(tc.input, "‹") && !strings.Contains(tc.input, "›") {
				// For inputs without markers, output should be simple ‹input›
				require.True(t, strings.HasPrefix(result, "‹"), "Should start with ‹")
				require.True(t, strings.HasSuffix(result, "›"), "Should end with ›")
				require.Equal(t, 2, strings.Count(result, "‹")-strings.Count(result, "››")-strings.Count(result, "‹‹")+1,
					"Should have exactly one opening marker (accounting for escapes)")
			}
		})
	}
}

// TestMarkerModeDoesNotBehaveAsOnMode ensures MARKER doesn't degrade to ON mode behavior
func TestMarkerModeDoesNotBehaveAsOnMode(t *testing.T) {
	testValue := "sensitive_data_123"

	// Set MARKER mode
	perrors.RedactLogEnabled.Store(perrors.RedactLogMarker)

	// Get result
	result := util.RedactValue(testValue)

	// MARKER mode should NOT behave like ON mode
	require.NotEqual(t, "?", result, "MARKER mode should NOT return '?' like redact.Value()")
	require.NotEqual(t, "", result, "MARKER mode should NOT return empty string like ON mode")

	// MARKER mode MUST contain the original value
	require.Contains(t, result, testValue, "MARKER mode must preserve original value inside markers")

	// MARKER mode MUST have markers
	require.Contains(t, result, "‹", "MARKER mode must have opening marker")
	require.Contains(t, result, "›", "MARKER mode must have closing marker")

	// Verify exact format
	require.Equal(t, "‹"+testValue+"›", result, "MARKER mode should be exactly ‹value›")
}

// TestMarkerModeWithRedactArgs validates RedactArgs() properly wraps each argument
func TestMarkerModeWithRedactArgs(t *testing.T) {
	perrors.RedactLogEnabled.Store(perrors.RedactLogMarker)

	testCases := []struct {
		name     string
		args     []interface{}
		validate func(t *testing.T, result string)
	}{
		{
			name: "simple args",
			args: []interface{}{"value1", 123, "value2"},
			validate: func(t *testing.T, result string) {
				require.Equal(t, "(‹value1›, ‹123›, ‹value2›)", result)
			},
		},
		{
			name: "sensitive data",
			args: []interface{}{"4532-1000-1000-1000", "Password123!", 999},
			validate: func(t *testing.T, result string) {
				require.Contains(t, result, "‹4532-1000-1000-1000›")
				require.Contains(t, result, "‹Password123!›")
				require.Contains(t, result, "‹999›")
				require.NotContains(t, result, "4532-1000-1000-1000,", "Raw value should not be present without markers")
			},
		},
		{
			name: "nil value",
			args: []interface{}{nil, "test"},
			validate: func(t *testing.T, result string) {
				require.Contains(t, result, "NULL")
				require.Contains(t, result, "‹test›")
			},
		},
		{
			name: "empty args",
			args: []interface{}{},
			validate: func(t *testing.T, result string) {
				require.Equal(t, "()", result)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := util.RedactArgs(tc.args)
			tc.validate(t, result)
		})
	}
}

// TestMarkerModeConsistency ensures all redaction functions behave consistently
func TestMarkerModeConsistency(t *testing.T) {
	perrors.RedactLogEnabled.Store(perrors.RedactLogMarker)

	testValue := "test_data"
	expected := "‹test_data›"

	// All functions should produce identical output for the same input
	resultValue := util.RedactValue(testValue)
	resultAny := util.RedactAny(testValue)
	resultString := redact.String(perrors.RedactLogMarker, testValue)

	require.Equal(t, expected, resultValue, "RedactValue should produce correct format")
	require.Equal(t, expected, resultAny, "RedactAny should produce correct format")
	require.Equal(t, expected, resultString, "redact.String should produce correct format")

	// All should be equal to each other
	require.Equal(t, resultValue, resultAny, "RedactValue and RedactAny should match")
	require.Equal(t, resultValue, resultString, "RedactValue and redact.String should match")
}

// TestMarkerModeEscaping validates that existing markers are properly escaped
func TestMarkerModeEscaping(t *testing.T) {
	perrors.RedactLogEnabled.Store(perrors.RedactLogMarker)

	testCases := []struct {
		input    string
		expected string
	}{
		{
			input:    "‹value›", // Already has markers
			expected: "‹‹‹value›››", // Both should be doubled
		},
		{
			input:    "val‹middle›ue",
			expected: "‹val‹‹middle››ue›",
		},
		{
			input:    "start‹",
			expected: "‹start‹‹›",
		},
		{
			input:    "›end",
			expected: "‹››end›",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			result := util.RedactValue(tc.input)
			require.Equal(t, tc.expected, result,
				"Existing markers should be properly escaped (doubled)")
		})
	}
}

// TestAllModesDistinct ensures OFF, MARKER, and ON modes produce different outputs
func TestAllModesDistinct(t *testing.T) {
	testValue := "sensitive_123"

	// Collect outputs from all three modes
	outputs := make(map[string]string)

	// OFF mode
	perrors.RedactLogEnabled.Store(perrors.RedactLogDisable)
	outputs["OFF"] = util.RedactValue(testValue)

	// MARKER mode
	perrors.RedactLogEnabled.Store(perrors.RedactLogMarker)
	outputs["MARKER"] = util.RedactValue(testValue)

	// ON mode
	perrors.RedactLogEnabled.Store(perrors.RedactLogEnable)
	outputs["ON"] = util.RedactValue(testValue)

	// Verify expected outputs
	require.Equal(t, "sensitive_123", outputs["OFF"], "OFF should return plain value")
	require.Equal(t, "‹sensitive_123›", outputs["MARKER"], "MARKER should wrap with ‹›")
	require.Equal(t, "", outputs["ON"], "ON should return empty string")

	// Verify all three are different
	require.NotEqual(t, outputs["OFF"], outputs["MARKER"], "OFF and MARKER should differ")
	require.NotEqual(t, outputs["OFF"], outputs["ON"], "OFF and ON should differ")
	require.NotEqual(t, outputs["MARKER"], outputs["ON"], "MARKER and ON should differ")
}
