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

package util

import (
	"strings"
	"testing"

	perrors "github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

func TestParseRedactMode(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		// ON variants
		{"ON", perrors.RedactLogEnable},
		{"on", perrors.RedactLogEnable},
		{"TRUE", perrors.RedactLogEnable},
		{"true", perrors.RedactLogEnable},
		{"1", perrors.RedactLogEnable},
		{"ENABLE", perrors.RedactLogEnable},
		{"ENABLED", perrors.RedactLogEnable},
		// OFF variants
		{"OFF", perrors.RedactLogDisable},
		{"off", perrors.RedactLogDisable},
		{"FALSE", perrors.RedactLogDisable},
		{"false", perrors.RedactLogDisable},
		{"0", perrors.RedactLogDisable},
		{"DISABLE", perrors.RedactLogDisable},
		{"DISABLED", perrors.RedactLogDisable},
		{"", perrors.RedactLogDisable},
		// MARKER variants
		{"MARKER", perrors.RedactLogMarker},
		{"marker", perrors.RedactLogMarker},
		{"MARK", perrors.RedactLogMarker},
		// Invalid defaults to OFF
		{"invalid", perrors.RedactLogDisable},
		{"xyz", perrors.RedactLogDisable},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := ParseRedactMode(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestIsValidRedactMode(t *testing.T) {
	tests := []struct {
		input    string
		parsed   string
		expected bool
	}{
		{"ON", perrors.RedactLogEnable, true},
		{"OFF", perrors.RedactLogDisable, true},
		{"MARKER", perrors.RedactLogMarker, true},
		{"TRUE", perrors.RedactLogEnable, true},
		{"FALSE", perrors.RedactLogDisable, true},
		{"1", perrors.RedactLogEnable, true},
		{"0", perrors.RedactLogDisable, true},
		{"MARK", perrors.RedactLogMarker, true},
		{"", perrors.RedactLogDisable, true},
		{"invalid", perrors.RedactLogDisable, false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := IsValidRedactMode(tt.input, tt.parsed)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestValidateAndParseRedactMode(t *testing.T) {
	tests := []struct {
		input       string
		expected    string
		shouldError bool
	}{
		// Valid inputs
		{"on", perrors.RedactLogEnable, false},
		{"ON", perrors.RedactLogEnable, false},
		{"off", perrors.RedactLogDisable, false},
		{"OFF", perrors.RedactLogDisable, false},
		{"marker", perrors.RedactLogMarker, false},
		{"MARKER", perrors.RedactLogMarker, false},
		{"true", perrors.RedactLogEnable, false},
		{"false", perrors.RedactLogDisable, false},
		{"1", perrors.RedactLogEnable, false},
		{"0", perrors.RedactLogDisable, false},
		{"", perrors.RedactLogDisable, false},
		// Invalid inputs - typos and invalid values
		{"maker", "", true},                         // common typo for "marker"
		{"MAKER", "", true},                         // uppercase typo
		{"onnn", "", true},                          // typo for "on"
		{"offf", "", true},                          // typo for "off"
		{"invalid", "", true},                       // completely invalid
		{"xyz", "", true},                           // random string
		{"marks", "", true},                         // similar to "marker" but wrong
		{"enabled", perrors.RedactLogEnable, false}, // valid alias
		{"enable", perrors.RedactLogEnable, false},  // valid alias
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := ValidateAndParseRedactMode(tt.input)
			if tt.shouldError {
				require.Error(t, err, "Expected error for input: %s", tt.input)
				require.Contains(t, err.Error(), "invalid redaction mode")
				require.Contains(t, err.Error(), tt.input)
			} else {
				require.NoError(t, err, "Unexpected error for input: %s", tt.input)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestRedactValue(t *testing.T) {
	testValue := "sensitive-data-123"

	// Test OFF mode
	perrors.RedactLogEnabled.Store(perrors.RedactLogDisable)
	result := RedactValue(testValue)
	require.Equal(t, testValue, result)

	// Test ON mode
	perrors.RedactLogEnabled.Store(perrors.RedactLogEnable)
	result = RedactValue(testValue)
	require.Equal(t, "?", result)

	// Test MARKER mode
	perrors.RedactLogEnabled.Store(perrors.RedactLogMarker)
	result = RedactValue(testValue)
	require.Equal(t, "‹"+testValue+"›", result)

	// Reset to OFF
	perrors.RedactLogEnabled.Store(perrors.RedactLogDisable)
}

func TestRedactAny(t *testing.T) {
	// Test nil
	result := RedactAny(nil)
	require.Equal(t, "NULL", result)

	// Test OFF mode
	perrors.RedactLogEnabled.Store(perrors.RedactLogDisable)
	result = RedactAny("test-string")
	require.Equal(t, "test-string", result)

	result = RedactAny(12345)
	require.Equal(t, "12345", result)

	result = RedactAny([]int{1, 2, 3})
	require.Equal(t, "[1 2 3]", result)

	// Test ON mode
	perrors.RedactLogEnabled.Store(perrors.RedactLogEnable)
	result = RedactAny("test-string")
	require.Equal(t, "?", result)

	result = RedactAny(12345)
	require.Equal(t, "?", result)

	// Test MARKER mode
	perrors.RedactLogEnabled.Store(perrors.RedactLogMarker)
	result = RedactAny("test-string")
	require.Equal(t, "‹test-string›", result)

	result = RedactAny(12345)
	require.Equal(t, "‹12345›", result)

	// Reset to OFF
	perrors.RedactLogEnabled.Store(perrors.RedactLogDisable)
}

func TestRedactArgs(t *testing.T) {
	// Test empty args
	result := RedactArgs(nil)
	require.Equal(t, "()", result)

	result = RedactArgs([]interface{}{})
	require.Equal(t, "()", result)

	// Test OFF mode
	perrors.RedactLogEnabled.Store(perrors.RedactLogDisable)
	args := []interface{}{"value1", 123, nil, "value2"}
	result = RedactArgs(args)
	require.Equal(t, "(value1, 123, NULL, value2)", result)

	// Test ON mode
	perrors.RedactLogEnabled.Store(perrors.RedactLogEnable)
	result = RedactArgs(args)
	require.Equal(t, "(?, ?, NULL, ?)", result)

	// Test MARKER mode
	perrors.RedactLogEnabled.Store(perrors.RedactLogMarker)
	result = RedactArgs(args)
	require.Equal(t, "(‹value1›, ‹123›, NULL, ‹value2›)", result)

	// Reset to OFF
	perrors.RedactLogEnabled.Store(perrors.RedactLogDisable)
}

func TestRedactValueEmpty(t *testing.T) {
	// Test empty string
	perrors.RedactLogEnabled.Store(perrors.RedactLogDisable)
	require.Equal(t, "", RedactValue(""))

	perrors.RedactLogEnabled.Store(perrors.RedactLogEnable)
	require.Equal(t, "?", RedactValue(""))

	perrors.RedactLogEnabled.Store(perrors.RedactLogMarker)
	require.Equal(t, "‹›", RedactValue(""))

	// Reset to OFF
	perrors.RedactLogEnabled.Store(perrors.RedactLogDisable)
}

func TestRedactKey(t *testing.T) {
	testKey := []byte{0x12, 0x34, 0x56, 0xAB, 0xCD}
	expectedHex := "123456ABCD"

	// Test OFF mode - returns uppercase hex string
	perrors.RedactLogEnabled.Store(perrors.RedactLogDisable)
	result := RedactKey(testKey)
	require.Equal(t, expectedHex, result)

	// Test ON mode - returns "?" without hex conversion
	perrors.RedactLogEnabled.Store(perrors.RedactLogEnable)
	result = RedactKey(testKey)
	require.Equal(t, "?", result)

	// Test MARKER mode - returns hex wrapped with ‹›
	perrors.RedactLogEnabled.Store(perrors.RedactLogMarker)
	result = RedactKey(testKey)
	require.Equal(t, "‹"+expectedHex+"›", result)

	// Test empty key
	perrors.RedactLogEnabled.Store(perrors.RedactLogDisable)
	result = RedactKey([]byte{})
	require.Equal(t, "", result)

	perrors.RedactLogEnabled.Store(perrors.RedactLogEnable)
	result = RedactKey([]byte{})
	require.Equal(t, "?", result)

	perrors.RedactLogEnabled.Store(perrors.RedactLogMarker)
	result = RedactKey([]byte{})
	require.Equal(t, "‹›", result)

	// Reset to OFF
	perrors.RedactLogEnabled.Store(perrors.RedactLogDisable)
}

// TestRedactKeyUppercaseHex validates that hex output is uppercase (existing behavior)
func TestRedactKeyUppercaseHex(t *testing.T) {
	testCases := []struct {
		name        string
		key         []byte
		expectedHex string
	}{
		{"lowercase letters in hex", []byte{0xab, 0xcd, 0xef}, "ABCDEF"},
		{"mixed case", []byte{0x1a, 0x2b, 0x3c}, "1A2B3C"},
		{"all uppercase already", []byte{0xAB, 0xCD, 0xEF}, "ABCDEF"},
	}

	perrors.RedactLogEnabled.Store(perrors.RedactLogDisable)
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := RedactKey(tc.key)
			require.Equal(t, tc.expectedHex, result)
			require.Equal(t, strings.ToUpper(result), result, "Hex should be uppercase")
		})
	}
}

// =============================================================================
// Embedded Marker Escaping Tests (MARKER mode only)
// =============================================================================

// TestEmbeddedMarkersEscaping validates that embedded markers are properly escaped.
// TiDB's redact.String() escapes markers by doubling (‹ -> ‹‹, › -> ››).
// This ensures log parsers can correctly identify redacted content boundaries.
func TestEmbeddedMarkersEscaping(t *testing.T) {
	perrors.RedactLogEnabled.Store(perrors.RedactLogMarker)

	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "embedded opening marker",
			input:    "before‹after",
			expected: "‹before‹‹after›", // ‹ escaped to ‹‹
		},
		{
			name:     "embedded closing marker",
			input:    "before›after",
			expected: "‹before››after›", // › escaped to ››
		},
		{
			name:     "embedded marker pair",
			input:    "data‹secret›data",
			expected: "‹data‹‹secret››data›", // both escaped
		},
		{
			name:     "only opening marker",
			input:    "‹",
			expected: "‹‹‹›", // ‹ escaped to ‹‹
		},
		{
			name:     "only closing marker",
			input:    "›",
			expected: "‹›››", // › escaped to ››
		},
		{
			name:     "multiple opening markers",
			input:    "a‹b‹c",
			expected: "‹a‹‹b‹‹c›",
		},
		{
			name:     "multiple closing markers",
			input:    "a›b›c",
			expected: "‹a››b››c›",
		},
		{
			name:     "alternating markers",
			input:    "‹›‹›",
			expected: "‹‹‹››‹‹›››",
		},
		{
			name:     "markers at boundaries",
			input:    "‹start",
			expected: "‹‹‹start›",
		},
		{
			name:     "markers at end",
			input:    "end›",
			expected: "‹end›››",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test RedactValue
			result := RedactValue(tc.input)
			require.Equal(t, tc.expected, result, "RedactValue should escape embedded markers")

			// Test RedactAny
			result = RedactAny(tc.input)
			require.Equal(t, tc.expected, result, "RedactAny should escape embedded markers")
		})
	}

	// Reset to OFF
	perrors.RedactLogEnabled.Store(perrors.RedactLogDisable)
}

// TestEmbeddedMarkersInArgs validates marker escaping works in RedactArgs
func TestEmbeddedMarkersInArgs(t *testing.T) {
	perrors.RedactLogEnabled.Store(perrors.RedactLogMarker)

	args := []interface{}{"normal", "has‹marker", "has›marker"}
	result := RedactArgs(args)

	require.Contains(t, result, "‹normal›", "Normal value should be wrapped")
	require.Contains(t, result, "‹has‹‹marker›", "Opening marker should be escaped")
	require.Contains(t, result, "‹has››marker›", "Closing marker should be escaped")

	// Reset to OFF
	perrors.RedactLogEnabled.Store(perrors.RedactLogDisable)
}

func TestRedactBytes(t *testing.T) {
	testBytes := []byte(`{"password":"secret123","data":"value"}`)
	expectedStr := string(testBytes)

	// Test OFF mode - returns string as-is
	perrors.RedactLogEnabled.Store(perrors.RedactLogDisable)
	result := RedactBytes(testBytes)
	require.Equal(t, expectedStr, result)

	// Test ON mode - returns "?"
	perrors.RedactLogEnabled.Store(perrors.RedactLogEnable)
	result = RedactBytes(testBytes)
	require.Equal(t, "?", result)

	// Test MARKER mode - returns string wrapped with ‹›
	perrors.RedactLogEnabled.Store(perrors.RedactLogMarker)
	result = RedactBytes(testBytes)
	require.Equal(t, "‹"+expectedStr+"›", result)

	// Test empty bytes
	perrors.RedactLogEnabled.Store(perrors.RedactLogDisable)
	result = RedactBytes([]byte{})
	require.Equal(t, "", result)

	perrors.RedactLogEnabled.Store(perrors.RedactLogEnable)
	result = RedactBytes([]byte{})
	require.Equal(t, "?", result)

	perrors.RedactLogEnabled.Store(perrors.RedactLogMarker)
	result = RedactBytes([]byte{})
	require.Equal(t, "‹›", result)

	// Test nil bytes
	perrors.RedactLogEnabled.Store(perrors.RedactLogDisable)
	result = RedactBytes(nil)
	require.Equal(t, "", result)

	// Reset to OFF
	perrors.RedactLogEnabled.Store(perrors.RedactLogDisable)
}

// TestRedactBytesWithEmbeddedMarkers validates marker escaping in byte data
func TestRedactBytesWithEmbeddedMarkers(t *testing.T) {
	perrors.RedactLogEnabled.Store(perrors.RedactLogMarker)

	// Test bytes containing marker characters
	testBytes := []byte("data‹with›markers")
	result := RedactBytes(testBytes)
	require.Equal(t, "‹data‹‹with››markers›", result, "Embedded markers should be escaped")

	// Reset to OFF
	perrors.RedactLogEnabled.Store(perrors.RedactLogDisable)
}

// TestRedactKeyNoEscapingNeeded validates that RedactKey doesn't need marker escaping.
// Keys are converted to uppercase hex first, which eliminates any special characters
// including marker characters - the hex string will only contain 0-9 and A-F.
func TestRedactKeyNoEscapingNeeded(t *testing.T) {
	perrors.RedactLogEnabled.Store(perrors.RedactLogMarker)

	testCases := []struct {
		name        string
		key         []byte
		expectedHex string
	}{
		{
			name:        "simple key",
			key:         []byte{0x12, 0x34, 0x56},
			expectedHex: "123456",
		},
		{
			name:        "key with marker byte values",
			key:         []byte{0xE2, 0x80, 0xB9}, // UTF-8 encoding of ‹
			expectedHex: "E280B9",                 // Just hex, no escaping needed
		},
		{
			name:        "key with closing marker bytes",
			key:         []byte{0xE2, 0x80, 0xBA}, // UTF-8 encoding of ›
			expectedHex: "E280BA",
		},
		{
			name:        "key with all byte values",
			key:         []byte{0x00, 0xFF, 0xAB, 0xCD},
			expectedHex: "00FFABCD",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := RedactKey(tc.key)
			expected := "‹" + tc.expectedHex + "›"
			require.Equal(t, expected, result, "RedactKey should wrap hex with markers, no escaping needed")

			// Verify no doubled markers in result (since hex can't contain markers)
			require.NotContains(t, result, "‹‹", "Should not have escaped opening markers")
			require.NotContains(t, result, "››", "Should not have escaped closing markers")
		})
	}

	// Reset to OFF
	perrors.RedactLogEnabled.Store(perrors.RedactLogDisable)
}
