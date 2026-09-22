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

// withRedactMode is a test helper that sets redaction mode and returns a cleanup function
// that restores the original mode. Use with defer to ensure proper cleanup.
// Example:
//
//	func TestSomething(t *testing.T) {
//	    defer withRedactMode(t, perrors.RedactLogEnable)()
//	    // test code runs with ON mode...
//	}
func withRedactMode(t *testing.T, mode string) func() {
	t.Helper()
	original := perrors.RedactLogEnabled.Load()
	perrors.RedactLogEnabled.Store(mode)
	return func() {
		perrors.RedactLogEnabled.Store(original)
	}
}

func TestParseRedactMode_internal(t *testing.T) {
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
			result := parseRedactMode(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestIsValidRedactMode_internal(t *testing.T) {
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
			result := isValidRedactMode(tt.input, tt.parsed)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestParseRedactMode(t *testing.T) {
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
		// Empty string returns empty (not an error - means "not set")
		{"", "", false},
		// Invalid inputs - typos and invalid values
		{"maker", "", true},   // common typo for "marker"
		{"MAKER", "", true},   // uppercase typo
		{"onnn", "", true},    // typo for "on"
		{"offf", "", true},    // typo for "off"
		{"invalid", "", true}, // completely invalid
		{"xyz", "", true},     // random string
		{"marks", "", true},   // similar to "marker" but wrong
		// Valid aliases
		{"enabled", perrors.RedactLogEnable, false},
		{"enable", perrors.RedactLogEnable, false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := ParseRedactMode(tt.input)
			if tt.shouldError {
				require.Error(t, err, "Expected error for input: %s", tt.input)
				require.Contains(t, err.Error(), "invalid redact mode")
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
	t.Run("OFF mode", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogDisable)()
		result := RedactValue(testValue)
		require.Equal(t, testValue, result)
	})

	// Test ON mode
	t.Run("ON mode", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogEnable)()
		result := RedactValue(testValue)
		require.Equal(t, "?", result)
	})

	// Test MARKER mode
	t.Run("MARKER mode", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogMarker)()
		result := RedactValue(testValue)
		require.Equal(t, "‹"+testValue+"›", result)
	})
}

func TestRedactAny(t *testing.T) {
	// Test nil (mode-independent)
	result := RedactAny(nil)
	require.Equal(t, "NULL", result)

	// Test OFF mode
	t.Run("OFF mode", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogDisable)()
		require.Equal(t, "test-string", RedactAny("test-string"))
		require.Equal(t, "12345", RedactAny(12345))
		require.Equal(t, "[1 2 3]", RedactAny([]int{1, 2, 3}))
	})

	// Test ON mode
	t.Run("ON mode", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogEnable)()
		require.Equal(t, "?", RedactAny("test-string"))
		require.Equal(t, "?", RedactAny(12345))
	})

	// Test MARKER mode
	t.Run("MARKER mode", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogMarker)()
		require.Equal(t, "‹test-string›", RedactAny("test-string"))
		require.Equal(t, "‹12345›", RedactAny(12345))
	})
}

func TestRedactArgs(t *testing.T) {
	// Test empty args (mode-independent)
	require.Equal(t, "()", RedactArgs(nil))
	require.Equal(t, "()", RedactArgs([]interface{}{}))

	args := []interface{}{"value1", 123, nil, "value2"}

	// Test OFF mode
	t.Run("OFF mode", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogDisable)()
		require.Equal(t, "(value1, 123, NULL, value2)", RedactArgs(args))
	})

	// Test ON mode
	t.Run("ON mode", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogEnable)()
		require.Equal(t, "(?, ?, NULL, ?)", RedactArgs(args))
	})

	// Test MARKER mode
	t.Run("MARKER mode", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogMarker)()
		require.Equal(t, "(‹value1›, ‹123›, NULL, ‹value2›)", RedactArgs(args))
	})
}

func TestRedactValueEmpty(t *testing.T) {
	t.Run("OFF mode", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogDisable)()
		require.Equal(t, "", RedactValue(""))
	})

	t.Run("ON mode", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogEnable)()
		require.Equal(t, "?", RedactValue(""))
	})

	t.Run("MARKER mode", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogMarker)()
		require.Equal(t, "‹›", RedactValue(""))
	})
}

func TestRedactKey(t *testing.T) {
	testKey := []byte{0x12, 0x34, 0x56, 0xAB, 0xCD}
	expectedHex := "123456ABCD"

	t.Run("OFF mode", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogDisable)()
		require.Equal(t, expectedHex, RedactKey(testKey))
	})

	t.Run("ON mode", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogEnable)()
		require.Equal(t, "?", RedactKey(testKey))
	})

	t.Run("MARKER mode", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogMarker)()
		require.Equal(t, "‹"+expectedHex+"›", RedactKey(testKey))
	})

	// Test empty key
	t.Run("empty key OFF mode", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogDisable)()
		require.Equal(t, "", RedactKey([]byte{}))
	})

	t.Run("empty key ON mode", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogEnable)()
		require.Equal(t, "?", RedactKey([]byte{}))
	})

	t.Run("empty key MARKER mode", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogMarker)()
		require.Equal(t, "‹›", RedactKey([]byte{}))
	})
}

// TestRedactKeyUppercaseHex validates that hex output is uppercase (existing behavior)
func TestRedactKeyUppercaseHex(t *testing.T) {
	defer withRedactMode(t, perrors.RedactLogDisable)()

	testCases := []struct {
		name        string
		key         []byte
		expectedHex string
	}{
		{"lowercase letters in hex", []byte{0xab, 0xcd, 0xef}, "ABCDEF"},
		{"mixed case", []byte{0x1a, 0x2b, 0x3c}, "1A2B3C"},
		{"all uppercase already", []byte{0xAB, 0xCD, 0xEF}, "ABCDEF"},
	}

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
	defer withRedactMode(t, perrors.RedactLogMarker)()

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
}

// TestEmbeddedMarkersInArgs validates marker escaping works in RedactArgs
func TestEmbeddedMarkersInArgs(t *testing.T) {
	defer withRedactMode(t, perrors.RedactLogMarker)()

	args := []interface{}{"normal", "has‹marker", "has›marker"}
	result := RedactArgs(args)

	require.Contains(t, result, "‹normal›", "Normal value should be wrapped")
	require.Contains(t, result, "‹has‹‹marker›", "Opening marker should be escaped")
	require.Contains(t, result, "‹has››marker›", "Closing marker should be escaped")
}

func TestRedactBytes(t *testing.T) {
	testBytes := []byte(`{"password":"secret123","data":"value"}`)
	expectedStr := string(testBytes)

	t.Run("OFF mode", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogDisable)()
		require.Equal(t, expectedStr, RedactBytes(testBytes))
		require.Equal(t, "", RedactBytes([]byte{}))
		require.Equal(t, "", RedactBytes(nil))
	})

	t.Run("ON mode", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogEnable)()
		require.Equal(t, "?", RedactBytes(testBytes))
		require.Equal(t, "?", RedactBytes([]byte{}))
	})

	t.Run("MARKER mode", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogMarker)()
		require.Equal(t, "‹"+expectedStr+"›", RedactBytes(testBytes))
		require.Equal(t, "‹›", RedactBytes([]byte{}))
	})
}

// TestRedactBytesWithEmbeddedMarkers validates marker escaping in byte data
func TestRedactBytesWithEmbeddedMarkers(t *testing.T) {
	defer withRedactMode(t, perrors.RedactLogMarker)()

	// Test bytes containing marker characters
	testBytes := []byte("data‹with›markers")
	result := RedactBytes(testBytes)
	require.Equal(t, "‹data‹‹with››markers›", result, "Embedded markers should be escaped")
}

// TestRedactKeyNoEscapingNeeded validates that RedactKey doesn't need marker escaping.
// Keys are converted to uppercase hex first, which eliminates any special characters
// including marker characters - the hex string will only contain 0-9 and A-F.
func TestRedactKeyNoEscapingNeeded(t *testing.T) {
	defer withRedactMode(t, perrors.RedactLogMarker)()

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
}

// =============================================================================
// RedactStrings Tests
// =============================================================================

func TestRedactStrings(t *testing.T) {
	args := []string{"value1", "123", "secret-data"}

	t.Run("OFF mode", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogDisable)()
		result := RedactStrings(args)
		require.Equal(t, []string{"value1", "123", "secret-data"}, result)
	})

	t.Run("ON mode", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogEnable)()
		result := RedactStrings(args)
		require.Equal(t, []string{"?", "?", "?"}, result)
	})

	t.Run("MARKER mode", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogMarker)()
		result := RedactStrings(args)
		require.Equal(t, []string{"‹value1›", "‹123›", "‹secret-data›"}, result)
	})
}

func TestRedactStringsNilAndEmpty(t *testing.T) {
	t.Run("nil slice", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogMarker)()
		result := RedactStrings(nil)
		require.Nil(t, result)
	})

	t.Run("empty slice", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogMarker)()
		result := RedactStrings([]string{})
		require.NotNil(t, result)
		require.Empty(t, result)
	})
}

func TestRedactStringsWithEmptyElements(t *testing.T) {
	args := []string{"", "value", ""}

	t.Run("OFF mode with empty strings", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogDisable)()
		result := RedactStrings(args)
		require.Equal(t, []string{"", "value", ""}, result)
	})

	t.Run("ON mode with empty strings", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogEnable)()
		result := RedactStrings(args)
		require.Equal(t, []string{"?", "?", "?"}, result)
	})

	t.Run("MARKER mode with empty strings", func(t *testing.T) {
		defer withRedactMode(t, perrors.RedactLogMarker)()
		result := RedactStrings(args)
		require.Equal(t, []string{"‹›", "‹value›", "‹›"}, result)
	})
}

func TestRedactStringsPreservesLength(t *testing.T) {
	defer withRedactMode(t, perrors.RedactLogEnable)()

	testCases := []struct {
		name     string
		input    []string
		expected int
	}{
		{"single element", []string{"a"}, 1},
		{"three elements", []string{"a", "b", "c"}, 3},
		{"ten elements", []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}, 10},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := RedactStrings(tc.input)
			require.Len(t, result, tc.expected, "RedactStrings should preserve slice length")
		})
	}
}

// TestRedactStringsWithEmbeddedMarkers validates marker escaping in string slices
func TestRedactStringsWithEmbeddedMarkers(t *testing.T) {
	defer withRedactMode(t, perrors.RedactLogMarker)()

	args := []string{"normal", "has‹marker", "has›marker", "both‹and›here"}
	result := RedactStrings(args)

	require.Equal(t, "‹normal›", result[0], "Normal value should be wrapped")
	require.Equal(t, "‹has‹‹marker›", result[1], "Opening marker should be escaped")
	require.Equal(t, "‹has››marker›", result[2], "Closing marker should be escaped")
	require.Equal(t, "‹both‹‹and››here›", result[3], "Both markers should be escaped")
}

// TestRedactStringsSQLComparators validates that MARKER mode values work with SQL comparators.
// Unicode markers ‹› (U+2039, U+203A) are distinct from SQL operators <> (U+003C, U+003E).
func TestRedactStringsSQLComparators(t *testing.T) {
	defer withRedactMode(t, perrors.RedactLogMarker)()

	// Simulate SQL args used with dbutil.ReplacePlaceholder
	args := []string{"1", "2"}
	result := RedactStrings(args)

	require.Equal(t, "‹1›", result[0])
	require.Equal(t, "‹2›", result[1])

	// Verify Unicode markers are different from SQL operators
	// SQL operators: < (U+003C), > (U+003E)
	// Redact markers: ‹ (U+2039), › (U+203A)
	require.NotEqual(t, "<", "‹", "Opening marker should differ from less-than")
	require.NotEqual(t, ">", "›", "Closing marker should differ from greater-than")

	// Verify markers don't contain SQL operators
	require.NotContains(t, result[0], "<", "Result should not contain <")
	require.NotContains(t, result[0], ">", "Result should not contain >")
}

// TestRedactStringsReturnsNewSlice validates that original slice is not modified
func TestRedactStringsReturnsNewSlice(t *testing.T) {
	defer withRedactMode(t, perrors.RedactLogEnable)()

	original := []string{"a", "b", "c"}
	originalCopy := make([]string, len(original))
	copy(originalCopy, original)

	result := RedactStrings(original)

	// Original should be unchanged
	require.Equal(t, originalCopy, original, "Original slice should not be modified")

	// Result should be different (redacted)
	require.NotEqual(t, original, result, "Result should be different from original")
	require.Equal(t, []string{"?", "?", "?"}, result)
}
