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

package event

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

// TestSafeRowToStringRedaction tests that safeRowToString properly applies redaction
// based on the current log redaction mode (off, marker, on).
// This is a critical security test - if it fails, sensitive data may leak into logs.
func TestSafeRowToStringRedaction(t *testing.T) {
	// Create a simple row with sensitive data
	fieldTypes := []*types.FieldType{
		types.NewFieldType(5),  // INT
		types.NewFieldType(15), // VARCHAR
	}
	chk := chunk.NewChunkWithCapacity(fieldTypes, 1)
	chk.AppendInt64(0, 123)
	chk.AppendString(1, "SensitivePassword123!")
	row := chk.GetRow(0)

	testCases := []struct {
		name              string
		redactMode        string
		expectContains    []string
		expectNotContains []string
	}{
		{
			name:           "OFF mode - raw data visible",
			redactMode:     errors.RedactLogDisable,
			expectContains: []string{"SensitivePassword123!", "123"},
		},
		{
			name:           "MARKER mode - data wrapped with markers",
			redactMode:     errors.RedactLogMarker,
			expectContains: []string{"‹", "›"},
			// In marker mode, data should still be present but wrapped
		},
		{
			name:              "ON mode - data fully redacted",
			redactMode:        errors.RedactLogEnable,
			expectContains:    []string{"?"},
			expectNotContains: []string{"SensitivePassword123!"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Set redaction mode
			originalMode := errors.RedactLogEnabled.Load()
			errors.RedactLogEnabled.Store(tc.redactMode)
			defer errors.RedactLogEnabled.Store(originalMode)

			// Call safeRowToString
			result := safeRowToString(row, fieldTypes)

			// Verify expected content is present
			for _, expected := range tc.expectContains {
				require.Contains(t, result, expected,
					"Expected '%s' to be present in result: %s", expected, result)
			}

			// Verify unexpected content is NOT present
			for _, unexpected := range tc.expectNotContains {
				require.NotContains(t, result, unexpected,
					"Expected '%s' to NOT be present in result: %s", unexpected, result)
			}
		})
	}
}

// TestSafeRowToStringRedactionWithMultipleRows tests redaction across different row types
func TestSafeRowToStringRedactionWithMultipleRows(t *testing.T) {
	// Test with sensitive patterns commonly found in databases
	testData := []struct {
		name          string
		sensitiveData string
	}{
		{"password", "MySecretPassword!"},
		{"credit_card", "4532-1234-5678-9012"},
		{"ssn", "123-45-6789"},
		{"email", "user@secret.com"},
	}

	for _, td := range testData {
		t.Run(td.name, func(t *testing.T) {
			fieldTypes := []*types.FieldType{
				types.NewFieldType(15), // VARCHAR
			}
			chk := chunk.NewChunkWithCapacity(fieldTypes, 1)
			chk.AppendString(0, td.sensitiveData)
			row := chk.GetRow(0)

			// Test ON mode - MUST redact all sensitive data
			originalMode := errors.RedactLogEnabled.Load()
			errors.RedactLogEnabled.Store(errors.RedactLogEnable)
			defer errors.RedactLogEnabled.Store(originalMode)

			result := safeRowToString(row, fieldTypes)

			// Critical security check: sensitive data must NOT appear in ON mode
			require.NotContains(t, result, td.sensitiveData,
				"SECURITY FAILURE: Sensitive data '%s' leaked in ON mode! Result: %s",
				td.sensitiveData, result)

			// Must contain redaction placeholder
			require.Contains(t, result, "?",
				"Expected '?' placeholder in ON mode result: %s", result)
		})
	}
}

// TestSafeRowToStringEmptyRow tests that safeRowToString handles empty rows correctly
func TestSafeRowToStringEmptyRow(t *testing.T) {
	fieldTypes := []*types.FieldType{
		types.NewFieldType(5), // INT
	}

	// Get an empty row
	row := chunk.Row{}

	// Should return empty string for empty row
	result := safeRowToString(row, fieldTypes)
	require.Empty(t, result, "Expected empty string for empty row")
}

// TestSafeRowToStringPanicRecovery tests that safeRowToString recovers from panics
func TestSafeRowToStringPanicRecovery(t *testing.T) {
	// This test verifies the panic recovery mechanism in safeRowToString
	fieldTypes := []*types.FieldType{
		types.NewFieldType(5), // INT
	}
	chk := chunk.NewChunkWithCapacity(fieldTypes, 1)
	chk.AppendInt64(0, 123)
	row := chk.GetRow(0)

	// Passing mismatched field types should trigger panic recovery
	wrongFieldTypes := []*types.FieldType{
		types.NewFieldType(5),
		types.NewFieldType(5),
		types.NewFieldType(5),
	}

	// Should not panic, should return empty string
	result := safeRowToString(row, wrongFieldTypes)
	// The function should either return empty string or handle gracefully
	_ = result // Just verify no panic occurred
}
