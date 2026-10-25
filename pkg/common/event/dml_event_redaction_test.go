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
	"github.com/stretchr/testify/require"
)

// TestBatchDMLEventStringRedaction tests that BatchDMLEvent.String() properly applies redaction
// to both Rows (chunk.Chunk) and RawRows ([]byte) fields.
// This is a critical security test - if it fails, sensitive data may leak into logs.
func TestBatchDMLEventStringRedaction(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.tk.MustExec("use test")

	// Create table with sensitive data column
	createSQL := "CREATE TABLE t (id INT PRIMARY KEY, password VARCHAR(100))"
	helper.DDL2Job(createSQL)

	// Insert sensitive data - this will be in the DML event
	insertSQL := "INSERT INTO t VALUES (1, 'SensitivePassword123!')"
	dmlEvent := helper.DML2Event("test", "t", insertSQL)
	require.NotNil(t, dmlEvent)

	// Create BatchDMLEvent with sensitive data in both Rows and RawRows
	batchEvent := &BatchDMLEvent{
		Version:   BatchDMLEventVersion1,
		DMLEvents: []*DMLEvent{dmlEvent},
		Rows:      dmlEvent.Rows,
		RawRows:   []byte("raw_sensitive_data_bytes"),
		TableInfo: dmlEvent.TableInfo,
	}

	testCases := []struct {
		name              string
		redactMode        string
		expectContains    []string
		expectNotContains []string
	}{
		{
			name:              "OFF mode",
			redactMode:        errors.RedactLogDisable,
			expectContains:    []string{"RawRowsSize:"},
			expectNotContains: []string{"SensitivePassword123!", "raw_sensitive_data_bytes"},
		},
		{
			name:              "MARKER mode",
			redactMode:        errors.RedactLogMarker,
			expectContains:    []string{"RawRowsSize:"},
			expectNotContains: []string{"SensitivePassword123!", "raw_sensitive_data_bytes"},
		},
		{
			name:              "ON mode",
			redactMode:        errors.RedactLogEnable,
			expectContains:    []string{"RawRowsSize:"},
			expectNotContains: []string{"SensitivePassword123!", "raw_sensitive_data_bytes"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Set redaction mode
			originalMode := errors.RedactLogEnabled.Load()
			errors.RedactLogEnabled.Store(tc.redactMode)
			defer errors.RedactLogEnabled.Store(originalMode)

			// Call BatchDMLEvent.String()
			result := batchEvent.String()
			dmlResult := dmlEvent.String()
			require.Contains(t, dmlResult, "DMLEvent{")
			require.NotContains(t, dmlResult, "SensitivePassword123!")

			// Verify expected content
			for _, expected := range tc.expectContains {
				require.Contains(t, result, expected,
					"Expected '%s' in result: %s", expected, result)
			}

			// Verify sensitive data is NOT present
			for _, unexpected := range tc.expectNotContains {
				require.NotContains(t, result, unexpected,
					"SECURITY FAILURE: '%s' leaked! Result: %s", unexpected, result)
			}
		})
	}
}

// TestBatchDMLEventStringRedactionMultipleRows tests redaction with multiple DMLEvents
func TestBatchDMLEventStringRedactionMultipleRows(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.tk.MustExec("use test")

	// Create table with sensitive data column
	createSQL := "CREATE TABLE t2 (id INT PRIMARY KEY, secret VARCHAR(100))"
	helper.DDL2Job(createSQL)

	// Insert multiple rows to create multiple DMLEvents
	insertSQL1 := "INSERT INTO t2 VALUES (1, 'FirstSecret123!')"
	dmlEvent1 := helper.DML2Event("test", "t2", insertSQL1)
	require.NotNil(t, dmlEvent1)

	insertSQL2 := "INSERT INTO t2 VALUES (2, 'SecondSecret456!')"
	dmlEvent2 := helper.DML2Event("test", "t2", insertSQL2)
	require.NotNil(t, dmlEvent2)

	// Create BatchDMLEvent with multiple DMLEvents
	batchEvent := &BatchDMLEvent{
		Version:   BatchDMLEventVersion1,
		DMLEvents: []*DMLEvent{dmlEvent1, dmlEvent2},
		Rows:      dmlEvent1.Rows, // Use first event's rows for Rows field
		RawRows:   []byte("multi_row_raw_data"),
		TableInfo: dmlEvent1.TableInfo,
	}

	// Test ON mode - all sensitive data from all DMLEvents must be redacted
	originalMode := errors.RedactLogEnabled.Load()
	errors.RedactLogEnabled.Store(errors.RedactLogEnable)
	defer errors.RedactLogEnabled.Store(originalMode)

	result := batchEvent.String()

	// SECURITY: Verify no sensitive data leaks from ANY DMLEvent
	sensitiveData := []string{"FirstSecret123!", "SecondSecret456!", "multi_row_raw_data"}
	for _, sensitive := range sensitiveData {
		require.NotContains(t, result, sensitive,
			"SECURITY FAILURE: '%s' leaked in multi-row batch! Result: %s", sensitive, result)
	}

	require.Contains(t, result, "DMLEventCount: 2")
}
