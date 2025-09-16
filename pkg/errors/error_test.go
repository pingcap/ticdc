// Copyright 2025 PingCAP, Inc.
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

package errors

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

// TestShouldFailChangefeed tests the ShouldFailChangefeed function to ensure it correctly
// identifies changefeed unretryable errors that should cause the changefeed to fail.
func TestShouldFailChangefeed(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error should return false",
			err:      nil,
			expected: false,
		},
		{
			name:     "ErrGCTTLExceeded should return true",
			err:      ErrGCTTLExceeded.GenWithStackByArgs(12345, "test-changefeed"),
			expected: true,
		},
		{
			name:     "ErrSnapshotLostByGC should return true",
			err:      ErrSnapshotLostByGC.GenWithStackByArgs(12345, 67890),
			expected: true,
		},
		{
			name:     "ErrStartTsBeforeGC should return true",
			err:      ErrStartTsBeforeGC.GenWithStackByArgs(12345, 67890),
			expected: true,
		},
		{
			name:     "ErrExpressionColumnNotFound should return true",
			err:      ErrExpressionColumnNotFound.GenWithStackByArgs("test-column"),
			expected: true,
		},
		{
			name:     "ErrExpressionParseFailed should return true",
			err:      ErrExpressionParseFailed.GenWithStackByArgs("invalid expression"),
			expected: true,
		},
		{
			name:     "ErrSchemaSnapshotNotFound should return true",
			err:      ErrSchemaSnapshotNotFound.GenWithStackByArgs(12345),
			expected: true,
		},
		{
			name:     "ErrSyncRenameTableFailed should return true",
			err:      ErrSyncRenameTableFailed.GenWithStackByArgs("old-table", "new-table"),
			expected: true,
		},
		{
			name:     "ErrChangefeedUnretryable should return true",
			err:      ErrChangefeedUnretryable.GenWithStackByArgs("test error"),
			expected: true,
		},
		{
			name:     "ErrCorruptedDataMutation should return true",
			err:      ErrCorruptedDataMutation.GenWithStackByArgs("test mutation"),
			expected: true,
		},
		{
			name:     "ErrDispatcherFailed should return true",
			err:      ErrDispatcherFailed.GenWithStackByArgs(),
			expected: true,
		},
		{
			name:     "ErrColumnSelectorFailed should return true",
			err:      ErrColumnSelectorFailed.GenWithStackByArgs(),
			expected: true,
		},
		{
			name:     "ErrSinkURIInvalid should return true",
			err:      ErrSinkURIInvalid.GenWithStackByArgs("invalid-uri"),
			expected: true,
		},
		{
			name:     "ErrKafkaInvalidConfig should return true",
			err:      ErrKafkaInvalidConfig.GenWithStackByArgs("invalid config"),
			expected: true,
		},
		{
			name:     "ErrMySQLInvalidConfig should return true",
			err:      ErrMySQLInvalidConfig.GenWithStackByArgs("invalid config"),
			expected: true,
		},
		{
			name:     "ErrStorageSinkInvalidConfig should return true",
			err:      ErrStorageSinkInvalidConfig.GenWithStackByArgs("invalid config"),
			expected: true,
		},
		{
			name:     "Error with RFC code matching should return true",
			err:      errors.New("CDC:ErrGCTTLExceeded some error message"),
			expected: true,
		},
		{
			name:     "Error message containing RFC code should return true",
			err:      errors.New("some error CDC:ErrSnapshotLostByGC happened"),
			expected: true,
		},
		{
			name:     "Regular error should return false",
			err:      errors.New("regular error"),
			expected: false,
		},
		{
			name:     "ErrChangeFeedNotExists should return false (not in unretryable list)",
			err:      ErrChangeFeedNotExists.GenWithStackByArgs("test-changefeed"),
			expected: false,
		},
		{
			name:     "ErrChangeFeedAlreadyExists should return false (not in unretryable list)",
			err:      ErrChangeFeedAlreadyExists.GenWithStackByArgs("test-changefeed"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ShouldFailChangefeed(tt.err)
			require.Equal(t, tt.expected, result, "ShouldFailChangefeed(%v) = %v, want %v", tt.err, result, tt.expected)
		})
	}
}

// TestShouldFailChangefeedWithWrappedErrors tests ShouldFailChangefeed with wrapped errors
// to ensure it can handle error chains properly.
func TestShouldFailChangefeedWithWrappedErrors(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "Wrapped ErrGCTTLExceeded should return true",
			err:      errors.Wrap(ErrGCTTLExceeded.GenWithStackByArgs(12345, "test-changefeed"), "wrapped error"),
			expected: true,
		},
		{
			name:     "Wrapped ErrSnapshotLostByGC should return true",
			err:      errors.Wrap(ErrSnapshotLostByGC.GenWithStackByArgs(12345, 67890), "wrapped error"),
			expected: true,
		},
		{
			name:     "Wrapped regular error should return false",
			err:      errors.Wrap(errors.New("regular error"), "wrapped error"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ShouldFailChangefeed(tt.err)
			require.Equal(t, tt.expected, result, "ShouldFailChangefeed(%v) = %v, want %v", tt.err, result, tt.expected)
		})
	}
}
