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

package main

import (
	"fmt"
	"testing"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestExitError_Error(t *testing.T) {
	t.Parallel()
	inner := fmt.Errorf("something went wrong")
	ee := &ExitError{Code: ExitCodeTransient, Err: inner}
	require.Equal(t, "something went wrong", ee.Error())
}

func TestExitError_Unwrap(t *testing.T) {
	t.Parallel()
	inner := fmt.Errorf("root cause")
	ee := &ExitError{Code: ExitCodeCheckpointCorruption, Err: inner}
	require.ErrorIs(t, ee, inner)
	require.Equal(t, inner, ee.Unwrap())
}

func TestExitError_Unwrap_deep(t *testing.T) {
	t.Parallel()
	root := errors.New("root")
	wrapped := fmt.Errorf("layer1: %w", root)
	ee := &ExitError{Code: ExitCodeTransient, Err: wrapped}
	require.ErrorIs(t, ee, root)
}

func TestValidateS3BucketPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		changefeedURI string
		configURI     string
		wantErr       bool
		errContains   string
	}{
		{
			name:          "exact match",
			changefeedURI: "s3://my-bucket/cluster1/",
			configURI:     "s3://my-bucket/cluster1/",
			wantErr:       false,
		},
		{
			name:          "match ignoring trailing slash",
			changefeedURI: "s3://my-bucket/cluster1",
			configURI:     "s3://my-bucket/cluster1/",
			wantErr:       false,
		},
		{
			name:          "match with query params in changefeed URI",
			changefeedURI: "s3://my-bucket/prefix/?protocol=canal-json&date-separator=day",
			configURI:     "s3://my-bucket/prefix/",
			wantErr:       false,
		},
		{
			name:          "bucket mismatch",
			changefeedURI: "s3://bucket-a/prefix/",
			configURI:     "s3://bucket-b/prefix/",
			wantErr:       true,
			errContains:   "bucket/prefix mismatch",
		},
		{
			name:          "prefix mismatch",
			changefeedURI: "s3://my-bucket/cluster1/",
			configURI:     "s3://my-bucket/cluster2/",
			wantErr:       true,
			errContains:   "bucket/prefix mismatch",
		},
		{
			name:          "scheme mismatch",
			changefeedURI: "gcs://my-bucket/prefix/",
			configURI:     "s3://my-bucket/prefix/",
			wantErr:       true,
			errContains:   "bucket/prefix mismatch",
		},
		{
			name:          "deeper prefix mismatch",
			changefeedURI: "s3://my-bucket/a/b/c/",
			configURI:     "s3://my-bucket/a/b/d/",
			wantErr:       true,
			errContains:   "bucket/prefix mismatch",
		},
		{
			name:          "empty config URI",
			changefeedURI: "s3://my-bucket/prefix/",
			configURI:     "",
			wantErr:       true,
			errContains:   "bucket/prefix mismatch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateS3BucketPrefix(tt.changefeedURI, tt.configURI, "test-cluster", "cf-1")
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestExitCodeFromError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		fallback int
		expected int
	}{
		{
			name:     "ExitError with transient code",
			err:      &ExitError{Code: ExitCodeTransient, Err: fmt.Errorf("timeout")},
			fallback: ExitCodeUnrecoverable,
			expected: ExitCodeTransient,
		},
		{
			name:     "ExitError with checkpoint corruption code",
			err:      &ExitError{Code: ExitCodeCheckpointCorruption, Err: fmt.Errorf("bad checkpoint")},
			fallback: ExitCodeTransient,
			expected: ExitCodeCheckpointCorruption,
		},
		{
			name:     "ExitError with unrecoverable code",
			err:      &ExitError{Code: ExitCodeUnrecoverable, Err: fmt.Errorf("fatal")},
			fallback: ExitCodeTransient,
			expected: ExitCodeUnrecoverable,
		},
		{
			name:     "ExitError with invalid config code",
			err:      &ExitError{Code: ExitCodeInvalidConfig, Err: fmt.Errorf("missing field")},
			fallback: ExitCodeTransient,
			expected: ExitCodeInvalidConfig,
		},
		{
			name:     "ExitError with decode config failed code",
			err:      &ExitError{Code: ExitCodeDecodeConfigFailed, Err: fmt.Errorf("bad toml")},
			fallback: ExitCodeTransient,
			expected: ExitCodeDecodeConfigFailed,
		},
		{
			name:     "plain error returns fallback",
			err:      fmt.Errorf("some plain error"),
			fallback: ExitCodeTransient,
			expected: ExitCodeTransient,
		},
		{
			name:     "plain error returns different fallback",
			err:      fmt.Errorf("another plain error"),
			fallback: ExitCodeUnrecoverable,
			expected: ExitCodeUnrecoverable,
		},
		{
			name:     "wrapped ExitError is still extracted",
			err:      fmt.Errorf("outer: %w", &ExitError{Code: ExitCodeCheckpointCorruption, Err: fmt.Errorf("inner")}),
			fallback: ExitCodeTransient,
			expected: ExitCodeCheckpointCorruption,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			code := exitCodeFromError(tt.err, tt.fallback)
			require.Equal(t, tt.expected, code)
		})
	}
}
