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

package util

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseHostAndPortFromAddress(t *testing.T) {
	tests := []struct {
		name          string
		address       string
		expectedHost  string
		expectedPort  uint
		expectedError bool
	}{
		{
			name:         "valid address",
			address:      "127.0.0.1:2379",
			expectedHost: "127.0.0.1",
			expectedPort: 2379,
		},
		{
			name:         "valid address with IPv6",
			address:      "[::1]:2379",
			expectedHost: "::1",
			expectedPort: 2379,
		},
		{
			name:          "invalid address format",
			address:       "127.0.0.1",
			expectedError: true,
		},
		{
			name:          "invalid port number",
			address:       "127.0.0.1:0",
			expectedError: true,
		},
		{
			name:          "invalid port format",
			address:       "127.0.0.1:abc",
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, port, err := ParseHostAndPortFromAddress(tt.address)
			if tt.expectedError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expectedHost, host)
			require.Equal(t, tt.expectedPort, port)
		})
	}
}

func TestHang(t *testing.T) {
	t.Run("normal completion", func(t *testing.T) {
		ctx := context.Background()
		duration := 100 * time.Millisecond
		start := time.Now()
		err := Hang(ctx, duration)
		elapsed := time.Since(start)
		require.NoError(t, err)
		require.GreaterOrEqual(t, elapsed, duration)
	})

	t.Run("context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		duration := 1 * time.Second
		done := make(chan struct{})

		go func() {
			err := Hang(ctx, duration)
			require.Error(t, err)
			require.Equal(t, context.Canceled, err)
			close(done)
		}()

		// Cancel context after a short delay
		time.Sleep(100 * time.Millisecond)
		cancel()

		select {
		case <-done:
			// Test passed
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Hang did not return after context cancellation")
		}
	})
}
