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

package franz

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestNewRequiredAcks(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		requiredAcks int16
		expected     kgo.Acks
	}{
		{name: "all", requiredAcks: -1, expected: kgo.AllISRAcks()},
		{name: "leader", requiredAcks: 1, expected: kgo.LeaderAck()},
		{name: "none", requiredAcks: 0, expected: kgo.NoAck()},
		{name: "invalid fallback all", requiredAcks: 2, expected: kgo.AllISRAcks()},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.expected, newRequiredAcks(&Options{RequiredAcks: tc.requiredAcks}))
		})
	}

	require.Equal(t, kgo.AllISRAcks(), newRequiredAcks(nil))
}

func TestMaxTimeoutWithDefault(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		readTimeout  time.Duration
		writeTimeout time.Duration
		expected     time.Duration
	}{
		{name: "read timeout is max", readTimeout: 3 * time.Second, writeTimeout: 2 * time.Second, expected: 3 * time.Second},
		{name: "write timeout is max", readTimeout: 2 * time.Second, writeTimeout: 4 * time.Second, expected: 4 * time.Second},
		{name: "both zero use default", readTimeout: 0, writeTimeout: 0, expected: defaultRequestTimeout},
		{name: "both negative use default", readTimeout: -time.Second, writeTimeout: -2 * time.Second, expected: defaultRequestTimeout},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.expected, maxTimeoutWithDefault(tc.readTimeout, tc.writeTimeout))
		})
	}
}
