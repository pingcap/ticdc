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

package kafka

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewClientOptionMapsRequiredAcks(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		requiredAcks RequiredAcks
	}{
		{name: "wait for all", requiredAcks: WaitForAll},
		{name: "wait for local", requiredAcks: WaitForLocal},
		{name: "no response", requiredAcks: NoResponse},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			options := NewOptions()
			options.RequiredAcks = tc.requiredAcks

			kafkaOptions := newClientOption(options)
			require.Equal(t, int16(tc.requiredAcks), kafkaOptions.RequiredAcks)
		})
	}
}

func TestNewClientOptionMapsMaxRetry(t *testing.T) {
	t.Parallel()

	options := NewOptions()
	options.MaxRetry = 7

	kafkaOptions := newClientOption(options)
	require.Equal(t, 7, kafkaOptions.MaxRetry)
}
