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

package maintainer

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestShouldApplyMaintainerRemove(t *testing.T) {
	tests := []struct {
		name         string
		requestEpoch uint64
		localEpoch   uint64
		expected     bool
	}{
		{name: "old request without epoch keeps compatibility", requestEpoch: 0, localEpoch: 7, expected: true},
		{name: "local maintainer without epoch keeps compatibility", requestEpoch: 7, localEpoch: 0, expected: true},
		{name: "same epoch applies", requestEpoch: 7, localEpoch: 7, expected: true},
		{name: "newer request removes older maintainer", requestEpoch: 8, localEpoch: 7, expected: true},
		{name: "older request cannot remove newer maintainer", requestEpoch: 6, localEpoch: 7, expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, shouldApplyMaintainerRemove(tt.requestEpoch, tt.localEpoch))
		})
	}
}

func TestMaintainerResponseEpochMatches(t *testing.T) {
	maintainer := &Maintainer{info: &config.ChangeFeedInfo{Epoch: 7}}

	require.True(t, maintainer.maintainerResponseEpochMatches(0))
	require.True(t, maintainer.maintainerResponseEpochMatches(7))
	require.False(t, maintainer.maintainerResponseEpochMatches(6))
	require.False(t, maintainer.maintainerResponseEpochMatches(8))
}
