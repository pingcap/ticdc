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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMergeAndDedupPDEndpoints(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		seed       []string
		discovered []string
		expected   []string
	}{
		{
			name:       "preserve first seen order across sources",
			seed:       []string{"http://pd-1:2379", "http://pd-2:2379"},
			discovered: []string{"http://pd-2:2379", "http://pd-3:2379", "http://pd-1:2379"},
			expected:   []string{"http://pd-1:2379", "http://pd-2:2379", "http://pd-3:2379"},
		},
		{
			name:       "drop duplicates within each source",
			seed:       []string{"http://pd-1:2379", "http://pd-1:2379"},
			discovered: []string{"http://pd-2:2379", "http://pd-2:2379"},
			expected:   []string{"http://pd-1:2379", "http://pd-2:2379"},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := mergeAndDedupPDEndpoints(tc.seed, tc.discovered)
			require.Equal(t, tc.expected, result)
		})
	}
}
