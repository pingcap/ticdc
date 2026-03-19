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

package types_test

import (
	"testing"

	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/types"
	"github.com/stretchr/testify/require"
)

func TestCdcVersion_GetCompareTs(t *testing.T) {
	tests := []struct {
		name     string
		version  types.CdcVersion
		expected uint64
	}{
		{
			name: "OriginTs is set",
			version: types.CdcVersion{
				CommitTs: 100,
				OriginTs: 200,
			},
			expected: 200,
		},
		{
			name: "OriginTs is smaller than CommitTs",
			version: types.CdcVersion{
				CommitTs: 200,
				OriginTs: 100,
			},
			expected: 100,
		},
		{
			name: "OriginTs is zero",
			version: types.CdcVersion{
				CommitTs: 100,
				OriginTs: 0,
			},
			expected: 100,
		},
		{
			name: "Both are zero",
			version: types.CdcVersion{
				CommitTs: 0,
				OriginTs: 0,
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.version.GetCompareTs()
			require.Equal(t, tt.expected, result)
		})
	}
}
