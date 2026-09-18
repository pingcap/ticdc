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

package routing

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/stretchr/testify/require"
)

func TestValidateSameClusterRouting(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test")
	capturedTables := []common.TableName{{Schema: "src", Table: "t1"}}
	// The filter replicates every table of the src schema.
	f, err := filter.NewFilter(&config.FilterConfig{Rules: []string{"src.*"}}, "UTC", false, false)
	require.NoError(t, err)

	cases := []struct {
		name      string
		rules     []*config.DispatchRule
		wantError string
	}{
		{
			name:      "table routing is not enabled",
			rules:     []*config.DispatchRule{{Matcher: []string{"src.*"}, PartitionRule: "table"}},
			wantError: "allow-same-cluster requires table route to be enabled",
		},
		{
			name:      "no rule matches the replicated table",
			rules:     []*config.DispatchRule{{Matcher: []string{"other.*"}, TargetSchema: "dst"}},
			wantError: "table src.t1 is not routed",
		},
		{
			name:      "the table is routed to itself",
			rules:     []*config.DispatchRule{{Matcher: []string{"src.t1"}, TargetSchema: "{schema}", TargetTable: "{table}"}},
			wantError: "table src.t1 is not routed",
		},
		{
			name:      "the route target is replicated as well",
			rules:     []*config.DispatchRule{{Matcher: []string{"src.t1"}, TargetSchema: "src", TargetTable: "{table}_copy"}},
			wantError: "table src.t1 is routed to src.t1_copy, which is replicated as well",
		},
		{
			name:  "the route target stays outside the changefeed",
			rules: []*config.DispatchRule{{Matcher: []string{"src.*"}, TargetSchema: "dst", TargetTable: "{table}_routed"}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateSameClusterRouting(changefeedID, false, tc.rules, capturedTables, f)
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				return
			}
			require.NoError(t, err)
		})
	}
}
