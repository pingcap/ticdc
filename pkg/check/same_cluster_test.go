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

package check

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestValidateSameClusterRouting(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		allowSame   bool
		filterRules []string
		dispatch    []*config.DispatchRule
		wantError   string
	}{
		{
			name:        "allow-same-cluster is not set",
			filterRules: []string{"src.*"},
		},
		{
			name:        "table routing is not enabled",
			allowSame:   true,
			filterRules: []string{"src.*"},
			wantError:   "allow-same-cluster requires table routing to be enabled",
		},
		{
			name:        "the routing covers the filter and targets another schema",
			allowSame:   true,
			filterRules: []string{"src.*"},
			dispatch:    []*config.DispatchRule{{Matcher: []string{"src.*"}, TargetSchema: "dst", TargetTable: "{table}_routed"}},
		},
		{
			name:        "the target schema is derived from the source schema",
			allowSame:   true,
			filterRules: []string{"src.*"},
			dispatch:    []*config.DispatchRule{{Matcher: []string{"src.*"}, TargetSchema: "{schema}_routed", TargetTable: "{table}_routed"}},
		},
		{
			name:      "the filter matches every table and the target schema is derived",
			allowSame: true,
			dispatch:  []*config.DispatchRule{{Matcher: []string{"*.*"}, TargetSchema: "{schema}_routed", TargetTable: "{table}_routed"}},
			wantError: "which the filter replicates",
		},
		{
			name:      "the filter matches every table",
			allowSame: true,
			dispatch:  []*config.DispatchRule{{Matcher: []string{"*.*"}, TargetSchema: "dst", TargetTable: "{table}"}},
			wantError: "which the filter replicates",
		},
		{
			name:        "the matcher is narrower than the filter rule",
			allowSame:   true,
			filterRules: []string{"src.*"},
			dispatch:    []*config.DispatchRule{{Matcher: []string{"src.t1"}, TargetSchema: "dst", TargetTable: "{table}_routed"}},
			wantError:   `filter rule "src.*" is not covered by any dispatch rule matcher`,
		},
		{
			name:        "the target schema is replicated as well",
			allowSame:   true,
			filterRules: []string{"src.*"},
			dispatch:    []*config.DispatchRule{{Matcher: []string{"src.*"}, TargetSchema: "src", TargetTable: "{table}_copy"}},
			wantError:   "which the filter replicates",
		},
		{
			name:        "the target table is replicated as well",
			allowSame:   true,
			filterRules: []string{"src.*", "dst.t1"},
			dispatch: []*config.DispatchRule{
				{Matcher: []string{"src.*"}, TargetSchema: "dst", TargetTable: "{table}"},
				{Matcher: []string{"dst.t1"}, TargetSchema: "dst2"},
			},
			wantError: "which the filter replicates",
		},
		{
			name:        "a different target table in the source schema is rejected",
			allowSame:   true,
			filterRules: []string{"src.t1"},
			dispatch:    []*config.DispatchRule{{Matcher: []string{"src.t1"}, TargetSchema: "src", TargetTable: "t1_bak"}},
			wantError:   "requires database isolation",
		},
		{
			name:        "a derived target table in another source schema is rejected",
			allowSame:   true,
			filterRules: []string{"src.*", "dst.t1"},
			dispatch: []*config.DispatchRule{
				{Matcher: []string{"src.*"}, TargetSchema: "dst", TargetTable: "{table}_routed"},
				{Matcher: []string{"dst.t1"}, TargetSchema: "dst2"},
			},
			wantError: "requires database isolation",
		},
		{
			name:        "matching schemas with a disjoint literal target table",
			allowSame:   true,
			filterRules: []string{"src.a*"},
			dispatch:    []*config.DispatchRule{{Matcher: []string{"src.a*"}, TargetTable: "b"}},
			wantError:   "requires database isolation",
		},
		{
			name:        "matching schemas with a disjoint derived target table",
			allowSame:   true,
			filterRules: []string{"src.a*"},
			dispatch:    []*config.DispatchRule{{Matcher: []string{"src.a*"}, TargetTable: "b{table}"}},
			wantError:   "requires database isolation",
		},
		{
			name:        "both dimensions match a literal target",
			allowSame:   true,
			filterRules: []string{"src.a*"},
			dispatch:    []*config.DispatchRule{{Matcher: []string{"src.a*"}, TargetSchema: "src", TargetTable: "a_copy"}},
			wantError:   "which the filter replicates",
		},
		{
			name:        "the target keeps the source name",
			allowSame:   true,
			filterRules: []string{"src.*"},
			dispatch:    []*config.DispatchRule{{Matcher: []string{"src.*"}, TargetSchema: "{schema}", TargetTable: "{table}"}},
			wantError:   "which the filter replicates",
		},
		{
			name:        "the target keeps the source schema",
			allowSame:   true,
			filterRules: []string{"src.*"},
			dispatch:    []*config.DispatchRule{{Matcher: []string{"src.*"}, TargetTable: "{table}_bak"}},
			wantError:   "which the filter replicates",
		},
		{
			name:        "quoted names are supported",
			allowSame:   true,
			filterRules: []string{"`src`.`t1`"},
			dispatch:    []*config.DispatchRule{{Matcher: []string{"`src`.`t1`"}, TargetSchema: "dst", TargetTable: "{table}"}},
		},
		{
			name:        "case insensitive names are compared case insensitively",
			allowSame:   true,
			filterRules: []string{"SRC.*"},
			dispatch:    []*config.DispatchRule{{Matcher: []string{"src.*"}, TargetSchema: "DST", TargetTable: "{table}"}},
		},
		{
			name:        "normalization does not accept an uppercase schema placeholder",
			allowSame:   true,
			filterRules: []string{"src.*"},
			dispatch:    []*config.DispatchRule{{Matcher: []string{"src.*"}, TargetSchema: "dst_{SCHEMA}"}},
			wantError:   "does not support the target schema",
		},
		{
			name:        "normalization does not accept an uppercase table placeholder",
			allowSame:   true,
			filterRules: []string{"src.*"},
			dispatch:    []*config.DispatchRule{{Matcher: []string{"src.*"}, TargetSchema: "dst", TargetTable: "{TABLE}"}},
			wantError:   "does not support the target table",
		},
		{
			name:        "database DDL can delete another source table",
			allowSame:   true,
			filterRules: []string{"a.t1", "b.t2"},
			dispatch: []*config.DispatchRule{
				{Matcher: []string{"a.t1"}, TargetSchema: "b", TargetTable: "t1_copy"},
				{Matcher: []string{"b.t2"}, TargetSchema: "c", TargetTable: "t2_copy"},
			},
			wantError: "requires database isolation",
		},
		{
			name:        "all schemas with only one replicated table name",
			allowSame:   true,
			filterRules: []string{"*.orders"},
			dispatch:    []*config.DispatchRule{{Matcher: []string{"*.orders"}, TargetSchema: "{schema}_backup", TargetTable: "orders_copy"}},
			wantError:   "requires database isolation",
		},
		{
			name:        "database DDL also matches an unreplicated table rule",
			allowSame:   true,
			filterRules: []string{"src.t1"},
			dispatch: []*config.DispatchRule{
				{Matcher: []string{"src.t1"}, TargetSchema: "dst"},
				{Matcher: []string{"src.other"}, TargetSchema: "src"},
			},
			wantError: "requires database isolation",
		},
		{
			name:        "different tables cannot route database DDL to different schemas",
			allowSame:   true,
			filterRules: []string{"src.t1", "src.t2"},
			dispatch: []*config.DispatchRule{
				{Matcher: []string{"src.t1"}, TargetSchema: "dst1"},
				{Matcher: []string{"src.t2"}, TargetSchema: "dst2"},
			},
			wantError: "different target-schema expressions",
		},
		{
			name:        "runtime schema targets are compared without case folding",
			allowSame:   true,
			filterRules: []string{"src.t1", "src.t2"},
			dispatch: []*config.DispatchRule{
				{Matcher: []string{"src.t1"}, TargetSchema: "DST"},
				{Matcher: []string{"src.t2"}, TargetSchema: "dst"},
			},
			wantError: "different target-schema expressions",
		},
		{
			name:        "different target tables may share a target schema",
			allowSame:   true,
			filterRules: []string{"src.t1", "src.t2"},
			dispatch: []*config.DispatchRule{
				{Matcher: []string{"src.t1"}, TargetSchema: "dst", TargetTable: "t1_copy"},
				{Matcher: []string{"src.t2"}, TargetSchema: "dst", TargetTable: "t2_copy"},
			},
		},
		{
			name:        "schema matchers may overlap outside the source filter",
			allowSame:   true,
			filterRules: []string{"foo.t1", "bar.t1"},
			dispatch: []*config.DispatchRule{
				{Matcher: []string{"f*.t1"}, TargetSchema: "dst1"},
				{Matcher: []string{"*r.t1"}, TargetSchema: "dst2"},
			},
		},

		{
			name:        "negated filter rules are not supported",
			allowSame:   true,
			filterRules: []string{"src.*", "!src.tmp"},
			dispatch:    []*config.DispatchRule{{Matcher: []string{"src.*"}, TargetSchema: "dst"}},
			wantError:   "does not support the filter rule",
		},
		{
			name:        "wildcard forms other than a single star are not supported",
			allowSame:   true,
			filterRules: []string{"src.t?"},
			dispatch:    []*config.DispatchRule{{Matcher: []string{"src.*"}, TargetSchema: "dst"}},
			wantError:   "does not support the filter rule",
		},
		{
			// `sr_rout*` only matches the target schema once the `{schema}_routed` suffix is appended,
			// so the witness schema `sr` is shorter than the text the pattern asks for.
			name:        "the substitution completes the target pattern",
			allowSame:   true,
			filterRules: []string{"*r.t1", "sr_rout*.t1"},
			dispatch: []*config.DispatchRule{
				{Matcher: []string{"*r.*"}, TargetSchema: "{schema}_routed", TargetTable: "{table}"},
				{Matcher: []string{"sr_rout*.*"}, TargetSchema: "dst"},
			},
			wantError: "which the filter replicates",
		},
		{
			name:        "the target schema must not use the table placeholder",
			allowSame:   true,
			filterRules: []string{"src.*"},
			dispatch:    []*config.DispatchRule{{Matcher: []string{"src.*"}, TargetSchema: "dst_{table}", TargetTable: "{table}"}},
			wantError:   "does not support the target schema",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.ChangefeedConfig{
				AllowSameCluster: tc.allowSame,
				Filter:           &config.FilterConfig{Rules: tc.filterRules},
				SinkConfig:       &config.SinkConfig{DispatchRules: tc.dispatch},
			}

			err := ValidateSameClusterRouting(cfg)
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				return
			}
			require.NoError(t, err)
		})
	}
}
