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

package check_test

import (
	"cmp"
	"fmt"
	"testing"

	"github.com/pingcap/ticdc/downstreamadapter/routing"
	"github.com/pingcap/ticdc/pkg/check"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestSameClusterRoutingRuntimeSemantics(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name                string
		filterRules         []string
		matcher             string
		targetSchema        string
		targetTable         string
		sourceSchema        string
		wantSchemaCaptured  bool
		caseInsensitiveOnly bool
	}{
		{
			name: "leading spaces", filterRules: []string{" src.*"},
			matcher: " src.*", targetSchema: "src", wantSchemaCaptured: true,
		},
		{
			name: "trailing spaces", filterRules: []string{"src.t1 "},
			matcher: "src.t1 ", targetSchema: "src", wantSchemaCaptured: true,
		},
		{
			name: "surrounding tabs", filterRules: []string{"\tsrc.t1\t"},
			matcher: "\tsrc.t1\t", targetSchema: "src", wantSchemaCaptured: true,
		},
		{
			name: "safe route with padded filter", filterRules: []string{" \tsrc.t1\t "},
			matcher: "src.t1", targetSchema: "dst",
		},
		{
			name: "safe route with padded matcher", filterRules: []string{"src.t1"},
			matcher: " \tsrc.t1\t ", targetSchema: "dst",
		},
		{
			name: "quoted whitespace is part of the name", filterRules: []string{" ` src`.`t1` "},
			matcher: "\t` src`.`t1`\t", targetSchema: "src", sourceSchema: " src",
		},
		{
			name: "uppercase target schema", filterRules: []string{"src.*"},
			matcher: "src.*", targetSchema: "SRC", wantSchemaCaptured: true, caseInsensitiveOnly: true,
		},
		{
			name: "uppercase target table", filterRules: []string{"src.t1"},
			matcher: "src.t1", targetTable: "T1", wantSchemaCaptured: true,
		},
		{
			name: "target schema prefix", filterRules: []string{"src.*", "copy_src.*"},
			matcher: "*.*", targetSchema: "COPY_{schema}", wantSchemaCaptured: true, caseInsensitiveOnly: true,
		},
		{
			name: "target schema suffix", filterRules: []string{"src.*", "src_copy.*"},
			matcher: "*.*", targetSchema: "{schema}_COPY", wantSchemaCaptured: true, caseInsensitiveOnly: true,
		},
		{
			name: "target table prefix", filterRules: []string{"src.t1", "src.copy_t1"},
			matcher: "src.*", targetTable: "COPY_{table}", wantSchemaCaptured: true,
		},
		{
			name: "target table suffix", filterRules: []string{"src.t1", "src.t1_copy"},
			matcher: "src.*", targetTable: "{table}_COPY", wantSchemaCaptured: true,
		},
		{
			name: "safe mixed case target", filterRules: []string{"src.*"},
			matcher: "src.*", targetSchema: "Dst_{schema}", targetTable: "{table}_Copy",
		},
	}

	for _, tc := range cases {
		for _, caseSensitive := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/caseSensitive=%t", tc.name, caseSensitive), func(t *testing.T) {
				cfg := &config.ChangefeedConfig{
					AllowSameCluster: true,
					CaseSensitive:    caseSensitive,
					Filter:           &config.FilterConfig{Rules: tc.filterRules},
					SinkConfig: &config.SinkConfig{DispatchRules: []*config.DispatchRule{{
						Matcher: []string{tc.matcher}, TargetSchema: tc.targetSchema, TargetTable: tc.targetTable,
					}}},
				}
				f, err := filter.NewFilter(cfg.Filter, "UTC", caseSensitive, false)
				require.NoError(t, err)
				router, err := routing.NewRouter(common.NewChangefeedID4Test("default", "same-cluster"), caseSensitive, cfg.SinkConfig.DispatchRules)
				require.NoError(t, err)
				schema := cmp.Or(tc.sourceSchema, "src")
				require.False(t, f.ShouldIgnoreTable(schema, "t1"))
				routed, err := router.ApplyToTableInfo(&common.TableInfo{
					TableName: common.TableName{Schema: schema, Table: "t1"},
				})
				require.NoError(t, err)
				// Table renaming cannot exclude database DDL from the source schema range.
				ddl, err := router.ApplyToDDLEvent(&event.DDLEvent{
					Type: byte(model.ActionDropSchema), SchemaName: schema,
					Query: "DROP DATABASE " + common.QuoteName(schema),
				})
				require.NoError(t, err)
				require.Equal(t, routed.GetTargetSchemaName(), ddl.GetTargetSchemaName())
				captured := !f.ShouldDiscardDDL(ddl.GetTargetSchemaName(), "", model.ActionDropSchema, nil)
				require.Equal(t, tc.wantSchemaCaptured && !(caseSensitive && tc.caseInsensitiveOnly), captured)

				err = check.ValidateSameClusterRouting(cfg)
				// TiDB resolves schema identifiers case insensitively even when the
				// in-memory filter does not capture the target's original spelling.
				if tc.wantSchemaCaptured {
					require.ErrorContains(t, err, "which the filter replicates")
					require.True(t, errors.ErrInvalidReplicaConfig.Equal(err))
				} else {
					require.NoError(t, err)
				}
			})
		}
	}
}
