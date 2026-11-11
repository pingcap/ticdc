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

package maintainer

import (
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestTxnOrderBootstrapSpans(t *testing.T) {
	f, err := filter.NewFilterWithTxnOrderTables(config.NewDefaultFilterConfig(), "", false, false, []string{"test.ordered"})
	require.NoError(t, err)
	tables := []commonEvent.Table{{TableID: 1, SchemaTableName: &commonEvent.SchemaTableName{SchemaName: "test", TableName: "ordered"}}}
	whole := common.TableIDToComparableSpan(0, 1)
	partial := whole
	partial.EndKey = append(append([]byte{}, whole.StartKey...), 1)
	for _, tc := range []struct {
		name      string
		span      *heartbeatpb.TableSpan
		operator  bool
		tableName string
		wantError bool
	}{
		{"whole table", &whole, false, "ordered", false},
		{"split table", &partial, false, "ordered", true},
		{"pending split", &partial, true, "ordered", true},
		{"unmatched table", &partial, false, "unordered", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tables[0].TableName = tc.tableName
			response := &heartbeatpb.MaintainerBootstrapResponse{}
			if tc.operator {
				response.Operators = []*heartbeatpb.ScheduleDispatcherRequest{{Config: &heartbeatpb.DispatcherConfig{Span: tc.span}}}
			} else {
				response.Spans = []*heartbeatpb.BootstrapTableSpan{{Span: tc.span}}
			}
			err := verifyTxnOrderBootstrapSpans(map[node.ID]*heartbeatpb.MaintainerBootstrapResponse{"node": response}, tables, nil, common.DefaultMode, f)
			if tc.wantError {
				require.ErrorContains(t, err, "does not support split spans")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
