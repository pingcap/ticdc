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

package filter

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestFilterIndependentOfTxnOrderRules(t *testing.T) {
	pb := &eventpb.FilterConfig{FilterConfig: &eventpb.InnerFilterConfig{
		Rules: []string{"app.*", "!app.excluded"},
	}}
	storage := &SharedFilterStorage{m: make(map[common.ChangeFeedID]FilterWithConfig)}
	id := common.NewChangefeedID4Test("test", "ordered")
	original, err := storage.GetOrSetFilter(id, pb, "")
	require.NoError(t, err)
	for _, rules := range [][]string{{"app.*"}, {"invalid"}} {
		updated := proto.Clone(pb).(*eventpb.FilterConfig)
		updated.TxnOrderTables = rules
		data, err := proto.Marshal(updated)
		require.NoError(t, err)
		decoded := &eventpb.FilterConfig{}
		require.NoError(t, proto.Unmarshal(data, decoded))
		require.Equal(t, rules, decoded.TxnOrderTables)
		current, err := storage.GetOrSetFilter(id, decoded, "")
		require.NoError(t, err)
		require.Same(t, original, current)
		require.False(t, current.ShouldIgnoreTable("app", "orders"))
		require.True(t, current.ShouldIgnoreTable("app", "excluded"))
		tableInfo := &common.TableInfo{
			TableName:        common.TableName{Schema: "app", Table: "orders", IsPartition: true},
			HasPKOrNotNullUK: true,
		}
		require.NoError(t, current.Verify([]*common.TableInfo{tableInfo}))
		ignored, err := current.ShouldIgnoreDMLByEventType(common.RowTypeInsert, tableInfo, 0)
		require.NoError(t, err)
		require.False(t, ignored)
	}
}
