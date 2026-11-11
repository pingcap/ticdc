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
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestTxnOrderTables(t *testing.T) {
	cfg := config.NewDefaultFilterConfig()
	cfg.Rules = []string{"app.*", "!app.excluded"}
	f, err := NewFilterWithTxnOrderTables(cfg, "", false, false, []string{"app.*", "!app.unordered"})
	require.NoError(t, err)
	for _, tc := range []struct {
		table                          string
		partition, eligible, wantError bool
	}{
		{"orders", false, true, false},
		{"orders", true, true, true},
		{"orders", true, false, false},
		{"excluded", true, true, false},
		{"unordered", true, true, false},
	} {
		ti := &common.TableInfo{
			TableName:        common.TableName{Schema: "app", Table: tc.table, IsPartition: tc.partition},
			HasPKOrNotNullUK: tc.eligible,
		}
		err := f.Verify([]*common.TableInfo{ti})
		if tc.wantError {
			require.ErrorContains(t, err, "txn-order-tables does not support partitioned table")
		} else {
			require.NoError(t, err)
		}
	}
	require.True(t, f.ShouldKeepTxnOrder("APP", "ORDERS"))
	require.False(t, f.ShouldKeepTxnOrder("app", "excluded"))

	// Ordering rules must survive transport and invalidate the shared filter cache.
	pb := &eventpb.FilterConfig{FilterConfig: &eventpb.InnerFilterConfig{Rules: cfg.Rules}}
	storage := &SharedFilterStorage{m: make(map[common.ChangeFeedID]FilterWithConfig)}
	id := common.NewChangefeedID4Test("test", "ordered")
	old, err := storage.GetOrSetFilter(id, pb, "")
	require.NoError(t, err)
	require.False(t, old.ShouldKeepTxnOrder("app", "orders"))
	updated := proto.Clone(pb).(*eventpb.FilterConfig)
	updated.TxnOrderTables = []string{"app.*"}
	data, err := proto.Marshal(updated)
	require.NoError(t, err)
	decoded := &eventpb.FilterConfig{}
	require.NoError(t, proto.Unmarshal(data, decoded))
	current, err := storage.GetOrSetFilter(id, decoded, "")
	require.NoError(t, err)
	require.True(t, current.ShouldKeepTxnOrder("app", "orders"))
	require.NotSame(t, old, current)
}
