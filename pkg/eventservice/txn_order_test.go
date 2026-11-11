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

package eventservice

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/logservice/eventstore"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/stretchr/testify/require"
)

func TestTxnOrderTablesKeepCompleteTransactions(t *testing.T) {
	original := config.GetGlobalServerConfig()
	cfg := original.Clone()
	cfg.Debug.EventService.DMLEventMaxRows = 1
	cfg.Debug.EventService.DMLEventMaxBytes = 1
	cfg.Debug.EventService.LargeTxnThresholdInBytes = 1
	cfg.DataDir = t.TempDir()
	config.StoreGlobalServerConfig(cfg)
	defer config.StoreGlobalServerConfig(original)

	helper := event.NewEventTestHelper(t)
	defer helper.Close()
	ddl, kvs := genEvents(helper, "create table test.t (id int primary key)", "insert into test.t values(1)", "insert into test.t values(2)", "insert into test.t values(3)")
	for _, kv := range kvs {
		kv.StartTs = ddl.FinishedTs + 1
		kv.CRTs = ddl.FinishedTs + 2
	}
	info := newMockDispatcherInfoForTest(t)
	tableID := info.GetTableSpan().TableID
	for _, rule := range []string{"test.t", "test.other"} {
		t.Run(rule, func(t *testing.T) {
			f, err := filter.NewFilterWithTxnOrderTables(config.NewDefaultFilterConfig(), "", false, false, []string{rule})
			require.NoError(t, err)
			status := newChangefeedStatus(info.GetChangefeedID(), 0)
			status.filter = f
			disp := newDispatcherStat(info, 1, 1, nil, status)
			makeDispatcherReady(disp)
			schema := NewMockSchemaStore()
			schema.AppendDDLEvent(tableID, ddl)
			scanner := newEventScanner(&stubEventGetter{iter: &mockEventIterator{events: kvs}}, schema, &mockMounter{}, 0)
			request := eventstore.ScanRequest{Range: common.DataRange{Span: info.GetTableSpan(), CommitTsStart: ddl.FinishedTs, CommitTsEnd: kvs[0].CRTs + 1}}
			_, events, _, _, err := scanner.scan(context.Background(), disp, request, scanLimit{maxDMLBytes: 1})
			require.NoError(t, err)
			var txns []*event.DMLEvent
			for _, item := range events {
				if batch, ok := item.(*event.BatchDMLEvent); ok {
					txns = append(txns, batch.DMLEvents...)
				}
			}
			if rule == "test.t" {
				require.Len(t, txns, 1)
				require.Equal(t, int32(3), txns[0].Len())
				require.Nil(t, disp.getLargeTxnState())
			} else {
				require.NotEmpty(t, txns)
				for _, txn := range txns {
					require.Equal(t, int32(1), txn.Len())
				}
			}
		})
	}
}
