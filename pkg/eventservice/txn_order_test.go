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

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/logservice/eventstore"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/ordering"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestTxnOrderMatcherChangesIndependentlyOfFilter(t *testing.T) {
	broker, _, _, _ := newEventBrokerForTest()
	defer broker.close()
	info := newMockDispatcherInfoForTest(t)
	original, err := broker.tryGetOrSetChangefeedStatus(info)
	require.NoError(t, err)
	require.False(t, original.txnOrderMatcher.Match("test", "ordered"))

	updated := proto.Clone(info.filterConfig).(*eventpb.FilterConfig)
	updated.TxnOrderTables = []string{"test.ordered"}
	data, err := proto.Marshal(updated)
	require.NoError(t, err)
	info.filterConfig = &eventpb.FilterConfig{}
	require.NoError(t, proto.Unmarshal(data, info.filterConfig))
	// Recreate changefeed-scoped state as happens after pausing for a config update.
	broker.changefeedMap.Delete(info.GetChangefeedID())
	current, err := broker.tryGetOrSetChangefeedStatus(info)
	require.NoError(t, err)
	require.Same(t, original.filter, current.filter)
	require.True(t, current.txnOrderMatcher.Match("TEST", "ORDERED"))
	require.False(t, current.txnOrderMatcher.Match("test", "unordered"))
	dispatcher := newDispatcherStat(info, 1, 1, nil, current)
	require.Same(t, current.txnOrderMatcher, dispatcher.txnOrderMatcher)
}

func TestTxnOrderPartitionValidationRespectsFiltering(t *testing.T) {
	helper := event.NewEventTestHelper(t)
	defer helper.Close()
	cfg := config.NewDefaultFilterConfig()
	cfg.Rules = []string{"test.*", "!test.excluded"}
	f, err := filter.NewFilter(cfg, "", false, false)
	require.NoError(t, err)
	matcher, err := ordering.NewTxnOrderMatcher([]string{"test.ordered", "test.excluded"}, false)
	require.NoError(t, err)
	for _, table := range []string{"ordered", "unordered", "excluded"} {
		t.Run(table, func(t *testing.T) {
			tableInfo := helper.DDL2Event("CREATE TABLE test." + table + " (id INT PRIMARY KEY) PARTITION BY HASH(id) PARTITIONS 2").TableInfo
			processor := newDMLProcessor(nil, nil, f, false, common.DefaultMode, false)
			processor.txnOrderMatcher = matcher
			err := processor.startTxn(common.NewDispatcherID(), tableInfo.TableName.TableID, tableInfo, 1, 2, true)
			if table == "ordered" {
				require.ErrorContains(t, err, "txn-order-tables does not support partitioned table")
				require.Nil(t, processor.currentTxn)
			} else {
				require.NoError(t, err)
				require.True(t, processor.currentTxn.shouldSplitTxn)
			}
		})
	}
}

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
	wholeSpan := common.TableIDToComparableSpan(testTableTriggerKeyspaceID, tableID)
	info.span = &wholeSpan
	for _, rule := range []string{"test.t", "test.other"} {
		t.Run(rule, func(t *testing.T) {
			f, err := filter.NewFilter(config.NewDefaultFilterConfig(), "", false, false)
			require.NoError(t, err)
			status := newChangefeedStatus(info.GetChangefeedID(), 0)
			status.filter = f
			status.txnOrderMatcher, err = ordering.NewTxnOrderMatcher([]string{rule}, false)
			require.NoError(t, err)
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

func TestTxnOrderScanReportsInvalidTable(t *testing.T) {
	helper := event.NewEventTestHelper(t)
	t.Cleanup(helper.Close)
	partitioned := helper.DDL2Event("CREATE TABLE test.partitioned (id INT PRIMARY KEY) PARTITION BY HASH(id) PARTITIONS 2")
	original := helper.DDL2Event("CREATE TABLE test.regular (id INT PRIMARY KEY)")
	renamed := helper.DDL2Event("RENAME TABLE test.regular TO test.ordered")
	require.NotNil(t, renamed.TableInfo)

	for _, tc := range []struct {
		name   string
		start  *event.DDLEvent
		rule   string
		rename bool
		split  bool
		want   string
	}{
		{name: "partitioned DML", start: partitioned, rule: "test.partitioned", want: "does not support partitioned table test.partitioned"},
		{name: "rename split span", start: original, rule: "test.ordered", rename: true, split: true, want: "does not support split spans for table test.ordered"},
		{name: "ordered DML on split span", start: renamed, rule: "test.ordered", split: true, want: "does not support split spans for table test.ordered"},
		{name: "rename complete span", start: original, rule: "test.ordered", rename: true},
		{name: "rename unmatched split span", start: original, rule: "test.unrelated", rename: true, split: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			broker, store, schema, _ := newEventBrokerForTest()
			broker.close()

			info := newMockDispatcherInfoForTest(t)
			info.epoch = 1
			info.filterConfig.TxnOrderTables = []string{tc.rule}
			info.startTs = tc.start.FinishedTs
			tableID := tc.start.TableInfo.TableName.TableID
			span := common.TableIDToComparableSpan(testTableTriggerKeyspaceID, tableID)
			if tc.split {
				span.EndKey = append(append([]byte{}, span.StartKey...), 1)
			}
			info.span = &span
			require.NoError(t, broker.addDispatcher(info))
			stat := broker.getDispatcher(info.GetID()).Load()
			stat.setHandshaked()
			schema.AppendDDLEvent(tableID, *tc.start)
			endTs := info.startTs + 2
			if tc.rename {
				ddl := *renamed
				ddl.TableStateChange = &event.TableStateChange{PhysicalTableID: tableID, Kind: event.TableStateUpdated}
				schema.AppendDDLEvent(tableID, ddl)
				endTs = ddl.FinishedTs
			} else {
				require.NoError(t, store.AppendEvents(info.GetID(), endTs, &common.RawKVEntry{
					StartTs: info.startTs + 1, CRTs: endTs, Key: span.StartKey,
				}))
			}
			schema.resolvedTs, schema.maxDDLCommitTs = endTs, endTs
			stat.receivedResolvedTs.Store(endTs)
			stat.eventStoreCommitTs.Store(endTs)
			quota := atomic.NewUint64(broker.scanLimitInBytes)
			stat.changefeedStat.availableMemoryQuota.Store(node.ID(info.GetServerID()), quota)
			if tc.want != "" {
				stat.enableSyncPoint = true
				stat.nextSyncPoint.Store(info.startTs + 1)
			}
			stat.scanState = dispatcherScanQueued
			broker.doScan(t.Context(), stat)
			messages := broker.getMessageCh(stat.messageWorkerIndex, false)
			if tc.want == "" {
				require.NotEmpty(t, messages)
				ddl := (<-messages).e.(*event.DDLEvent)
				require.Empty(t, ddl.Err)
				require.Equal(t, endTs, stat.sentResolvedTs.Load())
				return
			}
			require.Len(t, messages, 1)
			ddl := (<-messages).e.(*event.DDLEvent)
			data, err := ddl.Marshal()
			require.NoError(t, err)
			var decoded event.DDLEvent
			require.NoError(t, decoded.Unmarshal(data))
			require.ErrorContains(t, decoded.GetError(), tc.want)
			require.True(t, errors.ShouldFailChangefeed(decoded.GetError()))
			require.Equal(t, info.startTs, stat.sentResolvedTs.Load())
			require.Equal(t, broker.scanLimitInBytes, quota.Load())
		})
	}
}
