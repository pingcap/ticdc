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

package main

import (
	"testing"

	"github.com/pingcap/ticdc/cmd/util"
	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/codec/avro"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/schemamanager"
	mysqlsink "github.com/pingcap/ticdc/pkg/sink/mysql"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestAvroUpdateWithoutBeforeValueFlush(t *testing.T) {
	ctx := t.Context()
	codecConfig := codeccommon.NewConfig(config.ProtocolAvro)
	codecConfig.EnableTiDBExtension = true
	require.False(t, codecConfig.AvroIncludeBeforeValue)
	encoder, err := avro.SetupEncoderAndSchemaRegistry4Testing(ctx, codecConfig)
	require.NoError(t, err)
	defer avro.TeardownEncoderAndSchemaRegistry4Testing()
	schemaM, err := schemamanager.NewConfluentSchemaManager(ctx, "http://127.0.0.1:8081", nil)
	require.NoError(t, err)
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	tableInfo := helper.DDL2Event("create table test.t (id int primary key, v int)").TableInfo
	const topic = "avro-update"
	var payloads []*codeccommon.Message
	var previous chunk.Row
	for i, value := range []int64{10, 20, 30} {
		row := chunk.MutRowFromValues(int64(1), value).ToRow()
		require.NoError(t, encoder.AppendRowChangedEvent(ctx, topic, &commonEvent.RowEvent{
			TableInfo: tableInfo, Event: commonEvent.RowChange{PreRow: previous, Row: row}, CommitTs: uint64(100 + i),
			ColumnSelector: columnselector.NewDefaultColumnSelector(),
		}))
		payloads = append(payloads, encoder.Build()...)
		previous = row
	}
	for _, together := range []bool{true, false} {
		name := "one flush"
		if !together {
			name = "separate flushes"
		}
		t.Run(name, func(t *testing.T) {
			dec := avro.NewDecoder(codecConfig, 0, schemaM, topic, nil)
			var messages []*codeccommon.DMLMessage
			for _, payload := range payloads {
				dec.AddKeyValue(payload.Key, payload.Value)
				typ, ok := dec.HasNext()
				require.True(t, ok)
				require.Equal(t, codeccommon.MessageTypeRow, typ)
				messages = append(messages, dec.NextDMLMessage())
			}
			require.Less(t, messages[0].GetCommitTs(), messages[1].GetCommitTs())
			require.Equal(t, common.RowTypeInsert, messages[0].RowType)
			require.Equal(t, common.RowTypeUpdate, messages[1].RowType)
			require.Equal(t, common.RowTypeUpdate, messages[2].RowType)
			events := util.DMLMessagesToEvents(messages)
			sinkConfig := config.GetDefaultReplicaConfig().Sink
			sinkConfig.DispatchRules = []*config.DispatchRule{{Matcher: []string{"test.t"}, PartitionRule: "columns", Columns: []string{"v"}}}
			router, err := eventrouter.NewEventRouter(sinkConfig, true, topic, false, true)
			require.NoError(t, err)
			consumerWriter := &writer{protocol: config.ProtocolAvro, eventRouter: router, progresses: make([]*partitionProgress, 16)}
			for i, e := range events {
				after := commonEvent.RowChange{Row: e.Rows.GetRow(e.Rows.NumRows() - 1), RowType: common.RowTypeInsert}
				partition, _, err := router.GetPartitionGenerator("test", "t").GeneratePartitionIndexAndKey(&after, 16, e.TableInfo, e.CommitTs)
				require.NoError(t, err)
				consumerWriter.checkPartition(e, partition, 0, messages[i].RowType)
			}

			cfg := mysqlsink.New()
			require.False(t, cfg.SafeMode)
			require.True(t, cfg.BatchDMLEnable)
			cfg.DryRun = true
			id := common.NewChangeFeedIDWithName("avro-update-test", common.DefaultKeyspaceName)
			stats := metrics.NewStatistics(id, common.DefaultKeyspaceID, "mysql")
			defer stats.Close()
			writer := mysqlsink.NewWriter(t.Context(), -1, nil, cfg, id, stats, nil)
			defer writer.Close()
			if together {
				require.NoError(t, writer.Flush(events))
			} else {
				for _, e := range events {
					require.NoError(t, writer.Flush([]*commonEvent.DMLEvent{e}))
				}
			}
		})
	}
}
