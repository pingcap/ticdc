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
	"context"
	"testing"

	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

// recordingSink is a minimal sink.Sink implementation that records which DDLs are executed.
//
// It lets unit tests validate consumer-side DDL flushing behavior without requiring a real downstream.
type recordingSink struct {
	ddls []string
}

var _ sink.Sink = (*recordingSink)(nil)

func (s *recordingSink) SinkType() common.SinkType { return common.MysqlSinkType }
func (s *recordingSink) IsNormal() bool            { return true }
func (s *recordingSink) AddDMLEvent(_ *commonEvent.DMLEvent) {
}

func (s *recordingSink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	if ddl, ok := event.(*commonEvent.DDLEvent); ok {
		s.ddls = append(s.ddls, ddl.Query)
	}
	return nil
}

func (s *recordingSink) AddCheckpointTs(_ uint64) {
}

func (s *recordingSink) SetTableSchemaStore(_ *commonEvent.TableSchemaStore) {
}

func (s *recordingSink) Close(_ bool) {
}
func (s *recordingSink) Run(_ context.Context) error { return nil }

func TestWriterWrite_executesDDLWithNoBlockedTablesWithoutWatermark(t *testing.T) {
	// Scenario: If upstream resolved-ts is held back (e.g. failpoints in integration tests), the consumer
	// watermark can stall below CREATE TABLE / CREATE DATABASE commitTs. These DDLs don't block any table's
	// DML ordering and should still be applied to advance downstream schema.
	//
	// Steps:
	// 1) Enqueue a "no blocked tables" DDL with commitTs > watermark.
	// 2) Call writer.Write and expect the DDL is executed even without watermark catching up.
	ctx := context.Background()
	s := &recordingSink{}
	w := &writer{
		progresses: []*partitionProgress{
			{partition: 0, watermark: 0},
		},
		mysqlSink: s,
	}
	w.ddlList = []*commonEvent.DDLEvent{
		{
			Query:      "CREATE TABLE `test`.`t` (`id` INT PRIMARY KEY)",
			SchemaName: "test",
			TableName:  "t",
			Type:       byte(timodel.ActionCreateTable),
			FinishedTs: 100,
			BlockedTables: &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				TableIDs:      nil,
			},
		},
	}

	w.Write(ctx, codeccommon.MessageTypeDDL)

	require.Equal(t, []string{"CREATE TABLE `test`.`t` (`id` INT PRIMARY KEY)"}, s.ddls)
	require.Empty(t, w.ddlList)
}

func TestWriterWrite_preservesOrderWhenBlockedDDLNotReady(t *testing.T) {
	// Scenario: DDLs must execute in commitTs order. A later non-blocking DDL must not bypass an earlier
	// blocking DDL that is waiting for watermark.
	//
	// Steps:
	// 1) Enqueue a blocking DDL followed by a non-blocking DDL, with watermark behind the first DDL.
	// 2) Call writer.Write and expect nothing executes.
	// 3) Advance watermark beyond the first DDL and expect both execute in order.
	ctx := context.Background()
	s := &recordingSink{}
	p := &partitionProgress{partition: 0, watermark: 0}
	w := &writer{
		progresses: []*partitionProgress{p},
		mysqlSink:  s,
	}
	w.ddlList = []*commonEvent.DDLEvent{
		{
			Query:      "ALTER TABLE `test`.`t` ADD COLUMN `c2` INT",
			SchemaName: "test",
			TableName:  "t",
			Type:       byte(timodel.ActionAddColumn),
			FinishedTs: 100,
			BlockedTables: &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				TableIDs:      []int64{1},
			},
		},
		{
			Query:      "CREATE TABLE `test`.`t2` (`id` INT PRIMARY KEY)",
			SchemaName: "test",
			TableName:  "t2",
			Type:       byte(timodel.ActionCreateTable),
			FinishedTs: 110,
			BlockedTables: &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				TableIDs:      nil,
			},
		},
	}

	w.Write(ctx, codeccommon.MessageTypeDDL)
	require.Empty(t, s.ddls)
	require.Len(t, w.ddlList, 2)

	p.watermark = 200
	w.Write(ctx, codeccommon.MessageTypeDDL)
	require.Equal(t, []string{
		"ALTER TABLE `test`.`t` ADD COLUMN `c2` INT",
		"CREATE TABLE `test`.`t2` (`id` INT PRIMARY KEY)",
	}, s.ddls)
	require.Empty(t, w.ddlList)
}
