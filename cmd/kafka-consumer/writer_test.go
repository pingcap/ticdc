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

func TestWriterWrite_executesIndependentCreateTableWithoutWatermark(t *testing.T) {
	// Scenario: In some integration tests the upstream intentionally pauses dispatcher creation, which can
	// stall resolved-ts (consumer watermark) below the commitTs of CREATE TABLE / CREATE DATABASE DDLs.
	//
	// Steps:
	// 1) Enqueue an "independent" CREATE TABLE DDL (i.e. it does not depend on any existing table) with
	//    commitTs > watermark.
	// 2) Call writer.Write and expect the DDL is executed to advance downstream schema even without the
	//    watermark catching up.
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
				// DDLSpanTableID is always present; having only it means the DDL does not block any
				// existing table's DML ordering (unlike CREATE TABLE ... LIKE ...).
				TableIDs: []int64{common.DDLSpanTableID},
			},
		},
	}

	w.Write(ctx, codeccommon.MessageTypeDDL)

	require.Equal(t, []string{"CREATE TABLE `test`.`t` (`id` INT PRIMARY KEY)"}, s.ddls)
	require.Empty(t, w.ddlList)
}

func TestWriterWrite_preservesOrderWhenBlockedDDLNotReady(t *testing.T) {
	// Scenario: DDLs must be executed in commitTs order. If an earlier DDL requires watermark gating,
	// later "independent" CREATE TABLE DDLs must not leapfrog it.
	//
	// Steps:
	// 1) Enqueue a blocking DDL followed by an independent CREATE TABLE DDL, with watermark behind the first DDL.
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
				TableIDs:      []int64{common.DDLSpanTableID, 1},
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
				TableIDs:      []int64{common.DDLSpanTableID},
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

func TestWriterWrite_doesNotBypassWatermarkForCreateTableLike(t *testing.T) {
	// Scenario: CREATE TABLE ... LIKE ... depends on the referenced table schema being present and
	// up-to-date downstream, so it must not bypass watermark gating.
	//
	// Steps:
	// 1) Enqueue a CREATE TABLE ... LIKE ... DDL with commitTs > watermark.
	// 2) Call writer.Write and expect the DDL is NOT executed.
	// 3) Advance watermark beyond the DDL commitTs and expect the DDL executes.
	ctx := context.Background()
	s := &recordingSink{}
	p := &partitionProgress{partition: 0, watermark: 0}
	w := &writer{
		progresses: []*partitionProgress{p},
		mysqlSink:  s,
	}
	w.ddlList = []*commonEvent.DDLEvent{
		{
			Query:      "CREATE TABLE `test`.`t2` LIKE `test`.`t1`",
			SchemaName: "test",
			TableName:  "t2",
			Type:       byte(timodel.ActionCreateTable),
			FinishedTs: 100,
			BlockedTables: &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				// Besides the special DDL span, this DDL also blocks the referenced table (or its partitions).
				TableIDs: []int64{common.DDLSpanTableID, 101},
			},
			BlockedTableNames: []commonEvent.SchemaTableName{
				{SchemaName: "test", TableName: "t1"},
			},
		},
	}

	w.Write(ctx, codeccommon.MessageTypeDDL)
	require.Empty(t, s.ddls)
	require.Len(t, w.ddlList, 1)

	p.watermark = 200
	w.Write(ctx, codeccommon.MessageTypeDDL)
	require.Equal(t, []string{"CREATE TABLE `test`.`t2` LIKE `test`.`t1`"}, s.ddls)
	require.Empty(t, w.ddlList)
}

func TestWriterWrite_handlesOutOfOrderDDLsByCommitTs(t *testing.T) {
	// Scenario: In real Kafka topics, DDL messages can be received out of commit-ts order. For example,
	// a "future" CREATE TABLE might be observed before an earlier ALTER TABLE.
	//
	// Steps:
	// 1) Provide a ddlList whose slice order is out of commit-ts order, and set watermark such that a
	//    later DDL at the front is not yet eligible (commitTs > watermark).
	// 2) Call writer.Write and expect all DDLs with commitTs <= watermark execute (in commit-ts order),
	//    and only the truly "future" DDL remains pending.
	ctx := context.Background()
	s := &recordingSink{}
	p := &partitionProgress{partition: 0, watermark: 944040962}
	w := &writer{
		progresses: []*partitionProgress{p},
		mysqlSink:  s,
	}
	w.ddlList = []*commonEvent.DDLEvent{
		{
			Query:      "CREATE TABLE `common_1`.`add_and_drop_columns` (`id` INT(11) NOT NULL PRIMARY KEY)",
			SchemaName: "common_1",
			TableName:  "add_and_drop_columns",
			Type:       byte(timodel.ActionCreateTable),
			FinishedTs: 786754590,
			BlockedTables: &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
			},
		},
		{
			Query:      "CREATE DATABASE `common`",
			SchemaName: "common",
			Type:       byte(timodel.ActionCreateSchema),
			FinishedTs: 931195931,
			BlockedTables: &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
			},
		},
		{
			// This DDL is just barely in the future of watermark, and would block later DDLs if we
			// execute in slice order instead of commit-ts order.
			Query:      "CREATE TABLE `common_1`.`a` (`a` BIGINT PRIMARY KEY,`b` INT)",
			SchemaName: "common_1",
			TableName:  "a",
			Type:       byte(timodel.ActionCreateTable),
			FinishedTs: 944040963,
			BlockedTables: &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
			},
		},
		{
			Query:      "ALTER TABLE `common_1`.`add_and_drop_columns` ADD COLUMN `col1` INT NULL, ADD COLUMN `col2` INT NULL, ADD COLUMN `col3` INT NULL",
			SchemaName: "common_1",
			TableName:  "add_and_drop_columns",
			Type:       byte(timodel.ActionAddColumn),
			FinishedTs: 852290601,
			BlockedTables: &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				TableIDs:      []int64{9},
			},
		},
		{
			Query:      "ALTER TABLE `common_1`.`add_and_drop_columns` DROP COLUMN `col1`, DROP COLUMN `col2`",
			SchemaName: "common_1",
			TableName:  "add_and_drop_columns",
			Type:       byte(timodel.ActionDropColumn),
			FinishedTs: 904719361,
			BlockedTables: &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				TableIDs:      []int64{9},
			},
		},
	}

	w.Write(ctx, codeccommon.MessageTypeDDL)

	require.Equal(t, []string{
		"CREATE TABLE `common_1`.`add_and_drop_columns` (`id` INT(11) NOT NULL PRIMARY KEY)",
		"ALTER TABLE `common_1`.`add_and_drop_columns` ADD COLUMN `col1` INT NULL, ADD COLUMN `col2` INT NULL, ADD COLUMN `col3` INT NULL",
		"ALTER TABLE `common_1`.`add_and_drop_columns` DROP COLUMN `col1`, DROP COLUMN `col2`",
		"CREATE DATABASE `common`",
	}, s.ddls)
	require.Len(t, w.ddlList, 1)
	require.Equal(t, "CREATE TABLE `common_1`.`a` (`a` BIGINT PRIMARY KEY,`b` INT)", w.ddlList[0].Query)
}
