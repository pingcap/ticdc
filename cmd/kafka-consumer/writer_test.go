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

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pingcap/ticdc/cmd/util"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/chunk"
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

func (s *recordingSink) FlushDMLBeforeBlock(_ commonEvent.BlockEvent) error {
	return nil
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

func TestWriterAppendDDL_keepsCrossObjectDDLSpanOnlyDDLs(t *testing.T) {
	// Scenario: DDLSpanTableID is a shared barrier, not a logical object identity. Cross-object DDLs
	// that only carry the DDL span must not suppress each other before ddlList sorting executes.
	//
	// Steps:
	// 1) Append a newer CREATE SCHEMA DDL that only blocks the DDL span.
	// 2) Append an older independent CREATE TABLE DDL for another object that also only blocks the DDL span.
	// 3) Verify both DDLs stay queued, then call writer.Write and expect commit-ts order is preserved.
	ctx := context.Background()
	s := &recordingSink{}
	p := &partitionProgress{partition: 0, watermark: 300}
	w := &writer{
		progresses:                 []*partitionProgress{p},
		ddlList:                    make([]*commonEvent.DDLEvent, 0),
		ddlOrderKeyWithMaxCommitTs: make(map[ddlOrderKey]uint64),
		mysqlSink:                  s,
	}

	w.appendDDL(&commonEvent.DDLEvent{
		Query:      "CREATE DATABASE `db_newer`",
		SchemaID:   101,
		SchemaName: "db_newer",
		Type:       byte(timodel.ActionCreateSchema),
		FinishedTs: 200,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{common.DDLSpanTableID},
		},
	})
	w.appendDDL(&commonEvent.DDLEvent{
		Query:      "CREATE TABLE `db_existing`.`t_old` (`id` INT PRIMARY KEY)",
		SchemaName: "db_existing",
		TableName:  "t_old",
		Type:       byte(timodel.ActionCreateTable),
		FinishedTs: 100,
		TableInfo: &common.TableInfo{
			TableName: common.TableName{
				Schema:  "db_existing",
				Table:   "t_old",
				TableID: 202,
			},
		},
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{common.DDLSpanTableID},
		},
	})

	require.Len(t, w.ddlList, 2)

	w.Write(ctx, codeccommon.MessageTypeDDL)
	require.Equal(t, []string{
		"CREATE TABLE `db_existing`.`t_old` (`id` INT PRIMARY KEY)",
		"CREATE DATABASE `db_newer`",
	}, s.ddls)
	require.Empty(t, w.ddlList)
}

func TestWriterAppendDDL_keepsCrossObjectCreateDDLsWithEmptyBlockedTableIDs(t *testing.T) {
	// Scenario: Some producer paths decode CREATE SCHEMA / CREATE TABLE with empty blocked table IDs
	// instead of an explicit DDLSpanTableID entry. The consumer must still derive logical ordering keys
	// from schema/table identity so unrelated create DDLs do not suppress each other.
	//
	// Steps:
	// 1) Append a newer CREATE SCHEMA DDL with empty blocked table IDs.
	// 2) Append an older independent CREATE TABLE DDL for another object with empty blocked table IDs.
	// 3) Verify both DDLs stay queued, then call writer.Write and expect commit-ts order is preserved.
	ctx := context.Background()
	s := &recordingSink{}
	p := &partitionProgress{partition: 0, watermark: 300}
	w := &writer{
		progresses:                 []*partitionProgress{p},
		ddlList:                    make([]*commonEvent.DDLEvent, 0),
		ddlOrderKeyWithMaxCommitTs: make(map[ddlOrderKey]uint64),
		mysqlSink:                  s,
	}

	w.appendDDL(&commonEvent.DDLEvent{
		Query:      "CREATE DATABASE `db_newer_empty`",
		SchemaID:   102,
		SchemaName: "db_newer_empty",
		Type:       byte(timodel.ActionCreateSchema),
		FinishedTs: 200,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
		},
	})
	w.appendDDL(&commonEvent.DDLEvent{
		Query:      "CREATE TABLE `db_existing`.`t_old_empty` (`id` INT PRIMARY KEY)",
		SchemaName: "db_existing",
		TableName:  "t_old_empty",
		Type:       byte(timodel.ActionCreateTable),
		FinishedTs: 100,
		TableInfo: &common.TableInfo{
			TableName: common.TableName{
				Schema:  "db_existing",
				Table:   "t_old_empty",
				TableID: 203,
			},
		},
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
		},
	})

	require.Len(t, w.ddlList, 2)

	w.Write(ctx, codeccommon.MessageTypeDDL)
	require.Equal(t, []string{
		"CREATE TABLE `db_existing`.`t_old_empty` (`id` INT PRIMARY KEY)",
		"CREATE DATABASE `db_newer_empty`",
	}, s.ddls)
	require.Empty(t, w.ddlList)
}

func TestWriterAppendDDL_dropsStaleCreateSchemaWithEmptyBlockedTableIDs(t *testing.T) {
	// Scenario: The empty blocked-table-ID path must still reject truly stale DDLs for the same logical
	// schema after the consumer falls back to schema-scoped ordering keys.
	//
	// Steps:
	// 1) Append a newer CREATE SCHEMA DDL with empty blocked table IDs.
	// 2) Append an older CREATE SCHEMA DDL for the same schema with empty blocked table IDs.
	// 3) Verify the older DDL is ignored while the newer one remains queued.
	w := &writer{
		ddlList:                    make([]*commonEvent.DDLEvent, 0),
		ddlOrderKeyWithMaxCommitTs: make(map[ddlOrderKey]uint64),
	}

	w.appendDDL(&commonEvent.DDLEvent{
		Query:      "CREATE DATABASE `db_same_empty`",
		SchemaID:   302,
		SchemaName: "db_same_empty",
		Type:       byte(timodel.ActionCreateSchema),
		FinishedTs: 200,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
		},
	})
	w.appendDDL(&commonEvent.DDLEvent{
		Query:      "CREATE DATABASE `db_same_empty`",
		SchemaID:   302,
		SchemaName: "db_same_empty",
		Type:       byte(timodel.ActionCreateSchema),
		FinishedTs: 100,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
		},
	})

	require.Len(t, w.ddlList, 1)
	require.Equal(t, "CREATE DATABASE `db_same_empty`", w.ddlList[0].Query)
}

func TestWriterAppendDDL_dropsStaleDDLForSameDDLSpanOnlyObject(t *testing.T) {
	// Scenario: The stale-drop protection from #1781 must still reject truly stale DDLs for the same
	// DDLSpan-only object after we stop using DDLSpanTableID as the shared key.
	//
	// Steps:
	// 1) Append a newer CREATE SCHEMA DDL for one schema.
	// 2) Append an older CREATE SCHEMA DDL for the same schema.
	// 3) Verify the older DDL is ignored while the newer one remains queued.
	w := &writer{
		ddlList:                    make([]*commonEvent.DDLEvent, 0),
		ddlOrderKeyWithMaxCommitTs: make(map[ddlOrderKey]uint64),
	}

	w.appendDDL(&commonEvent.DDLEvent{
		Query:      "CREATE DATABASE `db_same`",
		SchemaID:   301,
		SchemaName: "db_same",
		Type:       byte(timodel.ActionCreateSchema),
		FinishedTs: 200,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{common.DDLSpanTableID},
		},
	})
	w.appendDDL(&commonEvent.DDLEvent{
		Query:      "CREATE DATABASE `db_same`",
		SchemaID:   301,
		SchemaName: "db_same",
		Type:       byte(timodel.ActionCreateSchema),
		FinishedTs: 100,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{common.DDLSpanTableID},
		},
	})

	require.Len(t, w.ddlList, 1)
	require.Equal(t, "CREATE DATABASE `db_same`", w.ddlList[0].Query)
}

func TestWriterAppendDDL_dropsStaleDDLForSameBlockedTable(t *testing.T) {
	// Scenario: The #1781 stale-drop protection must still work for DDLs that block a real table, even
	// after we stop using the shared DDL span as the ordering key.
	//
	// Steps:
	// 1) Append a newer ALTER TABLE DDL that blocks a specific table ID.
	// 2) Append an older ALTER TABLE DDL for the same blocked table.
	// 3) Verify the older DDL is ignored while the newer one remains queued.
	w := &writer{
		ddlList:                    make([]*commonEvent.DDLEvent, 0),
		ddlOrderKeyWithMaxCommitTs: make(map[ddlOrderKey]uint64),
	}

	w.appendDDL(&commonEvent.DDLEvent{
		Query:      "ALTER TABLE `test`.`t` ADD COLUMN `c1` INT",
		SchemaName: "test",
		TableName:  "t",
		Type:       byte(timodel.ActionAddColumn),
		FinishedTs: 200,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{common.DDLSpanTableID, 401},
		},
	})
	w.appendDDL(&commonEvent.DDLEvent{
		Query:      "ALTER TABLE `test`.`t` ADD COLUMN `c0` INT",
		SchemaName: "test",
		TableName:  "t",
		Type:       byte(timodel.ActionAddColumn),
		FinishedTs: 100,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{common.DDLSpanTableID, 401},
		},
	})

	require.Len(t, w.ddlList, 1)
	require.Equal(t, "ALTER TABLE `test`.`t` ADD COLUMN `c1` INT", w.ddlList[0].Query)
}

func TestAppendRow2GroupDoesNotDropCommitTsFallbackBeforeApplied(t *testing.T) {
	// Scenario: TiCDC can replay older commitTs after restart or network recovery, so the Kafka consumer
	// must keep fallback commitTs DMLs until the same table's events are already flushed downstream.
	//
	// Steps:
	// 1) Append a newer DML and then an older replayed DML for the same table.
	// 2) Verify the replayed DML is still resolvable before AppliedWatermark advances.
	// 3) Advance AppliedWatermark beyond the replayed commitTs and verify the same replay is ignored.
	replicaCfg := config.GetDefaultReplicaConfig()
	eventRouter, err := eventrouter.NewEventRouter(replicaCfg.Sink, "test-topic", false, false)
	require.NoError(t, err)

	w := &writer{
		progresses:             []*partitionProgress{{partition: 0, eventsGroup: make(map[int64]*util.EventsGroup)}},
		eventRouter:            eventRouter,
		protocol:               config.ProtocolCanalJSON,
		partitionTableAccessor: codeccommon.NewPartitionTableAccessor(),
	}

	newDMLEvent := func(tableID int64, commitTs uint64) *commonEvent.DMLEvent {
		return &commonEvent.DMLEvent{
			PhysicalTableID: tableID,
			CommitTs:        commitTs,
			RowTypes:        []common.RowType{common.RowTypeUpdate},
			Rows:            chunk.NewChunkWithCapacity(nil, 0),
			TableInfo: &common.TableInfo{
				TableName: common.TableName{Schema: "test", Table: "t"},
			},
		}
	}

	progress := w.progresses[0]

	w.appendRow2Group(newDMLEvent(1, 200), progress, kafka.Offset(10))
	w.appendRow2Group(newDMLEvent(1, 100), progress, kafka.Offset(11))

	group := progress.eventsGroup[1]
	require.NotNil(t, group)

	resolved := group.ResolveInto(150, nil)
	require.Len(t, resolved, 1)
	require.Equal(t, uint64(100), resolved[0].CommitTs)

	group.AppliedWatermark = 200
	w.appendRow2Group(newDMLEvent(1, 100), progress, kafka.Offset(12))
	resolved = group.ResolveInto(150, nil)
	require.Empty(t, resolved)
}
