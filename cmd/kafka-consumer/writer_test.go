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
	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/cmd/util"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter"
	sinkmock "github.com/pingcap/ticdc/downstreamadapter/sink/mock"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func newMockSink(t *testing.T) (*sinkmock.MockSink, *[]string) {
	t.Helper()

	ctrl := gomock.NewController(t)
	s := sinkmock.NewMockSink(ctrl)
	ddls := make([]string, 0)

	s.EXPECT().AddDMLEvent(gomock.Any()).AnyTimes()
	s.EXPECT().WriteBlockEvent(gomock.Any()).DoAndReturn(func(event commonEvent.BlockEvent) error {
		if ddl, ok := event.(*commonEvent.DDLEvent); ok {
			ddls = append(ddls, ddl.Query)
		}
		return nil
	}).AnyTimes()

	return s, &ddls
}

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
	s, ddls := newMockSink(t)
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

	require.Equal(t, []string{"CREATE TABLE `test`.`t` (`id` INT PRIMARY KEY)"}, *ddls)
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
	s, ddls := newMockSink(t)
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
	require.Empty(t, *ddls)
	require.Len(t, w.ddlList, 2)

	p.watermark = 200
	w.Write(ctx, codeccommon.MessageTypeDDL)
	require.Equal(t, []string{
		"ALTER TABLE `test`.`t` ADD COLUMN `c2` INT",
		"CREATE TABLE `test`.`t2` (`id` INT PRIMARY KEY)",
	}, *ddls)
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
	s, ddls := newMockSink(t)
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
	require.Empty(t, *ddls)
	require.Len(t, w.ddlList, 1)

	p.watermark = 200
	w.Write(ctx, codeccommon.MessageTypeDDL)
	require.Equal(t, []string{"CREATE TABLE `test`.`t2` LIKE `test`.`t1`"}, *ddls)
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
	s, ddls := newMockSink(t)
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
	}, *ddls)
	require.Len(t, w.ddlList, 1)
	require.Equal(t, "CREATE TABLE `common_1`.`a` (`a` BIGINT PRIMARY KEY,`b` INT)", w.ddlList[0].Query)
}

func TestWriterWrite_sortsOutOfOrderDMLByWatermark(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	s := sinkmock.NewMockSink(ctrl)
	flushedCommitTs := make([]uint64, 0)
	flushedRowTypeCounts := make([]int, 0)
	s.EXPECT().AddDMLEvent(gomock.Any()).Do(func(event *commonEvent.DMLEvent) {
		flushedCommitTs = append(flushedCommitTs, event.GetCommitTs())
		flushedRowTypeCounts = append(flushedRowTypeCounts, len(event.RowTypes))
		event.PostFlush()
	}).Times(2)

	replicaCfg := config.GetDefaultReplicaConfig()
	eventRouter, err := eventrouter.NewEventRouter(replicaCfg.Sink, "test-topic", false, false)
	require.NoError(t, err)

	p := &partitionProgress{
		partition:   0,
		eventsGroup: make(map[int64]*util.EventsGroup),
		watermark:   0,
	}
	w := &writer{
		progresses:  []*partitionProgress{p},
		mysqlSink:   s,
		eventRouter: eventRouter,
		protocol:    config.ProtocolOpen,
	}

	for _, item := range []struct {
		message *codeccommon.DMLMessage
		offset  kafka.Offset
	}{
		{newDMLMessageForWriterTest(20), kafka.Offset(1)},
		{newDMLMessageForWriterTest(10), kafka.Offset(2)},
		{newDMLMessageForWriterTest(20), kafka.Offset(3)},
	} {
		require.NoError(t, w.appendMessage2Group(attachDMLMessageDataForWriterTest(item.message), p, item.offset))
	}

	p.watermark = 20
	needCommit, err := w.Write(ctx, codeccommon.MessageTypeResolved)
	require.NoError(t, err)
	require.True(t, needCommit)
	require.Equal(t, []uint64{10, 20}, flushedCommitTs)
	require.Equal(t, []int{1, 2}, flushedRowTypeCounts)
}

func TestPartitionDDLFlushOrder(t *testing.T) {
	const (
		logicalTableID   = int64(100)
		physicalTableID  = int64(101)
		unrelatedTableID = int64(102)
	)

	ctrl := gomock.NewController(t)
	s := sinkmock.NewMockSink(ctrl)
	order := make([]string, 0, 2)
	s.EXPECT().AddDMLEvent(gomock.Any()).Do(func(event *commonEvent.DMLEvent) {
		order = append(order, "dml")
		event.PostFlush()
	})
	s.EXPECT().WriteBlockEvent(gomock.Any()).DoAndReturn(func(commonEvent.BlockEvent) error {
		order = append(order, "ddl")
		return nil
	})

	newMessage := func(tableID int64, table string) *codeccommon.DMLMessage {
		return codeccommon.NewDMLMessage(tableID, "test", table, 10, common.RowTypeInsert, func() *commonEvent.DMLEvent {
			return &commonEvent.DMLEvent{
				PhysicalTableID: tableID,
				CommitTs:        10,
				RowTypes:        []common.RowType{common.RowTypeInsert},
				Rows:            chunk.NewChunkWithCapacity(nil, 0),
				TableInfo: &common.TableInfo{
					TableName: common.TableName{Schema: "test", Table: table, TableID: tableID},
				},
			}
		})
	}

	partitionGroup := util.NewEventsGroup(1, physicalTableID)
	partitionGroup.AppendMessage(newMessage(physicalTableID, "members"))
	unrelatedGroup := util.NewEventsGroup(1, unrelatedTableID)
	unrelatedGroup.AppendMessage(newMessage(unrelatedTableID, "other"))

	w := &writer{
		progresses: []*partitionProgress{
			{
				partition: 0,
				decoder: &tableIDDecoder{
					tableIDs: []int64{logicalTableID, physicalTableID},
				},
				eventsGroup: make(map[int64]*util.EventsGroup),
			},
			{
				partition: 1,
				eventsGroup: map[int64]*util.EventsGroup{
					physicalTableID:  partitionGroup,
					unrelatedTableID: unrelatedGroup,
				},
			},
		},
		mysqlSink:              s,
		partitionTableAccessor: codeccommon.NewPartitionTableAccessor(),
	}
	w.partitionTableAccessor.Add("test", "members")

	err := w.flushDDLEvent(context.Background(), &commonEvent.DDLEvent{
		Query:      "ALTER TABLE members DROP PARTITION p0",
		SchemaName: "test",
		TableName:  "members",
		Type:       byte(timodel.ActionDropTablePartition),
		FinishedTs: 20,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{logicalTableID},
		},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"dml", "ddl"}, order)
	require.Empty(t, partitionGroup.GetAllMessages())
	require.Len(t, unrelatedGroup.GetAllMessages(), 1)
}

func TestWriteMessageIgnoresFallbackDMLBelowGlobalWatermark(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	s := sinkmock.NewMockSink(ctrl)
	s.EXPECT().AddDMLEvent(gomock.Any()).Times(0)

	progress := &partitionProgress{
		partition:   0,
		eventsGroup: make(map[int64]*util.EventsGroup),
		watermark:   20,
		decoder:     util.NewDMLMessageDecoder(&singleDMLDecoder{message: newDMLMessageForWriterTest(10)}),
	}
	w := &writer{
		progresses:      []*partitionProgress{progress},
		mysqlSink:       s,
		protocol:        config.ProtocolOpen,
		maxBatchSize:    64,
		maxMessageBytes: 1,
	}

	needCommit, err := w.WriteMessage(ctx, &kafka.Message{
		TopicPartition: kafka.TopicPartition{Partition: 0, Offset: kafka.Offset(10)},
	})
	require.NoError(t, err)

	require.False(t, needCommit)
	require.Nil(t, progress.eventsGroup[1])
}

func TestAppendMessageKeepsFallbackDMLAboveGlobalWatermark(t *testing.T) {
	replicaCfg := config.GetDefaultReplicaConfig()
	eventRouter, err := eventrouter.NewEventRouter(replicaCfg.Sink, "test-topic", false, false)
	require.NoError(t, err)

	progress := &partitionProgress{
		partition:   0,
		eventsGroup: make(map[int64]*util.EventsGroup),
		watermark:   20,
	}
	w := &writer{
		progresses: []*partitionProgress{
			progress,
			{partition: 1, watermark: 5},
		},
		eventRouter: eventRouter,
		protocol:    config.ProtocolOpen,
	}

	message := newDMLMessageForWriterTest(10)
	require.NoError(t, w.appendMessage2Group(attachDMLMessageDataForWriterTest(message), progress, kafka.Offset(10)))

	require.NotNil(t, progress.eventsGroup[1])
	resolved, err := progress.eventsGroup[1].ResolveInto(20, nil)
	require.NoError(t, err)
	require.Len(t, resolved, 1)
	require.Equal(t, uint64(10), resolved[0].GetCommitTs())
}

func TestOnDDLMarksRoutedCreateTableLikePartitionTableForAvro(t *testing.T) {
	replicaCfg := config.GetDefaultReplicaConfig()
	eventRouter, err := eventrouter.NewEventRouter(replicaCfg.Sink, "test-topic", false, true)
	require.NoError(t, err)

	w := &writer{
		progresses:             []*partitionProgress{{partition: 0, eventsGroup: make(map[int64]*util.EventsGroup)}},
		eventRouter:            eventRouter,
		protocol:               config.ProtocolAvro,
		partitionTableAccessor: codeccommon.NewPartitionTableAccessor(),
	}

	ddl := &commonEvent.DDLEvent{
		Query:      "CREATE TABLE `target`.`dst` LIKE `target`.`src`",
		SchemaName: "source",
		TableName:  "dst",
		Type:       byte(timodel.ActionCreateTable),
		TableInfo: &common.TableInfo{
			TableName: common.TableName{
				Schema:       "source",
				Table:        "dst",
				IsPartition:  true,
				TargetSchema: "target",
				TargetTable:  "dst",
			},
		},
	}
	w.onDDL(ddl)
	require.True(t, w.partitionTableAccessor.IsPartitionTable("target", "dst"))

	newDMLEvent := func(commitTs uint64) *commonEvent.DMLEvent {
		return &commonEvent.DMLEvent{
			PhysicalTableID: 1,
			CommitTs:        commitTs,
			RowTypes:        []common.RowType{common.RowTypeUpdate},
			Rows:            chunk.NewChunkWithCapacity(nil, 0),
			TableInfo: &common.TableInfo{
				TableName: common.TableName{Schema: "target", Table: "dst"},
			},
		}
	}

	progress := w.progresses[0]
	first := codeccommon.NewDMLMessageFromEvent(newDMLEvent(200))
	second := codeccommon.NewDMLMessageFromEvent(newDMLEvent(100))
	require.NoError(t, w.appendMessage2Group(attachDMLMessageDataForWriterTest(first), progress, kafka.Offset(10)))
	require.NoError(t, w.appendMessage2Group(attachDMLMessageDataForWriterTest(second), progress, kafka.Offset(11)))

	resolved, err := progress.eventsGroup[1].ResolveInto(150, nil)
	require.NoError(t, err)
	require.Len(t, resolved, 1)
	require.Equal(t, uint64(100), resolved[0].GetCommitTs())
}

func TestAppendRow2GroupKeepsDebeziumPartitionTableFallback(t *testing.T) {
	for _, protocol := range []config.Protocol{
		config.ProtocolDebezium,
		config.ProtocolDebeziumAvro,
	} {
		t.Run(protocol.String(), func(t *testing.T) {
			replicaCfg := config.GetDefaultReplicaConfig()
			eventRouter, err := eventrouter.NewEventRouter(replicaCfg.Sink, "test-topic", false, false)
			require.NoError(t, err)

			w := &writer{
				progresses:             []*partitionProgress{{partition: 0, eventsGroup: make(map[int64]*util.EventsGroup)}},
				eventRouter:            eventRouter,
				protocol:               protocol,
				partitionTableAccessor: codeccommon.NewPartitionTableAccessor(),
			}

			w.partitionTableAccessor.Add("target", "src")
			ddl := &commonEvent.DDLEvent{
				Query:      "CREATE TABLE `target`.`dst` LIKE `target`.`src`",
				SchemaName: "target",
				TableName:  "dst",
				Type:       byte(timodel.ActionCreateTable),
			}
			w.onDDL(ddl)
			require.True(t, w.partitionTableAccessor.IsPartitionTable("target", "dst"))

			newDMLEvent := func(commitTs uint64) *commonEvent.DMLEvent {
				return &commonEvent.DMLEvent{
					PhysicalTableID: 1,
					CommitTs:        commitTs,
					RowTypes:        []common.RowType{common.RowTypeUpdate},
					Rows:            chunk.NewChunkWithCapacity(nil, 0),
					TableInfo: &common.TableInfo{
						TableName: common.TableName{Schema: "target", Table: "dst"},
					},
				}
			}

			progress := w.progresses[0]
			first := codeccommon.NewDMLMessageFromEvent(newDMLEvent(200))
			second := codeccommon.NewDMLMessageFromEvent(newDMLEvent(100))
			require.NoError(t, w.appendMessage2Group(attachDMLMessageDataForWriterTest(first), progress, kafka.Offset(10)))
			require.NoError(t, w.appendMessage2Group(attachDMLMessageDataForWriterTest(second), progress, kafka.Offset(11)))

			resolved, err := progress.eventsGroup[1].ResolveInto(150, nil)
			require.NoError(t, err)
			require.Len(t, resolved, 1)
			require.Equal(t, uint64(100), resolved[0].GetCommitTs())
		})
	}
}

func newDMLMessageForWriterTest(commitTs uint64) *codeccommon.DMLMessage {
	return codeccommon.NewDMLMessage(1, "test", "t", commitTs, common.RowTypeUpdate, func() *commonEvent.DMLEvent {
		return &commonEvent.DMLEvent{
			PhysicalTableID: 1,
			StartTs:         commitTs - 1,
			CommitTs:        commitTs,
			RowTypes:        []common.RowType{common.RowTypeUpdate},
			Rows:            chunk.NewChunkWithCapacity(nil, 0),
			TableInfo: &common.TableInfo{
				TableName: common.TableName{Schema: "test", Table: "t", TableID: 1},
			},
		}
	})
}

func attachDMLMessageDataForWriterTest(message *codeccommon.DMLMessage) *codeccommon.DMLMessage {
	messageData := codeccommon.NewDMLMessageData(nil, nil,
		func([]byte, uint64) (*codeccommon.DMLMessage, error) {
			return message, nil
		},
	)
	messageData.AttachDMLMessage(message)
	return message
}

type singleDMLDecoder struct {
	message  *codeccommon.DMLMessage
	consumed bool
}

type tableIDDecoder struct {
	codeccommon.Decoder
	tableIDs []int64
}

func (d *tableIDDecoder) GetTableIDs(string, string) []int64 {
	return d.tableIDs
}

func (d *singleDMLDecoder) AddKeyValue(_, _ []byte) {
}

func (d *singleDMLDecoder) HasNext() (codeccommon.MessageType, bool) {
	return codeccommon.MessageTypeRow, !d.consumed
}

func (d *singleDMLDecoder) NextResolvedEvent() uint64 {
	return 0
}

func (d *singleDMLDecoder) NextDMLMessage() *codeccommon.DMLMessage {
	d.consumed = true
	return d.message
}

func (d *singleDMLDecoder) NextDDLEvent() *commonEvent.DDLEvent {
	return nil
}
