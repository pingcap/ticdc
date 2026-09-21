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

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/cmd/util"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter"
	sinkmock "github.com/pingcap/ticdc/downstreamadapter/sink/mock"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// drainResolvePipeline waits until everything submitted to the resolve pipeline
// reached the sink.
func drainResolvePipeline(t *testing.T, w *writer) {
	t.Helper()
	resume := w.pipeline.pause(t.Context())
	resume()
}

// newTestWriter builds a writer whose resolve pipeline is running, so tests
// exercise the same path the consumer runs.
func newTestWriter(t *testing.T, w *writer) *writer {
	t.Helper()

	// The read loop and the resolve pipeline reach the spill store through
	// getSpillStore, so create it before the pipeline starts: creating it
	// lazily from two goroutines is a data race.
	if w.spillStore == nil {
		w.spillStore = util.NewSpillStore()
	}
	w.pipeline = newPipeline(w)
	w.pipeline.run(t.Context())
	t.Cleanup(w.pipeline.stop)
	return w
}

func newMockSink(t *testing.T) (*sinkmock.MockSink, *[]string) {
	t.Helper()

	ctrl := gomock.NewController(t)
	s := sinkmock.NewMockSink(ctrl)
	ddls := make([]string, 0)

	// Behave like a sink that applies immediately: the resolve pipeline waits for
	// the flush callback before it acknowledges a batch.
	s.EXPECT().AddDMLEvent(gomock.Any()).Do(func(event *commonEvent.DMLEvent) {
		event.PostFlush()
	}).AnyTimes()
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
	ctx := t.Context()
	s, ddls := newMockSink(t)
	w := newTestWriter(t, &writer{
		progresses: []*partitionProgress{
			{partition: 0, watermark: 0},
		},
		mysqlSink: s,
	})
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

	w.Write(ctx, codecCommon.MessageTypeDDL)

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
	ctx := t.Context()
	s, ddls := newMockSink(t)
	p := &partitionProgress{partition: 0, watermark: 0}
	w := newTestWriter(t, &writer{
		progresses: []*partitionProgress{p},
		mysqlSink:  s,
	})
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

	w.Write(ctx, codecCommon.MessageTypeDDL)
	require.Empty(t, *ddls)
	require.Len(t, w.ddlList, 2)

	p.watermark = 200
	w.Write(ctx, codecCommon.MessageTypeDDL)
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
	ctx := t.Context()
	s, ddls := newMockSink(t)
	p := &partitionProgress{partition: 0, watermark: 0}
	w := newTestWriter(t, &writer{
		progresses: []*partitionProgress{p},
		mysqlSink:  s,
	})
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

	w.Write(ctx, codecCommon.MessageTypeDDL)
	require.Empty(t, *ddls)
	require.Len(t, w.ddlList, 1)

	p.watermark = 200
	w.Write(ctx, codecCommon.MessageTypeDDL)
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
	ctx := t.Context()
	s, ddls := newMockSink(t)
	p := &partitionProgress{partition: 0, watermark: 944040962}
	w := newTestWriter(t, &writer{
		progresses: []*partitionProgress{p},
		mysqlSink:  s,
	})
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

	w.Write(ctx, codecCommon.MessageTypeDDL)

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
	ctx := t.Context()
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
	eventRouter, err := eventrouter.NewEventRouter(replicaCfg.Sink, false, "test-topic", false, false)
	require.NoError(t, err)

	p := &partitionProgress{
		partition:   0,
		eventsGroup: make(map[int64]*util.EventsGroup),
		watermark:   0,
	}
	w := newTestWriter(t, &writer{
		progresses:  []*partitionProgress{p},
		mysqlSink:   s,
		eventRouter: eventRouter,
		protocol:    config.ProtocolOpen,
	})

	for _, item := range []struct {
		message *codecCommon.DMLMessage
		offset  int64
	}{
		{newDMLMessageForWriterTest(20), 1},
		{newDMLMessageForWriterTest(10), 2},
		{newDMLMessageForWriterTest(20), 3},
	} {
		require.NoError(t, w.appendMessage2Group(attachDMLMessageDataForWriterTest(item.message), p, item.offset))
	}

	p.watermark = 20
	// The read loop publishes the watermark of the partition it just read from a
	// resolved message, and that is what the resolve pipeline may apply up to.
	w.publishWatermark()
	needCommit, err := w.Write(ctx, codecCommon.MessageTypeResolved)
	require.NoError(t, err)
	require.True(t, needCommit)
	// The resolve pipeline applies events off the read loop, so drain it before
	// checking what the sink received.
	drainResolvePipeline(t, w)
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
	}).Times(2)
	s.EXPECT().WriteBlockEvent(gomock.Any()).DoAndReturn(func(commonEvent.BlockEvent) error {
		order = append(order, "ddl")
		return nil
	})

	newMessage := func(tableID int64, table string) *codecCommon.DMLMessage {
		message := codecCommon.NewDMLMessage(tableID, "test", table, 10, common.RowTypeInsert, func() *commonEvent.DMLEvent {
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
		data := codecCommon.NewDMLMessageData(nil, nil, func([]byte) ([]*codecCommon.DMLMessage, error) {
			return []*codecCommon.DMLMessage{message}, nil
		})
		data.AttachDMLMessage(message)
		return message
	}

	partitionGroup := util.NewEventsGroup(1, physicalTableID)
	require.NoError(t, partitionGroup.AppendMessage(newMessage(physicalTableID, "members")))
	unrelatedGroup := util.NewEventsGroup(1, unrelatedTableID)
	require.NoError(t, unrelatedGroup.AppendMessage(newMessage(unrelatedTableID, "other")))

	w := newTestWriter(t, &writer{
		progresses: []*partitionProgress{
			{
				partition: 0,
				decoder: util.NewDMLMessageDecoder(&tableIDDecoder{
					tableIDs: []int64{logicalTableID, physicalTableID},
				}),
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
		partitionTableAccessor: codecCommon.NewPartitionTableAccessor(),
	})
	w.partitionTableAccessor.Add("test", "members")

	err := w.flushDDLEvent(t.Context(), &commonEvent.DDLEvent{
		Query:      "ALTER TABLE members DROP PARTITION p0",
		SchemaName: "test",
		TableName:  "members",
		Type:       byte(timodel.ActionDropTablePartition),
		FinishedTs: 20,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{logicalTableID},
		},
	}, true)
	require.NoError(t, err)
	// Every event below the DDL commit ts is applied before the DDL, also for a
	// table the upstream did not mark as blocked: the blocked tables are
	// reconstructed from the ids the decoder allocated itself, so they can miss a
	// table whose events the consumer has not decoded yet.
	require.Len(t, order, 3)
	require.Equal(t, []string{"ddl"}, order[2:])
	partitionMessages, err := partitionGroup.GetAllMessages()
	require.NoError(t, err)
	require.Empty(t, partitionMessages)
	unrelatedMessages, err := unrelatedGroup.GetAllMessages()
	require.NoError(t, err)
	require.Empty(t, unrelatedMessages)
}

func TestWriteMessageIgnoresFallbackDMLBelowGlobalWatermark(t *testing.T) {
	ctx := t.Context()
	ctrl := gomock.NewController(t)
	s := sinkmock.NewMockSink(ctrl)
	s.EXPECT().AddDMLEvent(gomock.Any()).Times(0)

	progress := &partitionProgress{
		partition:   0,
		eventsGroup: make(map[int64]*util.EventsGroup),
		watermark:   20,
		decoder:     util.NewDMLMessageDecoder(&singleDMLDecoder{message: newDMLMessageForWriterTest(10)}),
	}
	w := newTestWriter(t, &writer{
		progresses:      []*partitionProgress{progress},
		mysqlSink:       s,
		protocol:        config.ProtocolOpen,
		maxBatchSize:    64,
		maxMessageBytes: 1,
	})

	err := w.WriteMessage(ctx, &kgo.Record{Partition: 0, Offset: 10})
	require.NoError(t, err)
	require.Nil(t, progress.eventsGroup[1])
}

func TestAppendMessageKeepsFallbackDMLAboveGlobalWatermark(t *testing.T) {
	replicaCfg := config.GetDefaultReplicaConfig()
	eventRouter, err := eventrouter.NewEventRouter(replicaCfg.Sink, false, "test-topic", false, false)
	require.NoError(t, err)

	progress := &partitionProgress{
		partition:   0,
		eventsGroup: make(map[int64]*util.EventsGroup),
		watermark:   20,
	}
	w := newTestWriter(t, &writer{
		progresses: []*partitionProgress{
			progress,
			{partition: 1, watermark: 5},
		},
		eventRouter: eventRouter,
		protocol:    config.ProtocolOpen,
	})

	message := newDMLMessageForWriterTest(10)
	require.NoError(t, w.appendMessage2Group(attachDMLMessageDataForWriterTest(message), progress, 10))

	require.NotNil(t, progress.eventsGroup[1])
	resolved, err := progress.eventsGroup[1].ResolveInto(20, nil)
	require.NoError(t, err)
	require.Len(t, resolved, 1)
	require.Equal(t, uint64(10), resolved[0].GetCommitTs())
}

func TestOnDDLMarksRoutedCreateTableLikePartitionTableForAvro(t *testing.T) {
	replicaCfg := config.GetDefaultReplicaConfig()
	eventRouter, err := eventrouter.NewEventRouter(replicaCfg.Sink, false, "test-topic", false, true)
	require.NoError(t, err)

	w := newTestWriter(t, &writer{
		progresses:             []*partitionProgress{{partition: 0, eventsGroup: make(map[int64]*util.EventsGroup)}},
		eventRouter:            eventRouter,
		protocol:               config.ProtocolAvro,
		partitionTableAccessor: codecCommon.NewPartitionTableAccessor(),
	})

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
	first := codecCommon.NewDMLMessageFromEvent(newDMLEvent(200))
	second := codecCommon.NewDMLMessageFromEvent(newDMLEvent(100))
	require.NoError(t, w.appendMessage2Group(attachDMLMessageDataForWriterTest(first), progress, 10))
	require.NoError(t, w.appendMessage2Group(attachDMLMessageDataForWriterTest(second), progress, 11))

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
			eventRouter, err := eventrouter.NewEventRouter(replicaCfg.Sink, false, "test-topic", false, false)
			require.NoError(t, err)

			w := newTestWriter(t, &writer{
				progresses:             []*partitionProgress{{partition: 0, eventsGroup: make(map[int64]*util.EventsGroup)}},
				eventRouter:            eventRouter,
				protocol:               protocol,
				partitionTableAccessor: codecCommon.NewPartitionTableAccessor(),
			})

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
			first := codecCommon.NewDMLMessageFromEvent(newDMLEvent(200))
			second := codecCommon.NewDMLMessageFromEvent(newDMLEvent(100))
			require.NoError(t, w.appendMessage2Group(attachDMLMessageDataForWriterTest(first), progress, 10))
			require.NoError(t, w.appendMessage2Group(attachDMLMessageDataForWriterTest(second), progress, 11))

			resolved, err := progress.eventsGroup[1].ResolveInto(150, nil)
			require.NoError(t, err)
			require.Len(t, resolved, 1)
			require.Equal(t, uint64(100), resolved[0].GetCommitTs())
		})
	}
}

func newDMLMessageForWriterTest(commitTs uint64) *codecCommon.DMLMessage {
	return codecCommon.NewDMLMessage(1, "test", "t", commitTs, common.RowTypeUpdate, func() *commonEvent.DMLEvent {
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

func attachDMLMessageDataForWriterTest(message *codecCommon.DMLMessage) *codecCommon.DMLMessage {
	messageData := codecCommon.NewDMLMessageData(nil, nil,
		func([]byte) ([]*codecCommon.DMLMessage, error) {
			return []*codecCommon.DMLMessage{message}, nil
		},
	)
	messageData.AttachDMLMessage(message)
	return message
}

type singleDMLDecoder struct {
	message  *codecCommon.DMLMessage
	consumed bool
}

type tableIDDecoder struct {
	codecCommon.Decoder
	tableIDs []int64
}

func (d *tableIDDecoder) GetTableIDs(string, string) []int64 {
	return d.tableIDs
}

func (d *singleDMLDecoder) AddKeyValue(_, _ []byte) {
}

func (d *singleDMLDecoder) HasNext() (codecCommon.MessageType, bool) {
	return codecCommon.MessageTypeRow, !d.consumed
}

func (d *singleDMLDecoder) NextResolvedEvent() uint64 {
	return 0
}

func (d *singleDMLDecoder) NextDMLMessage() *codecCommon.DMLMessage {
	d.consumed = true
	return d.message
}

func (d *singleDMLDecoder) NextDDLEvent() *commonEvent.DDLEvent {
	return nil
}

// TestWriteMessageLeavesTheDDLCommitToTheNextWatermark pins the commit rule of
// the DDL path: the DDL is applied while its message is read, but its offset is
// not committed on its own. A record below it in the partition can hold events
// above the DDL commit ts that the resolve pipeline has not applied yet, so
// committing the DDL offset would skip them on a restart. The offset of the next
// resolved message covers every record below it once its watermark was applied.
func TestWriteMessageLeavesTheDDLCommitToTheNextWatermark(t *testing.T) {
	ctx := t.Context()
	ctrl := gomock.NewController(t)
	s := sinkmock.NewMockSink(ctrl)
	s.EXPECT().WriteBlockEvent(gomock.Any()).Return(nil)

	const (
		tableID   = int64(1)
		watermark = uint64(20)
	)
	ddl := &commonEvent.DDLEvent{
		Query:      "ALTER TABLE t ADD COLUMN c INT",
		SchemaName: "test",
		TableName:  "t",
		Type:       byte(timodel.ActionAddColumn),
		FinishedTs: watermark,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{tableID},
		},
	}
	progress := &partitionProgress{
		partition:   0,
		eventsGroup: make(map[int64]*util.EventsGroup),
		watermark:   watermark,
		decoder:     util.NewDMLMessageDecoder(&singleDDLDecoder{ddl: ddl}),
	}
	w := newTestWriter(t, &writer{
		progresses:         []*partitionProgress{progress},
		mysqlSink:          s,
		protocol:           config.ProtocolOpen,
		ddlWithMaxCommitTs: make(map[int64]uint64),
	})

	err := w.WriteMessage(ctx, &kgo.Record{Partition: 0, Offset: 100})
	require.NoError(t, err)
	require.Empty(t, w.pendingCommits)

	// The next resolved message of the partition commits its own offset, which
	// covers the DDL record below it, once the applied watermark reached the
	// watermark that message carries.
	progress.decoder = util.NewDMLMessageDecoder(&singleResolvedDecoder{watermark: watermark})
	err = w.WriteMessage(ctx, &kgo.Record{Partition: 0, Offset: 101})
	require.NoError(t, err)
	require.Len(t, w.pendingCommits, 1)
	require.Equal(t, watermark, w.pendingCommits[0].watermark)
	require.Equal(t, int64(101), w.pendingCommits[0].message.Offset)

	require.Empty(t, w.takeCommittableMessages())
	w.pipeline.appliedWatermarkValue.Store(watermark)
	committable := w.takeCommittableMessages()
	require.Len(t, committable, 1)
	require.Equal(t, int64(101), committable[0].Offset,
		"committing the resolved record advances past the DDL below it")
}

type singleDDLDecoder struct {
	ddl      *commonEvent.DDLEvent
	consumed bool
}

func (d *singleDDLDecoder) AddKeyValue(_, _ []byte) {
}

func (d *singleDDLDecoder) HasNext() (codecCommon.MessageType, bool) {
	return codecCommon.MessageTypeDDL, !d.consumed
}

func (d *singleDDLDecoder) NextResolvedEvent() uint64 {
	return 0
}

func (d *singleDDLDecoder) NextDMLMessage() *codecCommon.DMLMessage {
	return nil
}

func (d *singleDDLDecoder) NextDDLEvent() *commonEvent.DDLEvent {
	d.consumed = true
	return d.ddl
}

// TestWriteMessageResolvedFlushesEligibleDDLBeforePublishingTheWatermark pins
// the DDL flush trigger: a DDL read before its commit ts reached the watermark
// waits in ddlList, and the resolved message that raises the watermark is what
// makes it eligible. It must be applied before the new watermark becomes visible
// to the resolve pipeline, otherwise the pipeline applies a DML above the DDL
// commit ts before the DDL created its downstream table.
func TestWriteMessageResolvedFlushesEligibleDDLBeforePublishingTheWatermark(t *testing.T) {
	ctx := t.Context()
	ctrl := gomock.NewController(t)
	s := sinkmock.NewMockSink(ctrl)

	const (
		tableID   = int64(1)
		watermark = uint64(20)
	)
	var (
		w              *writer
		written        []string
		publishedAtDDL []uint64
	)
	s.EXPECT().WriteBlockEvent(gomock.Any()).DoAndReturn(func(event commonEvent.BlockEvent) error {
		if ddl, ok := event.(*commonEvent.DDLEvent); ok {
			written = append(written, ddl.Query)
			publishedAtDDL = append(publishedAtDDL, w.globalWatermarkValue.Load())
		}
		return nil
	}).AnyTimes()

	ddl := &commonEvent.DDLEvent{
		Query:      "ALTER TABLE t ADD COLUMN c INT",
		SchemaName: "test",
		TableName:  "t",
		Type:       byte(timodel.ActionAddColumn),
		FinishedTs: watermark,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{tableID},
		},
	}
	progress := &partitionProgress{
		partition:   0,
		eventsGroup: make(map[int64]*util.EventsGroup),
		decoder:     util.NewDMLMessageDecoder(&singleDDLDecoder{ddl: ddl}),
	}
	w = newTestWriter(t, &writer{
		progresses:         []*partitionProgress{progress},
		mysqlSink:          s,
		protocol:           config.ProtocolOpen,
		ddlWithMaxCommitTs: make(map[int64]uint64),
	})

	// The DDL is read while the watermark is still below its commit ts, so it
	// waits for the watermark.
	err := w.WriteMessage(ctx, &kgo.Record{Partition: 0, Offset: 0})
	require.NoError(t, err)
	require.Empty(t, written)
	require.Len(t, w.ddlList, 1)

	// The resolved message raises the watermark, which makes the DDL eligible.
	progress.decoder = util.NewDMLMessageDecoder(&singleResolvedDecoder{watermark: watermark})
	err = w.WriteMessage(ctx, &kgo.Record{Partition: 0, Offset: 1})
	require.NoError(t, err)
	require.Equal(t, []string{ddl.Query}, written)
	require.Empty(t, w.ddlList)
	// The DDL ran before the new watermark was published to the resolve pipeline.
	require.Equal(t, []uint64{0}, publishedAtDDL)
	require.Equal(t, watermark, w.globalWatermarkValue.Load())
}

type singleResolvedDecoder struct {
	watermark uint64
	consumed  bool
}

func (d *singleResolvedDecoder) AddKeyValue(_, _ []byte) {
}

func (d *singleResolvedDecoder) HasNext() (codecCommon.MessageType, bool) {
	return codecCommon.MessageTypeResolved, !d.consumed
}

func (d *singleResolvedDecoder) NextResolvedEvent() uint64 {
	d.consumed = true
	return d.watermark
}

func (d *singleResolvedDecoder) NextDMLMessage() *codecCommon.DMLMessage {
	return nil
}

func (d *singleResolvedDecoder) NextDDLEvent() *commonEvent.DDLEvent {
	return nil
}

// TestAppendDDLIgnoresReplay pins the DDL replay filter: the upstream dispatches
// a DDL once per dispatcher, so the same DDL reaches the topic more than once,
// and executing it twice fails downstream with "table already exists", while a
// replayed DML only writes the same rows again.
func TestAppendDDLIgnoresReplay(t *testing.T) {
	w := newTestWriter(t, &writer{
		progresses:         []*partitionProgress{{partition: 0, eventsGroup: make(map[int64]*util.EventsGroup)}},
		ddlWithMaxCommitTs: make(map[int64]uint64),
	})
	ddl := &commonEvent.DDLEvent{
		Query:      "CREATE TABLE `test`.`t` (`id` INT PRIMARY KEY)",
		SchemaName: "test",
		TableName:  "t",
		Type:       byte(timodel.ActionCreateTable),
		FinishedTs: 10,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
		},
	}
	w.appendDDL(ddl)
	w.appendDDL(ddl)
	require.Len(t, w.ddlList, 1, "the second copy of the DDL is a replay")

	// A DDL of the same table at a later commit ts is a new DDL, not a replay.
	next := *ddl
	next.Query = "ALTER TABLE `test`.`t` ADD COLUMN `c` INT"
	next.FinishedTs = 20
	w.appendDDL(&next)
	require.Len(t, w.ddlList, 2)
}
