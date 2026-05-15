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
	"math"
	"sync"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter"
	sinkmock "github.com/pingcap/ticdc/downstreamadapter/sink/mock"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func newTestReplayEngine(t *testing.T) (*replayEngine, *[]string) {
	t.Helper()

	ctrl := gomock.NewController(t)
	s := sinkmock.NewMockSink(ctrl)
	ddls := make([]string, 0)
	s.EXPECT().AddDMLEvent(gomock.Any()).AnyTimes()
	s.EXPECT().WriteBlockEvent(gomock.Any()).DoAndReturn(func(event commonEvent.BlockEvent) error {
		if ddl, ok := event.(*commonEvent.DDLEvent); ok {
			ddls = append(ddls, ddl.Query)
		}
		event.PostFlush()
		return nil
	}).AnyTimes()

	cfg := config.GetDefaultReplicaConfig()
	router, err := eventrouter.NewEventRouter(cfg.Sink, "topic", false, false)
	require.NoError(t, err)

	return &replayEngine{
		partitions: []*partitionState{
			{partition: 0, groups: make(map[int64]*dmlGroup)},
		},
		ddlWithMaxCommitTs: make(map[int64]uint64),
		eventRouter:        router,
		protocol:           config.ProtocolCanalJSON,
		mysqlSink:          s,
		offsets:            newOffsetTracker(),
		inflight:           newInflightTracker(),
	}, &ddls
}

func TestAppendDMLBuffersFutureSyncpointData(t *testing.T) {
	ctrl := gomock.NewController(t)
	s := sinkmock.NewMockSink(ctrl)
	flushed := 0
	s.EXPECT().AddDMLEvent(gomock.Any()).DoAndReturn(func(event *commonEvent.DMLEvent) {
		flushed++
		event.PostFlush()
	}).AnyTimes()

	cfg := config.GetDefaultReplicaConfig()
	router, err := eventrouter.NewEventRouter(cfg.Sink, "topic", false, false)
	require.NoError(t, err)

	engine := &replayEngine{
		partitions: []*partitionState{
			{partition: 0, groups: make(map[int64]*dmlGroup)},
		},
		eventRouter: router,
		protocol:    config.ProtocolCanalJSON,
		mysqlSink:   s,
		offsets:     newOffsetTracker(),
		inflight:    newInflightTracker(),
		syncpoint: &syncpointManager{
			enabled:  true,
			nextTs:   100,
			interval: time.Minute,
		},
	}

	progress := engine.partitions[0]
	source90 := engine.offsets.NewSource("topic", 0, kafka.Offset(1))
	require.NoError(t, engine.appendDML(newTestDMLEvent(1, 90), progress, source90, kafka.Offset(1)))
	source90.Close()
	require.Equal(t, 1, flushed)

	source110 := engine.offsets.NewSource("topic", 0, kafka.Offset(2))
	require.NoError(t, engine.appendDML(newTestDMLEvent(1, 110), progress, source110, kafka.Offset(2)))
	source110.Close()
	require.Equal(t, 1, flushed)
	require.Len(t, progress.groups[1].events, 1)

	engine.syncpoint.nextTs = 200
	engine.dispatchBufferedDMLs()
	require.Equal(t, 2, flushed)
	require.Empty(t, progress.groups[1].events)
}

func TestAppendDMLDoesNotSkipSameCommitTsAsAppliedWatermark(t *testing.T) {
	ctrl := gomock.NewController(t)
	s := sinkmock.NewMockSink(ctrl)
	flushed := 0
	s.EXPECT().AddDMLEvent(gomock.Any()).DoAndReturn(func(event *commonEvent.DMLEvent) {
		flushed++
		event.PostFlush()
	}).AnyTimes()

	cfg := config.GetDefaultReplicaConfig()
	router, err := eventrouter.NewEventRouter(cfg.Sink, "topic", false, false)
	require.NoError(t, err)

	group := newDMLGroup(0, 1)
	group.MarkApplied(100)
	engine := &replayEngine{
		partitions: []*partitionState{
			{
				partition: 0,
				groups: map[int64]*dmlGroup{
					1: group,
				},
			},
		},
		eventRouter: router,
		protocol:    config.ProtocolCanalJSON,
		mysqlSink:   s,
		offsets:     newOffsetTracker(),
		inflight:    newInflightTracker(),
	}

	source99 := engine.offsets.NewSource("topic", 0, kafka.Offset(1))
	require.NoError(t, engine.appendDML(newTestDMLEvent(1, 99), engine.partitions[0], source99, kafka.Offset(1)))
	source99.Close()
	require.Equal(t, 0, flushed)

	source100 := engine.offsets.NewSource("topic", 0, kafka.Offset(2))
	require.NoError(t, engine.appendDML(newTestDMLEvent(1, 100), engine.partitions[0], source100, kafka.Offset(2)))
	source100.Close()
	require.Equal(t, 1, flushed)
}

func TestHandleMessageAllowsConcurrentPartitionDecode(t *testing.T) {
	ctrl := gomock.NewController(t)
	s := sinkmock.NewMockSink(ctrl)
	s.EXPECT().AddDMLEvent(gomock.Any()).DoAndReturn(func(event *commonEvent.DMLEvent) {
		event.PostFlush()
	}).AnyTimes()

	cfg := config.GetDefaultReplicaConfig()
	router, err := eventrouter.NewEventRouter(cfg.Sink, "topic", false, false)
	require.NoError(t, err)

	engine := &replayEngine{
		partitions: []*partitionState{
			{
				partition: 0,
				decoder:   &singleRowDecoder{event: newEmptyTestDMLEvent(1, 100)},
				groups:    make(map[int64]*dmlGroup),
			},
			{
				partition: 1,
				decoder:   &singleRowDecoder{event: newEmptyTestDMLEvent(2, 101)},
				groups:    make(map[int64]*dmlGroup),
			},
		},
		eventRouter:     router,
		protocol:        config.ProtocolCanalJSON,
		maxMessageBytes: math.MaxInt,
		maxBatchSize:    math.MaxInt,
		mysqlSink:       s,
		offsets:         newOffsetTracker(),
		inflight:        newInflightTracker(),
	}

	topic := "topic"
	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	for partition := int32(0); partition < 2; partition++ {
		wg.Add(1)
		go func(partition int32) {
			defer wg.Done()
			_, err := engine.HandleMessage(context.Background(), &kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &topic,
					Partition: partition,
					Offset:    kafka.Offset(partition + 1),
				},
			})
			errCh <- err
		}(partition)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}
}

type singleRowDecoder struct {
	event *commonEvent.DMLEvent
	ready bool
}

func (d *singleRowDecoder) AddKeyValue(_, _ []byte) {
	d.ready = true
}

func (d *singleRowDecoder) HasNext() (codeccommon.MessageType, bool) {
	return codeccommon.MessageTypeRow, d.ready
}

func (d *singleRowDecoder) NextResolvedEvent() uint64 {
	return 0
}

func (d *singleRowDecoder) NextDMLEvent() *commonEvent.DMLEvent {
	if !d.ready {
		return nil
	}
	d.ready = false
	return d.event
}

func (d *singleRowDecoder) NextDDLEvent() *commonEvent.DDLEvent {
	return nil
}

func newTestDMLEvent(tableID int64, commitTs uint64) *commonEvent.DMLEvent {
	return &commonEvent.DMLEvent{
		PhysicalTableID: tableID,
		CommitTs:        commitTs,
		RowTypes:        []commonType.RowType{commonType.RowTypeUpdate},
		Rows:            chunk.NewChunkWithCapacity(nil, 0),
		TableInfo: &commonType.TableInfo{
			TableName: commonType.TableName{Schema: "test", Table: "t"},
		},
	}
}

func newEmptyTestDMLEvent(tableID int64, commitTs uint64) *commonEvent.DMLEvent {
	event := newTestDMLEvent(tableID, commitTs)
	event.RowTypes = nil
	return event
}

func TestProcessReadyDDLsExecutesIndependentCreateTableWithoutWatermark(t *testing.T) {
	engine, ddls := newTestReplayEngine(t)
	source := engine.offsets.NewSource("topic", 0, kafka.Offset(10))
	source.AddWork()
	source.Close()

	engine.ddlQueue = []queuedDDL{
		{
			source: source,
			ddl: &commonEvent.DDLEvent{
				Query:      "CREATE TABLE `test`.`t` (`id` INT PRIMARY KEY)",
				SchemaName: "test",
				TableName:  "t",
				Type:       byte(timodel.ActionCreateTable),
				FinishedTs: 100,
				BlockedTables: &commonEvent.InfluencedTables{
					InfluenceType: commonEvent.InfluenceTypeNormal,
					TableIDs:      []int64{commonType.DDLSpanTableID},
				},
			},
		},
	}

	require.NoError(t, engine.processReadyDDLs(context.Background()))
	require.Equal(t, []string{"CREATE TABLE `test`.`t` (`id` INT PRIMARY KEY)"}, *ddls)
	committable := engine.offsets.DrainCommittable()
	require.Len(t, committable, 1)
	require.Equal(t, kafka.Offset(11), committable[0].Offset)
}

func TestProcessReadyDDLsPreservesBlockedDDLGating(t *testing.T) {
	engine, ddls := newTestReplayEngine(t)
	source := engine.offsets.NewSource("topic", 0, kafka.Offset(10))
	source.AddWork()
	source.Close()

	engine.ddlQueue = []queuedDDL{
		{
			source: source,
			ddl: &commonEvent.DDLEvent{
				Query:      "ALTER TABLE `test`.`t` ADD COLUMN `c` INT",
				SchemaName: "test",
				TableName:  "t",
				Type:       byte(timodel.ActionAddColumn),
				FinishedTs: 100,
				BlockedTables: &commonEvent.InfluencedTables{
					InfluenceType: commonEvent.InfluenceTypeNormal,
					TableIDs:      []int64{1},
				},
			},
		},
	}

	require.NoError(t, engine.processReadyDDLs(context.Background()))
	require.Empty(t, *ddls)
	require.Len(t, engine.ddlQueue, 1)
	require.Empty(t, engine.offsets.DrainCommittable())

	engine.partitions[0].watermark = 200
	require.NoError(t, engine.processReadyDDLs(context.Background()))
	require.Equal(t, []string{"ALTER TABLE `test`.`t` ADD COLUMN `c` INT"}, *ddls)
	require.Empty(t, engine.ddlQueue)
}
