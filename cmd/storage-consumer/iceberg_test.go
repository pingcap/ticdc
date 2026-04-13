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
	"time"

	"github.com/pingcap/ticdc/cmd/util"
	downstreamsink "github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	sinkiceberg "github.com/pingcap/ticdc/pkg/sink/iceberg"
	"github.com/stretchr/testify/require"
)

type blockingFlushSink struct {
	added     chan uint64
	firstDone chan struct{}
}

func (s *blockingFlushSink) SinkType() common.SinkType { return common.MysqlSinkType }
func (s *blockingFlushSink) IsNormal() bool            { return true }
func (s *blockingFlushSink) FlushDMLBeforeBlock(commonEvent.BlockEvent) error {
	return nil
}
func (s *blockingFlushSink) WriteBlockEvent(commonEvent.BlockEvent) error { return nil }
func (s *blockingFlushSink) AddCheckpointTs(uint64)                       {}
func (s *blockingFlushSink) SetTableSchemaStore(*commonEvent.TableSchemaStore) {
}
func (s *blockingFlushSink) Close(bool)                {}
func (s *blockingFlushSink) Run(context.Context) error { return nil }
func (s *blockingFlushSink) BatchCount() int           { return 0 }
func (s *blockingFlushSink) BatchBytes() int           { return 0 }

func (s *blockingFlushSink) AddDMLEvent(event *commonEvent.DMLEvent) {
	s.added <- event.CommitTs
	if event.CommitTs == 1 {
		go func() {
			<-s.firstDone
			event.PostFlush()
		}()
		return
	}
	event.PostFlush()
}

var _ downstreamsink.Sink = (*blockingFlushSink)(nil)

func TestBuildIcebergDDLEventsForCreateTable(t *testing.T) {
	version := &sinkiceberg.TableVersion{
		SchemaName: "test",
		TableName:  "t1",
		Columns: []sinkiceberg.Column{
			{ID: 1, Name: "id", Type: "long", Required: true},
			{ID: 2, Name: "name", Type: "string", Required: false},
		},
	}

	events, err := buildIcebergDDLEvents(nil, version)
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, "CREATE DATABASE IF NOT EXISTS `test`", events[0].Query)
	require.Equal(t, "CREATE TABLE IF NOT EXISTS `test`.`t1` (`id` BIGINT NOT NULL,`name` TEXT NULL)", events[1].Query)
}

func TestBuildIcebergDDLEventsForSchemaChange(t *testing.T) {
	prev := &sinkiceberg.TableVersion{
		SchemaName: "test",
		TableName:  "t1",
		Columns: []sinkiceberg.Column{
			{ID: 1, Name: "id", Type: "int", Required: true},
			{ID: 2, Name: "name", Type: "string", Required: false},
		},
	}
	curr := &sinkiceberg.TableVersion{
		SchemaName: "test",
		TableName:  "t1",
		Columns: []sinkiceberg.Column{
			{ID: 1, Name: "id", Type: "long", Required: true},
			{ID: 3, Name: "email", Type: "string", Required: false},
		},
	}

	events, err := buildIcebergDDLEvents(prev, curr)
	require.NoError(t, err)
	require.Len(t, events, 3)
	require.Equal(t, "ALTER TABLE `test`.`t1` DROP COLUMN `name`", events[0].Query)
	require.Equal(t, "ALTER TABLE `test`.`t1` ADD COLUMN `email` TEXT NULL", events[1].Query)
	require.Equal(t, "ALTER TABLE `test`.`t1` MODIFY COLUMN `id` BIGINT NOT NULL", events[2].Query)
}

func TestBuildIcebergDMLEventTreatsUpdateAsInsert(t *testing.T) {
	version := &sinkiceberg.TableVersion{
		SchemaName: "test",
		TableName:  "t1",
		Columns: []sinkiceberg.Column{
			{ID: 1, Name: "id", Type: "long", Required: true},
			{ID: 2, Name: "name", Type: "string", Required: false},
		},
	}

	id := "1"
	name := "alice"
	event, err := buildIcebergDMLEvent(version, 42, sinkiceberg.ChangeRow{
		Op:         "U",
		CommitTs:   "101",
		CommitTime: "2026-01-01T00:00:00Z",
		Columns: map[string]*string{
			"id":   &id,
			"name": &name,
		},
	})
	require.NoError(t, err)

	row, ok := event.GetNextRow()
	require.True(t, ok)
	require.Equal(t, 42, int(event.PhysicalTableID))
	require.Equal(t, uint64(101), event.CommitTs)
	require.Equal(t, uint64(102), event.ReplicatingTs)
	require.Equal(t, event.TableInfo.GetTableName(), "t1")
	require.True(t, row.PreRow.IsEmpty())
	require.False(t, row.Row.IsEmpty())
}

func TestFlushIcebergDMLEventsSerializesByFlushCompletion(t *testing.T) {
	tableID := int64(42)
	group := util.NewEventsGroup(0, tableID)
	group.Append(&commonEvent.DMLEvent{CommitTs: 1, PhysicalTableID: tableID}, false)
	group.Append(&commonEvent.DMLEvent{CommitTs: 2, PhysicalTableID: tableID}, false)

	sink := &blockingFlushSink{
		added:     make(chan uint64, 2),
		firstDone: make(chan struct{}),
	}
	c := &consumer{
		sink: sink,
		eventsGroup: map[int64]*util.EventsGroup{
			tableID: group,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.flushIcebergDMLEvents(ctx, tableID)
	}()

	require.Equal(t, uint64(1), <-sink.added)
	select {
	case commitTs := <-sink.added:
		t.Fatalf("second event %d was sent before first flush completed", commitTs)
	case <-time.After(100 * time.Millisecond):
	}

	close(sink.firstDone)

	require.Equal(t, uint64(2), <-sink.added)
	require.NoError(t, <-errCh)
}
