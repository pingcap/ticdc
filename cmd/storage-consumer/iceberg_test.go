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

	sinkiceberg "github.com/pingcap/ticdc/pkg/sink/iceberg"
	"github.com/stretchr/testify/require"
)

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
