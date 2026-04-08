//go:build intest
// +build intest

// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"path"
	"sync"
	"testing"

	"github.com/pingcap/ticdc/cmd/util"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

type recordingSink struct {
	mu  sync.Mutex
	ops []string
}

func (s *recordingSink) SinkType() common.SinkType { return common.BlackHoleSinkType }
func (s *recordingSink) IsNormal() bool            { return true }

func (s *recordingSink) AddDMLEvent(event *commonEvent.DMLEvent) {
	s.mu.Lock()
	s.ops = append(s.ops, fmt.Sprintf("dml:%s.%s", event.TableInfo.GetSchemaName(), event.TableInfo.GetTableName()))
	s.mu.Unlock()
	event.PostFlush()
}

func (s *recordingSink) FlushDMLBeforeBlock(event commonEvent.BlockEvent) error { return nil }

func (s *recordingSink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	ddl, ok := event.(*commonEvent.DDLEvent)
	if !ok {
		return fmt.Errorf("unexpected block event type %T", event)
	}
	s.mu.Lock()
	s.ops = append(s.ops, "ddl:"+ddl.Query)
	s.mu.Unlock()
	event.PostFlush()
	return nil
}

func (s *recordingSink) AddCheckpointTs(ts uint64) {}

func (s *recordingSink) SetTableSchemaStore(tableSchemaStore *commonEvent.TableSchemaStore) {}

func (s *recordingSink) Close(removeChangefeed bool) {}

func (s *recordingSink) Run(ctx context.Context) error { return nil }

func (s *recordingSink) Operations() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.ops...)
}

func newTestConsumer(t *testing.T) (*consumer, *recordingSink) {
	t.Helper()

	ctx := context.Background()
	dir := t.TempDir()
	uri := fmt.Sprintf("file:///%s", dir)
	externalStorage, err := putil.GetExternalStorageWithDefaultTimeout(ctx, uri)
	require.NoError(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.DateSeparator = putil.AddressOf(config.DateSeparatorDay.String())
	codecCfg := codeccommon.NewConfig(config.ProtocolCsv)
	codecCfg.Delimiter = ","
	codecCfg.Quote = "\""
	codecCfg.NullString = "\\N"
	codecCfg.Terminator = "\n"
	codecCfg.IncludeCommitTs = true

	recording := &recordingSink{}

	oldWidth := fileIndexWidth
	fileIndexWidth = config.DefaultFileIndexWidth

	t.Cleanup(func() {
		fileIndexWidth = oldWidth
		externalStorage.Close()
	})

	return &consumer{
		replicationCfg:    replicaConfig,
		codecCfg:          codecCfg,
		externalStorage:   externalStorage,
		fileExtension:     ".csv",
		sink:              recording,
		tableDMLIdxMap:    make(map[cloudstorage.DmlPathKey]fileIndexKeyMap),
		handledDMLIdxMap:  make(map[cloudstorage.DmlPathKey]fileIndexKeyMap),
		eventsGroup:       make(map[int64]*util.EventsGroup),
		tableDDLWatermark: make(map[string]uint64),
		tableDefMap:       make(map[string]map[uint64]*cloudstorage.TableDefinition),
		tableIDGenerator:  &fakeTableIDGenerator{tableIDs: make(map[string]int64)},
		errCh:             make(chan error, 1),
	}, recording
}

func writeMetadataFile(t *testing.T, c *consumer, checkpointTs uint64) {
	t.Helper()
	err := c.externalStorage.WriteFile(context.Background(), "metadata", []byte(fmt.Sprintf(`{"checkpoint-ts":%d}`, checkpointTs)))
	require.NoError(t, err)
}

func writeSchemaFile(t *testing.T, c *consumer, tableDef cloudstorage.TableDefinition) {
	t.Helper()
	path, err := tableDef.GenerateSchemaFilePath()
	require.NoError(t, err)
	data, err := tableDef.MarshalWithQuery()
	require.NoError(t, err)
	err = c.externalStorage.WriteFile(context.Background(), path, data)
	require.NoError(t, err)
}

func writeDMLFile(
	t *testing.T,
	c *consumer,
	pathKey cloudstorage.DmlPathKey,
	date string,
	content string,
) {
	t.Helper()

	fileIndex := &cloudstorage.FileIndex{Idx: 1}
	dataPath := pathKey.GenerateDMLFilePath(fileIndex, c.fileExtension, fileIndexWidth)
	indexPath := path.Join(
		pathKey.Schema,
		pathKey.Table,
		fmt.Sprintf("%d", pathKey.TableVersion),
		date,
		"meta",
		"CDC.index",
	)
	err := c.externalStorage.WriteFile(context.Background(), dataPath, []byte(content))
	require.NoError(t, err)
	err = c.externalStorage.WriteFile(context.Background(), indexPath, []byte("CDC000001.csv\n"))
	require.NoError(t, err)
}

func TestHandleNewFilesWaitsForStableCheckpointBeforeApplyingRenameChain(t *testing.T) {
	ctx := context.Background()
	consumer, recording := newTestConsumer(t)
	schemaName := "cdc_storage_test"

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.DDL2Job("create database " + schemaName)
	helper.DDL2Event(fmt.Sprintf("create table %s.table_8(id int primary key, v varchar(32))", schemaName))

	renameTo108 := helper.DDL2Event(fmt.Sprintf("rename table %s.table_8 to %s.table_108", schemaName, schemaName))
	renameBackTo8 := helper.DDL2Event(fmt.Sprintf("rename table %s.table_108 to %s.table_8", schemaName, schemaName))

	var renameTo108Def cloudstorage.TableDefinition
	renameTo108Def.FromTableInfo(schemaName, "table_108", renameTo108.TableInfo, renameTo108.FinishedTs, false)
	renameTo108Def.Query = renameTo108.Query
	renameTo108Def.Type = renameTo108.Type
	var renameBackTo8Def cloudstorage.TableDefinition
	renameBackTo8Def.FromTableInfo(schemaName, "table_8", renameBackTo8.TableInfo, renameBackTo8.FinishedTs, false)
	renameBackTo8Def.Query = renameBackTo8.Query
	renameBackTo8Def.Type = renameBackTo8.Type

	writeSchemaFile(t, consumer, renameTo108Def)
	writeSchemaFile(t, consumer, renameBackTo8Def)

	round1Checkpoint := renameTo108.FinishedTs - 1
	writeMetadataFile(t, consumer, round1Checkpoint)

	round1Files, err := consumer.getNewFiles(ctx)
	require.NoError(t, err)
	require.Len(t, round1Files, 2)
	require.NoError(t, consumer.handleNewFiles(ctx, round1Files, round1Checkpoint, 1))
	require.Empty(t, recording.Operations())
	require.Empty(t, consumer.handledDMLIdxMap)

	dmlDate := "2026-03-10"
	pathKey := cloudstorage.DmlPathKey{
		SchemaPathKey: cloudstorage.SchemaPathKey{
			Schema:       schemaName,
			Table:        "table_108",
			TableVersion: renameTo108.FinishedTs,
		},
		PartitionNum: 0,
		Date:         dmlDate,
	}
	writeDMLFile(
		t,
		consumer,
		pathKey,
		dmlDate,
		fmt.Sprintf("\"I\",\"%s\",\"%s\",%d,124,\"value\"\n",
			"table_108", schemaName, renameTo108.FinishedTs+1),
	)

	round2Checkpoint := renameBackTo8.FinishedTs
	writeMetadataFile(t, consumer, round2Checkpoint)

	round2Files, err := consumer.getNewFiles(ctx)
	require.NoError(t, err)
	require.Len(t, round2Files, 3)
	require.NoError(t, consumer.handleNewFiles(ctx, round2Files, round2Checkpoint, 2))
	require.Equal(t, []string{
		"ddl:" + renameTo108.Query,
		"dml:" + schemaName + ".table_108",
		"ddl:" + renameBackTo8.Query,
	}, recording.Operations())
	require.Len(t, consumer.handledDMLIdxMap, 3)
}
