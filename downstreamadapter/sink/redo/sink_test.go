// Copyright 2023 PingCAP, Inc.
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

package redo

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// Use a smaller worker number for test to speed up the test.
var workerNumberForTest = 2

func TestConsistentConfig(t *testing.T) {
	t.Parallel()
	levelCases := []struct {
		level string
		valid bool
	}{
		{"none", true},
		{"eventual", true},
		{"NONE", false},
		{"", false},
	}
	for _, lc := range levelCases {
		require.Equal(t, lc.valid, redo.IsValidConsistentLevel(lc.level))
	}

	levelEnableCases := []struct {
		level      string
		consistent bool
	}{
		{"invalid-level", false},
		{"none", false},
		{"eventual", true},
	}
	for _, lc := range levelEnableCases {
		require.Equal(t, lc.consistent, redo.IsConsistentEnabled(lc.level))
	}

	storageCases := []struct {
		storage string
		valid   bool
	}{
		{"local", true},
		{"nfs", true},
		{"s3", true},
		{"blackhole", true},
		{"Local", false},
		{"", false},
	}
	for _, sc := range storageCases {
		require.Equal(t, sc.valid, redo.IsValidConsistentStorage(sc.storage))
	}

	s3StorageCases := []struct {
		storage   string
		s3Enabled bool
	}{
		{"local", false},
		{"nfs", false},
		{"s3", true},
		{"blackhole", false},
	}
	for _, sc := range s3StorageCases {
		require.Equal(t, sc.s3Enabled, redo.IsExternalStorage(sc.storage))
	}
}

// TestRedoSinkInProcessor tests how redo log manager is used in processor.
func TestRedoSinkInProcessor(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t1 (id int, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)
	createTableSQL = "create table t2 (id int, name varchar(32));"
	job = helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)
	createTableSQL = "create table t3 (id int, name varchar(32));"
	job = helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)
	createTableSQL = "create table t4 (id int, name varchar(32));"
	job = helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testWriteDMLs := func(storage string, useFileBackend bool) {
		ctx, cancel := context.WithCancel(ctx)
		cfg := &config.ConsistentConfig{
			Level:                 util.AddressOf(string(redo.ConsistentLevelEventual)),
			MaxLogSize:            util.AddressOf(redo.DefaultMaxLogSize),
			Storage:               util.AddressOf(storage),
			FlushIntervalInMs:     util.AddressOf(int64(redo.MinFlushIntervalInMs)),
			MetaFlushIntervalInMs: util.AddressOf(int64(redo.MinFlushIntervalInMs)),
			EncodingWorkerNum:     util.AddressOf(workerNumberForTest),
			FlushWorkerNum:        util.AddressOf(workerNumberForTest),
			UseFileBackend:        util.AddressOf(useFileBackend),
		}
		startTs := uint64(100)
		dmlMgr := New(ctx, common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme), startTs, cfg)
		defer dmlMgr.Close(false)

		var eg errgroup.Group
		eg.Go(func() error {
			return dmlMgr.Run(ctx)
		})

		testCases := []struct {
			rows []*commonEvent.DMLEvent
		}{
			{
				rows: []*commonEvent.DMLEvent{
					helper.DML2Event("test", "t1",
						"insert into t1 values (1, 'test1')"),
					helper.DML2Event("test", "t1",
						"insert into t1 values (2, 'test2')"),
					helper.DML2Event("test", "t1",
						"insert into t1 values (3, 'test3')"),
					helper.DML2Event("test", "t1",
						"insert into t1 values (4, 'test4')"),
				},
			},
			{
				rows: []*commonEvent.DMLEvent{
					helper.DML2Event("test", "t2",
						"insert into t2 values (1, 'test1')"),
					helper.DML2Event("test", "t2",
						"insert into t2 values (2, 'test2')"),
				},
			},
			{
				rows: []*commonEvent.DMLEvent{
					helper.DML2Event("test", "t3",
						"insert into t3 values (1, 'test1')"),
				},
			},
			{
				rows: []*commonEvent.DMLEvent{
					helper.DML2Event("test", "t4",
						"insert into t4 values (1, 'test1')"),
					helper.DML2Event("test", "t4",
						"insert into t4 values (2, 'test1')"),
					helper.DML2Event("test", "t4",
						"insert into t4 values (3, 'test1')"),
				},
			},
		}
		for _, tc := range testCases {
			for _, row := range tc.rows {
				dmlMgr.AddDMLEvent(row)
			}
		}

		cancel()
		require.ErrorIs(t, eg.Wait(), context.Canceled)
	}

	testWriteDMLs("blackhole://", true)
	storages := []string{
		fmt.Sprintf("file://%s", t.TempDir()),
		fmt.Sprintf("nfs://%s", t.TempDir()),
	}
	for _, storage := range storages {
		testWriteDMLs(storage, true)
		testWriteDMLs(storage, false)
	}
}

// TestRedoSinkError tests whether internal error in bgUpdateLog could be managed correctly.
func TestRedoSinkError(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	cfg := &config.ConsistentConfig{
		Level:                 util.AddressOf(string(redo.ConsistentLevelEventual)),
		MaxLogSize:            util.AddressOf(redo.DefaultMaxLogSize),
		Storage:               util.AddressOf("blackhole-invalid://"),
		FlushIntervalInMs:     util.AddressOf(int64(redo.MinFlushIntervalInMs)),
		MetaFlushIntervalInMs: util.AddressOf(int64(redo.MinFlushIntervalInMs)),
		EncodingWorkerNum:     util.AddressOf(workerNumberForTest),
		FlushWorkerNum:        util.AddressOf(workerNumberForTest),
	}
	logMgr := New(ctx, common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme), 0, cfg)
	defer logMgr.Close(false)

	var eg errgroup.Group
	eg.Go(func() error {
		return logMgr.Run(ctx)
	})

	testCases := []struct {
		rows []*commonEvent.DMLEvent
	}{
		{
			rows: []*commonEvent.DMLEvent{
				helper.DML2Event("test", "t",
					"insert into t values (1, 'test1')"),
				helper.DML2Event("test", "t",
					"insert into t values (2, 'test2')"),
				helper.DML2Event("test", "t",
					"insert into t values (3, 'test3')"),
			},
		},
	}
	for _, tc := range testCases {
		for _, row := range tc.rows {
			logMgr.AddDMLEvent(row)
		}
	}

	err := eg.Wait()
	require.Regexp(t, ".*invalid black hole writer.*", err)
	require.Regexp(t, ".*WriteLog.*", err)
}

func BenchmarkBlackhole(b *testing.B) {
	runBenchTest(b, "blackhole://", false)
}

func BenchmarkMemoryWriter(b *testing.B) {
	storage := fmt.Sprintf("file://%s", b.TempDir())
	runBenchTest(b, storage, false)
}

func BenchmarkFileWriter(b *testing.B) {
	storage := fmt.Sprintf("file://%s", b.TempDir())
	runBenchTest(b, storage, true)
}

func runBenchTest(b *testing.B, storage string, useFileBackend bool) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := &config.ConsistentConfig{
		Level:                 util.AddressOf(string(redo.ConsistentLevelEventual)),
		MaxLogSize:            util.AddressOf(redo.DefaultMaxLogSize),
		Storage:               util.AddressOf(storage),
		FlushIntervalInMs:     util.AddressOf(int64(redo.MinFlushIntervalInMs)),
		MetaFlushIntervalInMs: util.AddressOf(int64(redo.MinFlushIntervalInMs)),
		EncodingWorkerNum:     util.AddressOf(redo.DefaultEncodingWorkerNum),
		FlushWorkerNum:        util.AddressOf(redo.DefaultFlushWorkerNum),
		UseFileBackend:        util.AddressOf(useFileBackend),
	}
	dmlMgr := New(ctx, common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme), 0, cfg)
	defer dmlMgr.Close(false)

	var eg errgroup.Group
	eg.Go(func() error {
		return dmlMgr.Run(ctx)
	})

	// Init tables
	numOfTables := 200
	tables := make([]common.TableID, 0, numOfTables)
	maxTsMap := common.NewSpanHashMap[*common.Ts]()
	startTs := uint64(100)
	for i := 0; i < numOfTables; i++ {
		tableID := common.TableID(i)
		tables = append(tables, tableID)
		span := common.TableIDToComparableSpan(common.DefaultKeyspaceID, tableID)
		ts := startTs
		maxTsMap.ReplaceOrInsert(span, &ts)
	}

	// write rows
	maxRowCount := 100000
	wg := sync.WaitGroup{}
	b.ResetTimer()
	for _, tableID := range tables {
		wg.Add(1)
		tableInfo := &common.TableInfo{TableName: common.TableName{Schema: "test", Table: fmt.Sprintf("t_%d", tableID)}}
		go func(span heartbeatpb.TableSpan) {
			defer wg.Done()
			maxCommitTs := maxTsMap.GetV(span)
			var rows []*commonEvent.DMLEvent
			for i := 0; i < maxRowCount; i++ {
				if i%100 == 0 {
					// prepare new row change events
					b.StopTimer()
					*maxCommitTs += rand.Uint64() % 10
					rows = []*commonEvent.DMLEvent{
						{CommitTs: *maxCommitTs, PhysicalTableID: span.TableID, TableInfo: tableInfo},
						{CommitTs: *maxCommitTs, PhysicalTableID: span.TableID, TableInfo: tableInfo},
						{CommitTs: *maxCommitTs, PhysicalTableID: span.TableID, TableInfo: tableInfo},
					}

					b.StartTimer()
				}
				for _, row := range rows {
					dmlMgr.AddDMLEvent(row)
				}
			}
		}(common.TableIDToComparableSpan(common.DefaultKeyspaceID, tableID))
	}
	wg.Wait()

	cancel()

	require.ErrorIs(b, eg.Wait(), context.Canceled)
}
