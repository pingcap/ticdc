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
	stderrors "errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/redo/writer"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/utils/chann"
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
		dmlMgr := New(ctx, common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName), cfg)
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
	logMgr := New(ctx, common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName), cfg)
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
	dmlMgr := New(ctx, common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName), cfg)
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

func TestRedoSinkSendMessagesInBatch(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockWriter := writer.NewMockRedoLogWriter(ctrl)
	expectWriteBatch := func(batchSize int) *gomock.Call {
		args := make([]interface{}, 0, batchSize+1)
		args = append(args, gomock.Any()) // context
		for i := 0; i < batchSize; i++ {
			args = append(args, gomock.Any())
		}
		return mockWriter.EXPECT().
			WriteEvents(args[0], args[1:]...).
			DoAndReturn(func(_ context.Context, events ...writer.RedoEvent) error {
				require.Len(t, events, batchSize)
				return nil
			})
	}

	gomock.InOrder(
		expectWriteBatch(redo.DefaultFlushBatchSize),
		expectWriteBatch(redo.DefaultFlushBatchSize),
		expectWriteBatch(17),
	)

	s := &Sink{
		dmlWriter: mockWriter,
		tasks:     chann.NewUnlimitedChannelDefault[*sinkTask](),
		flush:     newFlushTracker(),
	}

	doneCh := make(chan error, 1)
	go func() {
		doneCh <- s.sendMessages(ctx)
	}()

	totalEvents := redo.DefaultFlushBatchSize*2 + 17
	events := make([]*sinkTask, 0, totalEvents)
	for i := 0; i < totalEvents; i++ {
		events = append(events, newRowTask(common.NewDispatcherID(), uint64(i+1), &commonEvent.RedoRowEvent{}))
	}
	s.tasks.Push(events...)
	s.tasks.Close()

	err := <-doneCh
	require.NoError(t, err)
}

func TestRedoSinkFlushDMLBeforeBlockWaitsForSameDispatcherFlush(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job("create table t_flush_before_block (id int primary key, v int)")
	require.NotNil(t, job)

	dispatcherID := common.NewDispatcherID()
	dmlEvent := helper.DML2Event("test", "t_flush_before_block", "insert into t_flush_before_block values (1, 1)")
	dmlEvent.DispatcherID = dispatcherID

	flushedCh := make(chan struct{}, 1)
	dmlEvent.AddPostFlushFunc(func() {
		flushedCh <- struct{}{}
	})

	writeStarted := make(chan struct{})
	allowFlush := make(chan struct{})

	mockWriter := writer.NewMockRedoLogWriter(ctrl)
	mockWriter.EXPECT().
		WriteEvents(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, events ...writer.RedoEvent) error {
			require.Len(t, events, 1)
			select {
			case <-writeStarted:
			default:
				close(writeStarted)
			}
			<-allowFlush
			for _, event := range events {
				event.PostFlush()
			}
			return nil
		})

	s := &Sink{
		ctx:       ctx,
		dmlWriter: mockWriter,
		tasks:     chann.NewUnlimitedChannelDefault[*sinkTask](),
		flush:     newFlushTracker(),
	}

	sendDone := make(chan error, 1)
	go func() {
		sendDone <- s.sendMessages(ctx)
	}()

	s.AddDMLEvent(dmlEvent)
	<-writeStarted

	ddlEvent := &commonEvent.DDLEvent{DispatcherID: dispatcherID}
	flushDone := make(chan error, 1)
	go func() {
		flushDone <- s.FlushDMLBeforeBlock(ddlEvent)
	}()

	select {
	case err := <-flushDone:
		t.Fatalf("FlushDMLBeforeBlock returned before DML flush completed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(allowFlush)
	require.NoError(t, <-flushDone)

	select {
	case <-flushedCh:
	case <-time.After(time.Second):
		t.Fatal("dml event was not flushed")
	}

	s.tasks.Close()
	require.NoError(t, <-sendDone)
}

func TestRedoSinkFlushDMLBeforeBlockDoesNotWaitForOtherDispatcher(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job("create table t_flush_other_dispatcher (id int primary key, v int)")
	require.NotNil(t, job)

	firstDispatcherID := common.NewDispatcherID()
	secondDispatcherID := common.NewDispatcherID()

	firstEvent := helper.DML2Event("test", "t_flush_other_dispatcher", "insert into t_flush_other_dispatcher values (1, 1)")
	firstEvent.DispatcherID = firstDispatcherID
	firstEvent.CommitTs = 101

	secondEvent := helper.DML2Event("test", "t_flush_other_dispatcher", "insert into t_flush_other_dispatcher values (2, 2)")
	secondEvent.DispatcherID = secondDispatcherID
	secondEvent.CommitTs = 202

	secondFlushedCh := make(chan struct{}, 1)
	secondEvent.AddPostFlushFunc(func() {
		secondFlushedCh <- struct{}{}
	})

	allowSecondFlush := make(chan struct{})

	mockWriter := writer.NewMockRedoLogWriter(ctrl)
	mockWriter.EXPECT().
		WriteEvents(gomock.Any(), gomock.Any()).
		MinTimes(1).
		DoAndReturn(func(_ context.Context, events ...writer.RedoEvent) error {
			for _, event := range events {
				rowEvent, ok := event.(*commonEvent.RedoRowEvent)
				require.True(t, ok)

				switch rowEvent.CommitTs {
				case 101:
					event.PostFlush()
				case 202:
					go func(event writer.RedoEvent) {
						<-allowSecondFlush
						event.PostFlush()
					}(event)
				default:
					t.Fatalf("unexpected redo row commit ts %d", rowEvent.CommitTs)
				}
			}
			return nil
		})

	s := &Sink{
		ctx:       ctx,
		dmlWriter: mockWriter,
		tasks:     chann.NewUnlimitedChannelDefault[*sinkTask](),
		flush:     newFlushTracker(),
	}

	sendDone := make(chan error, 1)
	go func() {
		sendDone <- s.sendMessages(ctx)
	}()

	s.AddDMLEvent(firstEvent)
	s.AddDMLEvent(secondEvent)

	flushDone := make(chan error, 1)
	go func() {
		flushDone <- s.FlushDMLBeforeBlock(&commonEvent.DDLEvent{DispatcherID: firstDispatcherID})
	}()

	select {
	case err := <-flushDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("FlushDMLBeforeBlock waited for unrelated dispatcher")
	}

	select {
	case <-secondFlushedCh:
		t.Fatal("FlushDMLBeforeBlock forced unrelated dispatcher to flush")
	case <-time.After(100 * time.Millisecond):
	}

	close(allowSecondFlush)

	select {
	case <-secondFlushedCh:
	case <-time.After(time.Second):
		t.Fatal("unrelated dispatcher was not flushed")
	}

	s.tasks.Close()
	require.NoError(t, <-sendDone)
}

func TestRedoSinkPostEnqueueBeforeFlush(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job("create table t_post_enqueue_before_flush (id int primary key, v int)")
	require.NotNil(t, job)

	dispatcherID := common.NewDispatcherID()
	dmlEvent := helper.DML2Event("test", "t_post_enqueue_before_flush", "insert into t_post_enqueue_before_flush values (1, 1)")
	dmlEvent.DispatcherID = dispatcherID

	enqueuedCh := make(chan struct{}, 1)
	flushedCh := make(chan struct{}, 1)
	dmlEvent.AddPostEnqueueFunc(func() {
		enqueuedCh <- struct{}{}
	})
	dmlEvent.AddPostFlushFunc(func() {
		flushedCh <- struct{}{}
	})

	writeStarted := make(chan struct{})
	allowFlush := make(chan struct{})

	mockWriter := writer.NewMockRedoLogWriter(ctrl)
	mockWriter.EXPECT().
		WriteEvents(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, events ...writer.RedoEvent) error {
			require.Len(t, events, 1)
			select {
			case <-writeStarted:
			default:
				close(writeStarted)
			}
			<-allowFlush
			for _, event := range events {
				event.PostFlush()
			}
			return nil
		})

	s := &Sink{
		ctx:       ctx,
		dmlWriter: mockWriter,
		tasks:     chann.NewUnlimitedChannelDefault[*sinkTask](),
		flush:     newFlushTracker(),
	}

	sendDone := make(chan error, 1)
	go func() {
		sendDone <- s.sendMessages(ctx)
	}()

	s.AddDMLEvent(dmlEvent)

	select {
	case <-enqueuedCh:
	case <-time.After(time.Second):
		t.Fatal("dml event was not marked enqueued before flush")
	}

	<-writeStarted

	select {
	case <-flushedCh:
		t.Fatal("dml event was flushed before redo writer completed")
	case <-time.After(100 * time.Millisecond):
	}

	close(allowFlush)

	select {
	case <-flushedCh:
	case <-time.After(time.Second):
		t.Fatal("dml event was not flushed after redo writer completed")
	}

	s.tasks.Close()
	require.NoError(t, <-sendDone)
}

func TestRedoSinkFlushDMLBeforeBlockWaitsOnlyForEarlierEnqueuedDML(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job("create table t_flush_barrier_scope (id int primary key, v int)")
	require.NotNil(t, job)

	dispatcherID := common.NewDispatcherID()
	firstEvent := helper.DML2Event("test", "t_flush_barrier_scope", "insert into t_flush_barrier_scope values (1, 1)")
	firstEvent.DispatcherID = dispatcherID
	secondEvent := helper.DML2Event("test", "t_flush_barrier_scope", "insert into t_flush_barrier_scope values (2, 2)")
	secondEvent.DispatcherID = dispatcherID

	secondFlushedCh := make(chan struct{}, 1)
	secondEvent.AddPostFlushFunc(func() {
		secondFlushedCh <- struct{}{}
	})

	firstWriteStarted := make(chan struct{})
	secondWriteStarted := make(chan struct{})
	allowFirstFlush := make(chan struct{})
	allowSecondFlush := make(chan struct{})

	mockWriter := writer.NewMockRedoLogWriter(ctrl)
	gomock.InOrder(
		mockWriter.EXPECT().
			WriteEvents(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, events ...writer.RedoEvent) error {
				require.Len(t, events, 1)
				select {
				case <-firstWriteStarted:
				default:
					close(firstWriteStarted)
				}
				<-allowFirstFlush
				for _, event := range events {
					event.PostFlush()
				}
				return nil
			}),
		mockWriter.EXPECT().
			WriteEvents(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, events ...writer.RedoEvent) error {
				require.Len(t, events, 1)
				select {
				case <-secondWriteStarted:
				default:
					close(secondWriteStarted)
				}
				<-allowSecondFlush
				for _, event := range events {
					event.PostFlush()
				}
				return nil
			}),
	)

	s := &Sink{
		ctx:       ctx,
		dmlWriter: mockWriter,
		tasks:     chann.NewUnlimitedChannelDefault[*sinkTask](),
		flush:     newFlushTracker(),
	}

	sendDone := make(chan error, 1)
	go func() {
		sendDone <- s.sendMessages(ctx)
	}()

	s.AddDMLEvent(firstEvent)
	<-firstWriteStarted

	flushDone := make(chan error, 1)
	go func() {
		flushDone <- s.FlushDMLBeforeBlock(&commonEvent.DDLEvent{DispatcherID: dispatcherID})
	}()

	require.Eventually(t, func() bool {
		return s.tasks.Len() == 1
	}, time.Second, 10*time.Millisecond)

	s.AddDMLEvent(secondEvent)

	select {
	case err := <-flushDone:
		t.Fatalf("FlushDMLBeforeBlock returned before earlier DML flushed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(allowFirstFlush)

	select {
	case err := <-flushDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("FlushDMLBeforeBlock waited for DML enqueued after the barrier")
	}

	select {
	case <-secondFlushedCh:
		t.Fatal("later dml was flushed before the second writer call completed")
	case <-time.After(100 * time.Millisecond):
	}

	<-secondWriteStarted
	close(allowSecondFlush)
	s.tasks.Close()
	require.NoError(t, <-sendDone)
}

func TestRedoSinkFlushDMLBeforeBlockWaitsForActualRowFlush(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job("create table t_flush_actual_row_flush (id int primary key, v int)")
	require.NotNil(t, job)

	dispatcherID := common.NewDispatcherID()
	dmlEvent := helper.DML2Event("test", "t_flush_actual_row_flush", "insert into t_flush_actual_row_flush values (1, 1)")
	dmlEvent.DispatcherID = dispatcherID

	rowWrittenCh := make(chan []writer.RedoEvent, 1)
	mockWriter := writer.NewMockRedoLogWriter(ctrl)
	mockWriter.EXPECT().
		WriteEvents(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, events ...writer.RedoEvent) error {
			require.Len(t, events, 1)
			rowWrittenCh <- events
			return nil
		})

	s := &Sink{
		ctx:       ctx,
		dmlWriter: mockWriter,
		tasks:     chann.NewUnlimitedChannelDefault[*sinkTask](),
		flush:     newFlushTracker(),
	}

	sendDone := make(chan error, 1)
	go func() {
		sendDone <- s.sendMessages(ctx)
	}()

	s.AddDMLEvent(dmlEvent)
	writtenEvents := <-rowWrittenCh

	flushDone := make(chan error, 1)
	go func() {
		flushDone <- s.FlushDMLBeforeBlock(&commonEvent.DDLEvent{DispatcherID: dispatcherID})
	}()

	select {
	case err := <-flushDone:
		t.Fatalf("FlushDMLBeforeBlock returned before row event PostFlush: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	for _, event := range writtenEvents {
		event.PostFlush()
	}

	select {
	case err := <-flushDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("FlushDMLBeforeBlock did not wait for actual row flush")
	}

	s.tasks.Close()
	require.NoError(t, <-sendDone)
}

func TestRedoSinkFlushDMLBeforeBlockReturnsWriterError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job("create table t_flush_writer_error (id int primary key, v int)")
	require.NotNil(t, job)

	dispatcherID := common.NewDispatcherID()
	dmlEvent := helper.DML2Event("test", "t_flush_writer_error", "insert into t_flush_writer_error values (1, 1)")
	dmlEvent.DispatcherID = dispatcherID

	writerErr := stderrors.New("redo writer failed")
	mockWriter := writer.NewMockRedoLogWriter(ctrl)
	mockWriter.EXPECT().
		WriteEvents(gomock.Any(), gomock.Any()).
		Return(writerErr)

	s := &Sink{
		ctx:       ctx,
		dmlWriter: mockWriter,
		tasks:     chann.NewUnlimitedChannelDefault[*sinkTask](),
		flush:     newFlushTracker(),
	}

	sendDone := make(chan error, 1)
	go func() {
		sendDone <- s.sendMessages(ctx)
	}()

	s.AddDMLEvent(dmlEvent)
	require.ErrorIs(t, <-sendDone, writerErr)

	flushDone := make(chan error, 1)
	go func() {
		flushDone <- s.FlushDMLBeforeBlock(&commonEvent.DDLEvent{DispatcherID: dispatcherID})
	}()

	select {
	case err := <-flushDone:
		require.ErrorIs(t, err, writerErr)
	case <-time.After(time.Second):
		t.Fatal("FlushDMLBeforeBlock did not return after writer failure")
	}

	s.tasks.Close()
}
