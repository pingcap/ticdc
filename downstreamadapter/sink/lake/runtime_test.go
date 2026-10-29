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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lake

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/spool"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

// apiFixture models only the HTTP boundary and journal persistence. It does not
// execute Lake SQL and must not be cited as proof of server atomicity.
type apiFixture struct {
	mu                sync.Mutex
	columns           []columnDef
	epoch             uint64
	applied           bool
	plan              string
	sql               []string
	rawRows           int
	merges            int
	truncates         int
	unknownMerge      bool
	unknownRawAt      int
	targetMissing     bool
	remoteBytes       string
	maxAddTime        string
	failAfterTruncate bool
}

func newAPIFixture(t *testing.T) (*apiFixture, *httptest.Server) {
	s, err := schemaFromNative(nativeTestInfo(), "", "")
	require.NoError(t, err)
	f := &apiFixture{columns: toSchemaDef(s).Columns}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			SQL    string            `json:"sql"`
			Params []json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(400)
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		f.sql = append(f.sql, req.SQL)
		data := [][]string{}
		switch {
		case strings.HasPrefix(req.SQL, "SELECT epoch"):
			if f.epoch > 0 {
				data = [][]string{{strconv.FormatUint(f.epoch, 10), strconv.FormatBool(f.applied), f.plan}}
			}
		case strings.HasPrefix(req.SQL, "SELECT COALESCE(SUM(data_compressed_size)"):
			value := f.remoteBytes
			if value == "" {
				value = "0"
			}
			data = [][]string{{value}}
		case strings.HasPrefix(req.SQL, "SELECT COUNT(*)::VARCHAR FROM system.tables"):
			value := "1"
			if f.targetMissing {
				value = "0"
			}
			data = [][]string{{value}}
		case strings.HasPrefix(req.SQL, "CREATE TABLE") && !strings.Contains(req.SQL, "_cdc_"):
			if f.targetMissing {
				f.targetMissing = false
				var p schemaPlan
				_ = json.Unmarshal([]byte(f.plan), &p)
				if len(p.Schema.Columns) > 0 {
					f.columns = p.Schema.Columns
				}
			}
		case strings.HasPrefix(req.SQL, "DESCRIBE"):
			for _, c := range f.columns {
				nullable := "NO"
				if c.Nullable {
					nullable = "YES"
				}
				data = append(data, []string{c.Name, c.Type, nullable})
			}
		case strings.HasPrefix(req.SQL, "MERGE") && strings.Contains(req.SQL, "_cdc_schema_epoch"):
			var e uint64
			var a bool
			var p string
			_ = json.Unmarshal(req.Params[0], &e)
			_ = json.Unmarshal(req.Params[1], &a)
			_ = json.Unmarshal(req.Params[2], &p)
			if f.failAfterTruncate && a {
				f.failAfterTruncate = false
				w.WriteHeader(503)
				return
			}
			f.epoch, f.applied, f.plan = e, a, p
		case strings.HasPrefix(req.SQL, "INSERT"):
			f.rawRows += len(req.Params) / 6
			if f.unknownRawAt > 0 && f.rawRows >= f.unknownRawAt {
				w.WriteHeader(503)
				return
			}
		case strings.HasPrefix(req.SQL, "MERGE"):
			if f.unknownMerge {
				w.WriteHeader(503)
				return
			}
			f.merges++
		case strings.HasPrefix(req.SQL, "TRUNCATE"):
			f.truncates++
			f.rawRows = 0
		case strings.HasPrefix(req.SQL, "DROP TABLE") && strings.Contains(req.SQL, "_cdc_raw"):
			f.rawRows = 0
		case strings.HasPrefix(req.SQL, "ALTER TABLE"):
			var plan schemaPlan
			_ = json.Unmarshal([]byte(f.plan), &plan)
			f.columns = plan.Schema.Columns
		case strings.HasPrefix(req.SQL, "SELECT COALESCE(MAX(add_time)"):
			data = [][]string{{f.maxAddTime}}
		case strings.HasPrefix(req.SQL, "SELECT ?"):
			data = [][]string{{"1"}}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": r.Header.Get("X-DATABEND-QUERY-ID"), "state": "Succeeded", "data": data})
	}))
	t.Cleanup(server.Close)
	return f, server
}

func runtimeSink(t *testing.T, server *httptest.Server, extra string) *Sink {
	t.Helper()
	uri, err := url.Parse(strings.Replace(server.URL, "http://", "lake+http://", 1) + "?exclusive=true&workers=1&flush-interval=1h&query-timeout=1s&spool-dir=" + url.QueryEscape(t.TempDir()) + extra)
	require.NoError(t, err)
	s, err := New(context.Background(), common.NewChangeFeedIDWithName("lake-test", "default"), &config.ChangefeedConfig{}, uri, 0)
	require.NoError(t, err)
	t.Cleanup(s.Close)
	return s
}

func runRuntime(t *testing.T, s *Sink) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()
	return done
}

func waitEnqueued(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("event not enqueued")
	}
}

func TestRuntimeBatchApplyAndDDLBarrier(t *testing.T) {
	f, server := newAPIFixture(t)
	s := runtimeSink(t, server, "")
	done := runRuntime(t, s)
	var flushed atomic.Int64
	for _, ts := range []uint64{100, 120} {
		e := nativeTestEvent(nativeTestInfo(), ts, 2)
		enqueued := make(chan struct{})
		e.PostTxnEnqueued = []func(){func() { close(enqueued) }}
		e.PostTxnFlushed = []func(){func() { flushed.Add(1) }}
		s.AddDMLEvent(e)
		waitEnqueued(t, enqueued)
	}
	require.Zero(t, flushed.Load(), "raw acceptance is not target application")
	e := &commonEvent.DDLEvent{Type: byte(model.ActionAddIndex), TableInfo: nativeTestInfo(), FinishedTs: 150}
	require.NoError(t, s.FlushDMLBeforeBlock(e))
	require.EqualValues(t, 2, flushed.Load())
	f.mu.Lock()
	require.Equal(t, 4, f.rawRows)
	require.Equal(t, 1, f.merges)
	f.mu.Unlock()
	require.NoError(t, s.WriteBlockEvent(e))
	f.mu.Lock()
	require.EqualValues(t, 150, f.epoch)
	require.True(t, f.applied)
	for _, sql := range f.sql {
		require.NotContains(t, sql, "STREAM")
	}
	f.mu.Unlock()
	s.Close()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestRuntimeUnknownMergeFailsLifecycleWithoutFlush(t *testing.T) {
	f, server := newAPIFixture(t)
	f.unknownMerge = true
	s := runtimeSink(t, server, "&batch-rows=1")
	done := runRuntime(t, s)
	var flushed atomic.Int64
	e := nativeTestEvent(nativeTestInfo(), 100, 1)
	e.PostTxnFlushed = []func(){func() { flushed.Add(1) }}
	s.AddDMLEvent(e)
	select {
	case err := <-done:
		require.Error(t, err)
		require.Equal(t, errors.ErrChangefeedUnretryable.RFCCode(), errors.ErrorCode(err))
		require.Contains(t, err.Error(), "request")
	case <-time.After(5 * time.Second):
		t.Fatal("unknown mutation did not terminate Run")
	}
	require.Zero(t, flushed.Load())
	require.False(t, s.IsNormal())
	f.mu.Lock()
	n := len(f.sql)
	f.mu.Unlock()
	s.AddDMLEvent(nativeTestEvent(nativeTestInfo(), 120, 1))
	f.mu.Lock()
	require.Len(t, f.sql, n)
	f.mu.Unlock()
}

func TestRuntimeLargeEventStreamsAndReclaimsBeforeSeal(t *testing.T) {
	f, server := newAPIFixture(t)
	s := runtimeSink(t, server, "&batch-bytes=1024&spool-bytes=4096&memory-ratio=0.01")
	done := runRuntime(t, s)
	var flushed atomic.Int64
	e := nativeTestEvent(nativeTestInfo(), 100, 150)
	enqueued := make(chan struct{})
	e.PostTxnEnqueued = []func(){func() { close(enqueued) }}
	e.PostTxnFlushed = []func(){func() { flushed.Add(1) }}
	s.AddDMLEvent(e)
	waitEnqueued(t, enqueued)
	require.NoError(t, s.FlushDMLBeforeBlock(&commonEvent.DDLEvent{}))
	require.EqualValues(t, 1, flushed.Load())
	f.mu.Lock()
	require.Equal(t, 150, f.rawRows)
	require.Greater(t, f.merges, 1)
	f.mu.Unlock()
	s.Close()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestRuntimeFatalAdmissionAndCloseNeverFlush(t *testing.T) {
	_, server := newAPIFixture(t)
	t.Run("oversized event", func(t *testing.T) {
		s := runtimeSink(t, server, "&source-bytes=1048576")
		e := nativeTestEvent(nativeTestInfo(), 100, 100000)
		var flushed atomic.Int64
		e.PostTxnFlushed = []func(){func() { flushed.Add(1) }}
		s.AddDMLEvent(e)
		require.False(t, s.IsNormal())
		err := s.Run(context.Background())
		require.Error(t, err)
		require.Equal(t, errors.ErrChangefeedUnretryable.RFCCode(), errors.ErrorCode(err))
		require.Zero(t, flushed.Load())
	})
	t.Run("close pending", func(t *testing.T) {
		s := runtimeSink(t, server, "")
		done := runRuntime(t, s)
		e := nativeTestEvent(nativeTestInfo(), 100, 1)
		var flushed atomic.Int64
		enqueued := make(chan struct{})
		e.PostTxnFlushed = []func(){func() { flushed.Add(1) }}
		e.PostTxnEnqueued = []func(){func() { close(enqueued) }}
		s.AddDMLEvent(e)
		waitEnqueued(t, enqueued)
		s.Close()
		require.ErrorIs(t, <-done, context.Canceled)
		require.Zero(t, flushed.Load())
	})
}

func TestSpoolPressureDrainsOtherTablesWithoutSuccessOnDiscard(t *testing.T) {
	_, server := newAPIFixture(t)
	s := runtimeSink(t, server, "&batch-bytes=1024&spool-bytes=4096&memory-ratio=0.01")
	lane := s.lanes[0]
	var flush atomic.Int64
	tracker := newEventCompletion(func() { flush.Add(1) })
	for i := range 40 {
		schema := testSchema()
		schema.table = "table" + strconv.Itoa(i)
		writer, err := newTableWriter(schema, func(context.Context, string, []any) (mutationResult, error) {
			return mutationResult{state: committed}, nil
		}, 10, 1, 0)
		require.NoError(t, err)
		row := testRow(t, insertAction, 100, i)
		data, err := json.Marshal([]spoolRow{{row.id, row.payload, row.commitTs, row.action}})
		require.NoError(t, err)
		fragment, ok := tracker.register()
		require.True(t, ok)
		require.NoError(t, lane.enqueue(context.Background(), &tableState{schema: schema, writer: writer}, data, 1, fragment, nil))
	}
	require.Zero(t, flush.Load(), "already applied OPEN fragments do not acknowledge the whole event")
	tracker.seal()
	lane.cleanup()
	require.Zero(t, flush.Load(), "fatal cleanup cannot acknowledge retained fragments")
	require.EqualValues(t, 0, spool.AccountingBytes(nil))
}

func TestConcurrentAdmissionAndCloseReleaseAllSources(t *testing.T) {
	_, server := newAPIFixture(t)
	s := runtimeSink(t, server, "")
	done := runRuntime(t, s)
	info := nativeTestInfo()
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() { s.AddDMLEvent(nativeTestEvent(info, 100, 1)) })
	}
	s.Close()
	wg.Wait()
	require.ErrorIs(t, <-done, context.Canceled)
	s.mu.Lock()
	defer s.mu.Unlock()
	require.Zero(t, s.sourceBytes)
	require.Zero(t, s.sourceEvents)
}

func TestInternalTimeoutRemainsFatalNotLifecycleCancel(t *testing.T) {
	_, server := newAPIFixture(t)
	s := runtimeSink(t, server, "")
	err := errors.WrapError(errors.ErrLakeExecution, context.DeadlineExceeded)
	s.stop(err)
	require.True(t, errors.Is(context.Cause(s.ctx), context.DeadlineExceeded))
	err = s.Run(context.Background())
	require.Error(t, err)
	require.Equal(t, errors.ErrChangefeedUnretryable.RFCCode(), errors.ErrorCode(err))
}

func TestRuntimePartialRawCommitDoesNotConfirmOrRetry(t *testing.T) {
	f, server := newAPIFixture(t)
	f.unknownRawAt = 2
	s := runtimeSink(t, server, "&batch-rows=1")
	done := runRuntime(t, s)
	var flush atomic.Int64
	e := nativeTestEvent(nativeTestInfo(), 100, 3)
	e.PostTxnFlushed = []func(){func() { flush.Add(1) }}
	s.AddDMLEvent(e)
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("partial raw failure did not stop")
	}
	require.Zero(t, flush.Load())
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Equal(t, 2, f.rawRows, "unknown raw SQL must not be resent or continue to the third fragment")
	require.Equal(t, 1, f.merges, "only the first fragment had a confirmed MERGE")
}

func TestRuntimeUsesRetainedTimestampAfterRestart(t *testing.T) {
	f, server := newAPIFixture(t)
	f.maxAddTime = "2099-01-02 03:04:05.999999"
	s := runtimeSink(t, server, "")
	info := nativeTestInfo()
	schema, err := schemaFromNative(info, "", "")
	require.NoError(t, err)
	state, skip, err := s.lanes[0].schemas.prepareDML(context.Background(), schema, 100)
	require.NoError(t, err)
	require.False(t, skip)
	require.Equal(t, "2099-01-02 03:04:05.999999", state.writer.clock.last.Format("2006-01-02 15:04:05.000000"))
	_, args := schema.insertRawSQL([]normalizedRow{testRow(t, deleteAction, 100, 1)}, &state.writer.clock, time.Now())
	require.Equal(t, "2099-01-02 03:04:06.000000", args[3], "recovery must not allocate a lower same-TSO tie breaker")
}

func TestRuntimeLowTrafficAndTransportEpochDoesNotDiscardAdmittedData(t *testing.T) {
	_, server := newAPIFixture(t)
	s := runtimeSink(t, server, "")
	s.opts.flushInterval = 10 * time.Millisecond
	done := runRuntime(t, s)
	var flushed atomic.Int64
	for _, epoch := range []uint64{1, 2} {
		e := nativeTestEvent(nativeTestInfo(), 100+epoch, 1)
		e.Epoch = epoch
		e.PostTxnFlushed = []func(){func() { flushed.Add(1) }}
		s.AddDMLEvent(e)
	}
	require.Eventually(t, func() bool { return flushed.Load() == 2 }, 5*time.Second, time.Millisecond)
	s.Close()
	require.ErrorIs(t, <-done, context.Canceled)
	require.EqualValues(t, 2, flushed.Load())
}

func TestRemoteRawMonitoringThresholdStopsBeforeNextInsert(t *testing.T) {
	f, server := newAPIFixture(t)
	f.remoteBytes = "100"
	s := runtimeSink(t, server, "&remote-bytes-limit=100&batch-rows=1")
	done := runRuntime(t, s)
	var flushed atomic.Int64
	e := nativeTestEvent(nativeTestInfo(), 100, 1)
	e.PostTxnFlushed = []func(){func() { flushed.Add(1) }}
	s.AddDMLEvent(e)
	select {
	case err := <-done:
		require.ErrorContains(t, err, "monitoring threshold")
	case <-time.After(5 * time.Second):
		t.Fatal("remote threshold did not stop admission")
	}
	require.Zero(t, flushed.Load())
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Zero(t, f.rawRows)
}

func TestRuntimeConfiguredConflictKeyReachesDMLAndJournal(t *testing.T) {
	f, server := newAPIFixture(t)
	s := runtimeSink(t, server, "&conflict-key=VaLuE")
	done := runRuntime(t, s)
	info := nativeTestInfo()
	ddl := &commonEvent.DDLEvent{Type: byte(model.ActionCreateTable), TableInfo: info, FinishedTs: 90}
	require.NoError(t, s.WriteBlockEvent(ddl))
	var flushed atomic.Int64
	e := nativeTestEvent(info, 100, 1)
	e.PostTxnFlushed = []func(){func() { flushed.Add(1) }}
	s.AddDMLEvent(e)
	require.NoError(t, s.FlushDMLBeforeBlock(nil))
	require.EqualValues(t, 1, flushed.Load())
	f.mu.Lock()
	var plan schemaPlan
	require.NoError(t, json.Unmarshal([]byte(f.plan), &plan))
	require.Equal(t, []string{"value"}, plan.Schema.Keys)
	require.Equal(t, 1, f.rawRows)
	f.mu.Unlock()
	s.Close()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestRuntimeSkippedDDLDrainsAndContinues(t *testing.T) {
	for _, action := range []model.ActionType{model.ActionRenameTable, model.ActionRenameTables, model.ActionDropSchema} {
		t.Run(action.String(), func(t *testing.T) {
			f, server := newAPIFixture(t)
			s := runtimeSink(t, server, "")
			done := runRuntime(t, s)
			var dml, ddl atomic.Int64
			e := nativeTestEvent(nativeTestInfo(), 100, 1)
			e.PostTxnFlushed = []func(){func() { dml.Add(1) }}
			s.AddDMLEvent(e)
			// No TableInfo is intentional: DB/multi-table events need no new
			// metadata/progress protocol just to be explicitly skipped.
			ignored := &commonEvent.DDLEvent{Type: byte(action), SchemaName: "lake", TableName: "t", FinishedTs: 150, PostTxnFlushed: []func(){func() { ddl.Add(1) }}}
			require.NoError(t, s.WriteBlockEvent(ignored))
			require.EqualValues(t, 1, dml.Load(), "prior DML must drain before skipped DDL callback")
			require.EqualValues(t, 1, ddl.Load())
			f.mu.Lock()
			require.Zero(t, f.epoch, "skipping must not invent a target schema epoch")
			require.Equal(t, 1, f.merges)
			before := len(f.sql)
			f.mu.Unlock()
			require.NoError(t, s.WriteBlockEvent(&commonEvent.DDLEvent{Type: byte(action), FinishedTs: 150}))
			f.mu.Lock()
			require.Len(t, f.sql, before, "no SQL must be executed for the skipped event itself")
			f.mu.Unlock()
			e = nativeTestEvent(nativeTestInfo(), 160, 1)
			e.PostTxnFlushed = []func(){func() { dml.Add(1) }}
			s.AddDMLEvent(e)
			require.NoError(t, s.FlushDMLBeforeBlock(nil))
			require.EqualValues(t, 2, dml.Load())
			require.True(t, s.IsNormal())
			s.Close()
			require.ErrorIs(t, <-done, context.Canceled)
		})
	}
}

func TestSkippedDDLDoesNotBypassUnknownDML(t *testing.T) {
	f, server := newAPIFixture(t)
	f.unknownMerge = true
	s := runtimeSink(t, server, "")
	done := runRuntime(t, s)
	s.AddDMLEvent(nativeTestEvent(nativeTestInfo(), 100, 1))
	var flushed atomic.Int64
	ddl := &commonEvent.DDLEvent{Type: byte(model.ActionDropSchema), FinishedTs: 150, PostTxnFlushed: []func(){func() { flushed.Add(1) }}}
	require.Error(t, s.WriteBlockEvent(ddl))
	require.Zero(t, flushed.Load())
	require.Error(t, <-done)
	s.Close()
}

func TestRuntimeSharedAdmissionBackpressure(t *testing.T) {
	for _, budget := range []string{"events", "bytes", "lane queue"} {
		t.Run(budget, func(t *testing.T) {
			_, server := newAPIFixture(t)
			s := runtimeSink(t, server, "&source-events=2&batch-bytes=1024&spool-bytes=4096")
			var flushed atomic.Int64
			first := nativeTestEvent(nativeTestInfo(), 100, 1)
			second := nativeTestEvent(nativeTestInfo(), 120, 1)
			first.PostTxnFlushed = []func(){func() { flushed.Add(1) }}
			second.PostTxnFlushed = []func(){func() { flushed.Add(1) }}
			wantFlushed := int64(2)
			charge := sourceEventBytes(first)
			require.Equal(t, charge, sourceEventBytes(second))
			switch budget {
			case "events":
				s.opts.sourceEvents = 1
				s.AddDMLEvent(first)
			case "bytes":
				s.opts.sourceBytes = charge + charge/2
				s.AddDMLEvent(first)
			case "lane queue":
				// Existing barrier items are not part of the DML source pool.
				for range cap(s.lanes[0].input) {
					s.lanes[0].input <- workItem{done: make(chan error, 1)}
				}
				wantFlushed = 1
			}
			admitted := make(chan struct{})
			go func() {
				s.AddDMLEvent(second)
				close(admitted)
			}()
			select {
			case <-admitted:
				t.Fatal("full shared admission must wait, not fail or bypass quota")
			case <-time.After(50 * time.Millisecond):
			}
			require.True(t, s.IsNormal(), "ordinary pressure must not stop the feed")
			require.Zero(t, flushed.Load())
			s.mu.Lock()
			bytes, events := s.sourceBytes, s.sourceEvents
			s.mu.Unlock()
			require.LessOrEqual(t, bytes, s.opts.sourceBytes)
			require.LessOrEqual(t, events, s.opts.sourceEvents)
			done := runRuntime(t, s)
			waitEnqueued(t, admitted)
			require.NoError(t, s.FlushDMLBeforeBlock(nil))
			require.Equal(t, wantFlushed, flushed.Load())
			s.Close()
			require.ErrorIs(t, <-done, context.Canceled)
		})
	}
}

func TestRuntimeAdmissionWaitStopsWithoutFlush(t *testing.T) {
	for _, fatal := range []bool{false, true} {
		t.Run(strconv.FormatBool(fatal), func(t *testing.T) {
			_, server := newAPIFixture(t)
			s := runtimeSink(t, server, "&source-events=1")
			var flushed atomic.Int64
			for i := range 2 {
				e := nativeTestEvent(nativeTestInfo(), uint64(100+i), 1)
				e.PostTxnFlushed = []func(){func() { flushed.Add(1) }}
				if i == 0 {
					s.AddDMLEvent(e)
					continue
				}
				returned := make(chan struct{})
				go func() { s.AddDMLEvent(e); close(returned) }()
				select {
				case <-returned:
					t.Fatal("source pool must apply backpressure")
				case <-time.After(50 * time.Millisecond):
				}
				if fatal {
					s.stop(errors.ErrLakeExecution.GenWithStackByArgs())
				}
				s.Close()
				waitEnqueued(t, returned)
			}
			require.Zero(t, flushed.Load())
			s.mu.Lock()
			require.Zero(t, s.sourceBytes)
			require.Zero(t, s.sourceEvents)
			s.mu.Unlock()
		})
	}
}
