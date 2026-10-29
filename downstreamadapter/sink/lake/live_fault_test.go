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
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

// liveResponseCut delegates to the REAL warehouse, then discards a chosen
// Succeeded response at the client transport boundary. It never simulates SQL
// execution or invents terminal success. A before-send cut is used only to prove
// partial raw INSERTs; recovery is permitted only after this test knows all old
// requests have either not been sent or reached a matching real terminal state.
// This is response-loss/process-state replacement injection, not a server crash.
type liveResponseCut struct {
	base     http.RoundTripper
	matches  func(string, []any) bool
	at       int
	before   bool
	mu       sync.Mutex
	posts    int
	cutID    string
	terminal string
	fired    bool
}

func (f *liveResponseCut) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodPost {
		r, err := req.GetBody()
		if err != nil {
			return nil, err
		}
		var payload struct {
			SQL    string `json:"sql"`
			Params []any  `json:"params"`
		}
		err = json.NewDecoder(r).Decode(&payload)
		_ = r.Close()
		if err != nil {
			return nil, err
		}
		f.mu.Lock()
		if f.matches(payload.SQL, payload.Params) {
			f.posts++
			if f.posts == f.at {
				f.cutID = req.Header.Get("X-DATABEND-QUERY-ID")
			}
		}
		cutBefore := f.before && f.cutID == req.Header.Get("X-DATABEND-QUERY-ID")
		if cutBefore {
			f.fired = true
		}
		f.mu.Unlock()
		if cutBefore {
			return nil, io.ErrUnexpectedEOF
		}
	}
	response, err := f.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil {
		return nil, err
	}
	response.Body = io.NopCloser(bytes.NewReader(data))
	var page queryPage
	if json.Unmarshal(data, &page) == nil {
		f.mu.Lock()
		cut := !f.before && !f.fired && page.ID != "" && page.ID == f.cutID && page.State == "Succeeded"
		if cut {
			f.terminal, f.fired = page.ID, true
		}
		f.mu.Unlock()
		if cut {
			_ = response.Body.Close()
			return nil, io.ErrUnexpectedEOF
		}
	}
	return response, nil
}

func liveFaultEnvironment(t *testing.T) (context.Context, options, *httpClient) {
	t.Helper()
	raw := os.Getenv("LAKE_NATIVE_TEST_URI")
	if raw == "" {
		t.Skip("explicit LAKE_NATIVE_TEST_URI is required; no real warehouse evidence")
	}
	u, err := url.Parse(raw)
	require.NoError(t, err)
	u.Path = "/ticdc_fault_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	u.RawPath = ""
	q := u.Query()
	q.Set("exclusive", "true")
	q.Set("workers", "1")
	q.Set("batch-rows", "2")
	q.Set("flush-interval", "10ms")
	q.Set("spool-dir", t.TempDir())
	u.RawQuery = q.Encode()
	o, err := parseOptions(u, &config.ChangefeedConfig{})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)
	client := newHTTPClient(o)
	r, err := client.mutate(ctx, "CREATE DATABASE "+quoteIdentifier(o.database), nil)
	require.NoError(t, err)
	require.Equal(t, committed, r.state)
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		r, err := client.mutate(cleanup, "DROP DATABASE IF EXISTS "+quoteIdentifier(o.database), nil)
		require.NoError(t, err)
		require.Equal(t, committed, r.state)
	})
	t.Log("owned real Lake database", o.database)
	return ctx, o, client
}

func liveRuntime(ctx context.Context, t *testing.T, o options, name string) *Sink {
	t.Helper()
	u, err := url.Parse(os.Getenv("LAKE_NATIVE_TEST_URI"))
	require.NoError(t, err)
	u.Path, u.RawPath = "/"+o.database, ""
	q := u.Query()
	q.Set("exclusive", "true")
	q.Set("workers", "1")
	q.Set("flush-interval", "10ms")
	q.Set("spool-dir", t.TempDir())
	u.RawQuery = q.Encode()
	s, err := New(ctx, common.NewChangeFeedIDWithName(name, "default"), &config.ChangefeedConfig{}, u, 0)
	require.NoError(t, err)
	t.Cleanup(s.Close)
	return s
}

func liveNamedInfo(name string) *common.TableInfo {
	base := nativeTestInfo()
	return common.WrapTableInfo("lake", &model.TableInfo{ID: 1, Name: ast.NewCIStr(name), PKIsHandle: true, Columns: base.GetColumns()})
}

func liveCount(ctx context.Context, t *testing.T, c *httpClient, name string, expected string) {
	t.Helper()
	rows, err := c.selectRows(ctx, "SELECT COUNT(*)::VARCHAR FROM "+name, nil)
	require.NoError(t, err)
	require.Equal(t, expected, *rows[0][0])
}

func TestLakeNativeLiveRawPartialAndMergeResponseLoss(t *testing.T) {
	ctx, o, admin := liveFaultEnvironment(t)
	t.Run("partial_raw_before_second_insert", func(t *testing.T) {
		info := liveNamedInfo("partial_raw")
		schema, err := schemaFromNative(info, o.database, "")
		require.NoError(t, err)
		client := newHTTPClient(o)
		m := newSchemaManager(client, o)
		state, _, err := m.prepareDML(ctx, schema, 100)
		require.NoError(t, err)
		cut := &liveResponseCut{base: client.transport.Transport, at: 2, before: true, matches: func(sql string, _ []any) bool { return strings.HasPrefix(sql, "INSERT INTO "+schema.rawName()) }}
		client.transport.Transport = cut
		var rows []normalizedRow
		for i := range 4 {
			r, err := normalizeNativeRow(schema, insertAction, 100, map[string]any{"id": i, "value": "row"})
			require.NoError(t, err)
			rows = append(rows, r)
		}
		applied, err := state.writer.applyBatch(ctx, rows, time.Now())
		require.Error(t, err)
		require.False(t, applied.applied)
		require.True(t, cut.fired)
		require.Equal(t, 2, cut.posts, "unknown raw must not be retried")
		liveCount(ctx, t, admin, schema.rawName(), "2")
		liveCount(ctx, t, admin, schema.targetName(), "0")
		// Replacement process state; the second request was never sent and the
		// first request had a matching terminal success. Reapply ALL source rows.
		m = newSchemaManager(admin, o)
		state, _, err = m.prepareDML(ctx, schema, 100)
		require.NoError(t, err)
		applied, err = state.writer.applyBatch(ctx, rows, time.Now())
		require.NoError(t, err)
		require.True(t, applied.applied)
		liveCount(ctx, t, admin, schema.targetName(), "4")
		liveCount(ctx, t, admin, schema.rawName(), "4")
	})
	t.Run("real_merge_committed_terminal_response_discarded", func(t *testing.T) {
		info := liveNamedInfo("merge_lost")
		s := liveRuntime(ctx, t, o, "merge_lost")
		schema, err := schemaFromNative(info, o.database, "")
		require.NoError(t, err)
		cut := &liveResponseCut{base: s.client.transport.Transport, at: 1, matches: func(sql string, _ []any) bool {
			return strings.HasPrefix(sql, "MERGE INTO "+schema.targetName()+" a USING")
		}}
		s.client.transport.Transport = cut
		done := runRuntime(t, s)
		var flushed atomic.Int64
		e := nativeTestEvent(info, 100, 2)
		e.PostTxnFlushed = []func(){func() { flushed.Add(1) }}
		s.AddDMLEvent(e)
		select {
		case err := <-done:
			require.Error(t, err)
		case <-time.After(90 * time.Second):
			t.Fatal("unknown real MERGE did not terminate old sink lifecycle")
		}
		s.Close()
		require.Zero(t, flushed.Load(), "commit without client evidence must not flush")
		require.False(t, s.IsNormal())
		require.True(t, cut.fired)
		require.NotEmpty(t, cut.terminal, "test must know matching server terminal BEFORE recovery")
		require.Equal(t, 1, cut.posts)
		t.Log("discarded real MERGE Succeeded response", cut.terminal)
		liveCount(ctx, t, admin, schema.targetName(), "2")
		m := newSchemaManager(admin, o)
		state, _, err := m.prepareDML(ctx, schema, 100)
		require.NoError(t, err)
		var rows []normalizedRow
		for i := range 2 {
			r, err := normalizeNativeRow(schema, insertAction, 100, map[string]any{"id": i, "value": "payload"})
			require.NoError(t, err)
			rows = append(rows, r)
		}
		applied, err := state.writer.applyBatch(ctx, rows, time.Now())
		require.NoError(t, err)
		require.True(t, applied.applied)
		liveCount(ctx, t, admin, schema.targetName(), "2")
		liveCount(ctx, t, admin, schema.rawName(), "2")
	})
}

func TestLakeNativeLiveDDLInterruptionRecovery(t *testing.T) {
	ctx, o, admin := liveFaultEnvironment(t)
	for _, kind := range []string{"create", "alter", "truncate", "drop"} {
		cuts := []string{"pending", "target", "raw", "applied"}
		if kind == "alter" {
			cuts = []string{"pending", "add", "rename", "modify", "raw", "applied"}
		}
		if kind == "truncate" {
			cuts = []string{"pending", "drop_raw", "truncate", "raw", "applied"}
		}
		for _, point := range cuts {
			t.Run(kind+"/"+point, func(t *testing.T) {
				info := liveNamedInfo(kind + "_" + point)
				schema, err := schemaFromNative(info, o.database, "")
				require.NoError(t, err)
				s := liveRuntime(ctx, t, o, schema.table)
				client := s.client
				m := s.lanes[0].schemas
				if kind != "create" {
					state, _, err := m.prepareDML(ctx, schema, 100)
					require.NoError(t, err)
					r, err := normalizeNativeRow(schema, insertAction, 100, map[string]any{"id": 1, "value": "old"})
					require.NoError(t, err)
					_, err = state.writer.applyBatch(ctx, []normalizedRow{r}, time.Now())
					require.NoError(t, err)
				}
				e := &commonEvent.DDLEvent{TableInfo: info, FinishedTs: 150}
				switch kind {
				case "create":
					e.Type = byte(model.ActionCreateTable)
				case "truncate":
					e.Type = byte(model.ActionTruncateTable)
				case "drop":
					e.Type = byte(model.ActionDropTable)
				case "alter":
					e.Type = byte(model.ActionMultiSchemaChange)
					e.Query = "ALTER TABLE t ADD COLUMN extra BIGINT NULL, RENAME COLUMN value TO label, MODIFY COLUMN label TEXT NULL"
					cols := append([]*model.ColumnInfo(nil), info.GetColumns()...)
					copyValue := *cols[1]
					copyValue.Name = ast.NewCIStr("label")
					cols[1] = &copyValue
					cols = append(cols, &model.ColumnInfo{ID: 3, Name: ast.NewCIStr("extra"), FieldType: *types.NewFieldType(mysql.TypeLonglong), State: model.StatePublic})
					e.TableInfo = common.WrapTableInfo("lake", &model.TableInfo{ID: 1, Name: ast.NewCIStr(schema.table), PKIsHandle: true, Columns: cols})
				}
				cut := &liveResponseCut{base: client.transport.Transport, at: 1, matches: func(sql string, args []any) bool {
					switch point {
					case "pending", "applied":
						return strings.HasPrefix(sql, "MERGE INTO "+schema.journalName()) && len(args) == 3 && args[1] == (point == "applied")
					case "target":
						if kind == "drop" {
							return sql == "DROP TABLE IF EXISTS "+schema.targetName()
						}
						return strings.HasPrefix(sql, "CREATE TABLE IF NOT EXISTS "+schema.targetName()+" (")
					case "raw":
						if kind == "drop" {
							return sql == "DROP TABLE IF EXISTS "+schema.rawName()
						}
						return strings.HasPrefix(sql, "CREATE TABLE IF NOT EXISTS "+schema.rawName()+" (")
					case "drop_raw":
						return sql == "DROP TABLE IF EXISTS "+schema.rawName()
					case "truncate":
						return sql == "TRUNCATE TABLE "+schema.targetName()
					default:
						return strings.HasPrefix(sql, "ALTER TABLE "+schema.targetName()+" "+strings.ToUpper(point)+" COLUMN ")
					}
				}}
				client.transport.Transport = cut
				var callbacks atomic.Int64
				e.PostTxnFlushed = []func(){func() { callbacks.Add(1) }}
				done := runRuntime(t, s)
				require.Error(t, s.WriteBlockEvent(e))
				select {
				case err := <-done:
					require.Error(t, err)
				case <-time.After(90 * time.Second):
					t.Fatal("unknown DDL did not terminate old sink lifecycle")
				}
				s.Close()
				require.Zero(t, callbacks.Load(), "unknown DDL must not flush")
				require.True(t, cut.fired)
				require.Equal(t, 1, cut.posts)
				require.NotEmpty(t, cut.terminal)
				t.Log("cut after real committed DDL", point, cut.terminal)
				// The test has independent matching terminal proof for the old
				// request. Replace ALL local schema/DDL state; reuse only Lake data.
				m = newSchemaManager(admin, o)
				require.NoError(t, m.applyDDL(ctx, e))
				post, err := schemaFromNative(e.TableInfo, o.database, "")
				require.NoError(t, err)
				r, err := m.loadEpoch(ctx, post)
				require.NoError(t, err)
				require.EqualValues(t, 150, r.epoch)
				require.True(t, r.applied)
				if kind == "drop" {
					exists, err := m.targetExists(ctx, post)
					require.NoError(t, err)
					require.False(t, exists)
					rows, err := admin.selectRows(ctx, "SELECT COUNT(*)::VARCHAR FROM system.tables WHERE database=? AND name IN (?, ?)", []any{post.database, post.table, post.table + "_cdc_raw"})
					require.NoError(t, err)
					require.Equal(t, "0", *rows[0][0])
					return
				}
				state, skip, err := m.prepareDML(ctx, post, 160)
				require.NoError(t, err)
				require.False(t, skip)
				payload := map[string]any{"id": 2, "value": "new"}
				if kind == "alter" {
					payload = map[string]any{"id": 2, "label": "new", "extra": 7}
				}
				row, err := normalizeNativeRow(post, insertAction, 160, payload)
				require.NoError(t, err)
				_, err = state.writer.applyBatch(ctx, []normalizedRow{row}, time.Now())
				require.NoError(t, err)
				// Replaying an applied DDL must not clear new-generation data.
				require.NoError(t, m.applyDDL(ctx, e))
				expected := "1"
				if kind == "alter" {
					expected = "2"
				}
				liveCount(ctx, t, admin, post.targetName(), expected)
				projection := "id::VARCHAR, value::VARCHAR"
				want := [][]string{{"2", "new"}}
				if kind == "alter" {
					projection = "id::VARCHAR, label::VARCHAR, COALESCE(extra::VARCHAR, 'SQL_NULL')"
					want = [][]string{{"1", "old", "SQL_NULL"}, {"2", "new", "7"}}
				}
				actual, err := admin.selectRows(ctx, "SELECT "+projection+" FROM "+post.targetName()+" ORDER BY id", nil)
				require.NoError(t, err)
				values := make([][]string, len(actual))
				for i, row := range actual {
					for _, cell := range row {
						require.NotNil(t, cell)
						values[i] = append(values[i], *cell)
					}
				}
				require.Equal(t, want, values)
				_, skip, err = m.prepareDML(ctx, post, 120)
				require.NoError(t, err)
				require.True(t, skip, "old DML must not enter post-DDL schema")
			})
		}
	}
}
