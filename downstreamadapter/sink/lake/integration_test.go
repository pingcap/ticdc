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
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

// Explicit opt-in only. This test creates and drops its own unique database;
// never use a production warehouse or the running bend-cdc target for it.
func TestLakeNativeLiveIntegration(t *testing.T) {
	raw := os.Getenv("LAKE_NATIVE_TEST_URI")
	if raw == "" {
		t.Skip("LAKE_NATIVE_TEST_URI not configured; no live Lake evidence")
	}
	uri, err := url.Parse(raw)
	require.NoError(t, err)
	database := "ticdc_native_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	uri.Path = "/" + database
	uri.RawPath = ""
	q := uri.Query()
	q.Set("exclusive", "true")
	q.Set("workers", "1")
	q.Set("flush-interval", "10ms")
	q.Set("spool-dir", t.TempDir())
	uri.RawQuery = q.Encode()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	require.NoError(t, Verify(ctx, &config.ChangefeedConfig{}, uri))
	s, err := New(ctx, common.NewChangeFeedIDWithName(database, "default"), &config.ChangefeedConfig{}, uri, 0)
	require.NoError(t, err)
	t.Cleanup(func() {
		s.Close()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = s.client.mutate(cleanupCtx, "DROP DATABASE IF EXISTS "+quoteIdentifier(database), nil)
	})
	done := runRuntime(t, s)
	var callbacks atomic.Int64
	apply := func(e *commonEvent.DMLEvent) {
		e.PostTxnFlushed = []func(){func() { callbacks.Add(1) }}
		before := callbacks.Load()
		s.AddDMLEvent(e)
		require.Eventually(t, func() bool { return callbacks.Load() == before+1 }, 30*time.Second, 10*time.Millisecond)
	}
	info := nativeTestInfo()
	apply(nativeTestEvent(info, 100, 2))
	rows, err := s.client.selectRows(ctx, "SELECT COUNT(*)::VARCHAR FROM "+quoteIdentifier(database)+`."t"`, nil)
	require.NoError(t, err)
	require.Equal(t, "2", *rows[0][0])
	deleted := nativeTestEvent(info, 130, 1)
	deleted.RowTypes[0] = common.RowTypeDelete
	apply(deleted)
	// A historical UPSERT must not resurrect a retained tombstone.
	apply(nativeTestEvent(info, 100, 1))
	rows, err = s.client.selectRows(ctx, "SELECT COUNT(*)::VARCHAR FROM "+quoteIdentifier(database)+`."t"`, nil)
	require.NoError(t, err)
	require.Equal(t, "1", *rows[0][0])
	ddl := &commonEvent.DDLEvent{Type: byte(model.ActionTruncateTable), TableInfo: info, FinishedTs: 150}
	require.NoError(t, s.WriteBlockEvent(ddl))
	apply(nativeTestEvent(info, 120, 2)) // Applied epoch rejects old DML without projection.
	apply(nativeTestEvent(info, 160, 1))
	require.NoError(t, s.WriteBlockEvent(ddl)) // Applied replay must not truncate the new row.
	rows, err = s.client.selectRows(ctx, "SELECT COUNT(*)::VARCHAR FROM "+quoteIdentifier(database)+`."t"`, nil)
	require.NoError(t, err)
	require.Equal(t, "1", *rows[0][0])
	s.Close()
	require.ErrorIs(t, <-done, context.Canceled)
}
