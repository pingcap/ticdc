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
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/writelease"
	"github.com/stretchr/testify/require"
)

func TestHTTPClientRequiresTerminalEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		second any
		want   commitState
	}{
		{"result before failure", map[string]any{"state": "Failed", "error": map[string]any{"code": 1001}}, notCommitted},
		{"result before disconnect", nil, commitUnknown},
		{"committed statistics malformed", map[string]any{"state": "Succeeded", "data": "not result rows", "stats": "not statistics"}, committed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var posts, gets atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				id := r.Header.Get("X-DATABEND-QUERY-ID")
				if r.Method == "POST" {
					posts.Add(1)
					_ = json.NewEncoder(w).Encode(map[string]any{"id": id, "node_id": "node", "state": "Running", "data": [][]string{{"1"}}, "next_uri": "/v1/query/next"})
					return
				}
				gets.Add(1)
				if tc.second == nil {
					w.WriteHeader(503)
					return
				}
				response := tc.second.(map[string]any)
				response["id"] = id
				_ = json.NewEncoder(w).Encode(response)
			}))
			defer server.Close()
			c := newHTTPClient(options{endpoint: server.URL, queryTimeout: time.Second, workers: 1, username: "root"})
			defer c.transport.CloseIdleConnections()
			result, err := c.mutate(context.Background(), "MERGE INTO TARGET", nil)
			require.Equal(t, tc.want, result.state)
			if tc.want == committed {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.EqualValues(t, 1, posts.Load())
			require.EqualValues(t, 1, gets.Load())
		})
	}
}

func TestHTTPClientProtectsCredentialsAndDoesNotRetryPost(t *testing.T) {
	var leaked atomic.Int64
	foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Add(1) }))
	defer foreign.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"id": r.Header.Get("X-DATABEND-QUERY-ID"), "state": "Running", "next_uri": foreign.URL + "/v1/query/steal"})
	}))
	defer server.Close()
	c := newHTTPClient(options{endpoint: server.URL, username: "user", password: "SECRET", queryTimeout: time.Second})
	result, err := c.mutate(context.Background(), "INSERT SECRET_PAYLOAD", nil)
	require.Error(t, err)
	require.Equal(t, commitUnknown, result.state)
	require.Zero(t, leaked.Load())
	require.NotContains(t, err.Error(), "SECRET")
	gate := writelease.NewGate()
	c.gate.Store(gate)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err = c.mutate(ctx, "DELETE TARGET", nil)
	require.Error(t, err)
	require.Equal(t, notCommitted, result.state)
}

func TestHTTPClientRejectsWrongOrMissingSuccessfulIdentity(t *testing.T) {
	for _, id := range []string{"", "another-query"} {
		t.Run(id, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{"id": id, "state": "Succeeded", "data": [][]string{{"1"}}})
			}))
			defer server.Close()
			c := newHTTPClient(options{endpoint: server.URL, queryTimeout: time.Second})
			result, err := c.mutate(context.Background(), "INSERT TARGET", nil)
			require.Error(t, err)
			require.Equal(t, commitUnknown, result.state)
		})
	}
}
