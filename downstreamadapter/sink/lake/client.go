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
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/writelease"
)

const maxResponseBytes = 8 << 20

type queryPage struct {
	ID     string `json:"id"`
	NodeID string `json:"node_id"`
	State  string `json:"state"`
	Error  *struct {
		Code int `json:"code"`
	} `json:"error"`
	Data     json.RawMessage `json:"data"`
	NextURI  string          `json:"next_uri"`
	FinalURI string          `json:"final_uri"`
}

// httpClient intentionally has no POST retry, redirect or driver-level retry.
// Every statement is autocommit; DDL is never placed inside a DML transaction.
type httpClient struct {
	opts      options
	transport *http.Client
	gate      atomic.Pointer[writelease.Gate]
}

func newHTTPClient(o options) *httpClient {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = o.workers + 2
	return &httpClient{opts: o, transport: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}

func (c *httpClient) mutate(ctx context.Context, sql string, args []any) (mutationResult, error) {
	_, state, err := c.query(ctx, sql, args, false)
	return state, err
}

func (c *httpClient) selectRows(ctx context.Context, sql string, args []any) ([][]*string, error) {
	rows, state, err := c.query(ctx, sql, args, true)
	if err != nil {
		return nil, err
	}
	if state.state != committed {
		return nil, errors.ErrLakeExecution.GenWithStackByArgs()
	}
	return rows, nil
}

func (c *httpClient) query(ctx context.Context, sql string, args []any, readData bool) ([][]*string, mutationResult, error) {
	ctx, cancel := context.WithTimeout(ctx, c.opts.queryTimeout)
	defer cancel()
	if err := writelease.WaitForWrite(ctx, c.gate.Load()); err != nil {
		return nil, mutationResult{state: notCommitted}, errors.WrapError(errors.ErrLakeExecution, err)
	}
	id := uuid.NewString()
	payload := struct {
		SQL        string `json:"sql"`
		Params     []any  `json:"params,omitempty"`
		Session    any    `json:"session"`
		Pagination any    `json:"pagination"`
	}{sql, args, map[string]any{"database": "default", "settings": map[string]string{"timezone": "UTC"}}, map[string]int{"wait_time_secs": 1, "max_rows_per_page": 1024, "max_rows_in_buffer": 1024}}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, mutationResult{state: notCommitted}, errors.WrapError(errors.ErrLakeExecution, err)
	}
	method, path := http.MethodPost, "/v1/query"
	node, route := "", ""
	var rows [][]*string
	var total int
	state := mutationResult{state: commitUnknown}
	for {
		page, h, err := c.request(ctx, method, path, body, id, node, route)
		if err != nil {
			return nil, state, err
		}
		// A pre-start SQL rejection has no server query ID. All started
		// queries, especially successful mutations, must echo our identity.
		preStartFailure := method == http.MethodPost && page.State == "Failed" && page.ID == ""
		if page.ID != id && !preStartFailure {
			return nil, mutationResult{state: commitUnknown}, errors.ErrLakeProtocol.GenWithStackByArgs(id, "query identity mismatch")
		}
		if page.NodeID != "" {
			node = page.NodeID
		}
		if v := h.Get("X-DATABEND-ROUTE-HINT"); v != "" {
			route = v
		}
		if page.State == "Failed" {
			code := 0
			if page.Error != nil {
				code = page.Error.Code
			}
			return nil, mutationResult{state: notCommitted}, errors.ErrLakeProtocol.GenWithStackByArgs(id, "server terminal failure code "+jsonInt(code))
		}
		if page.Error != nil || (page.State != "Succeeded" && page.State != "Starting" && page.State != "Running") {
			return nil, mutationResult{state: commitUnknown}, errors.ErrLakeProtocol.GenWithStackByArgs(id, "missing or inconsistent execution state")
		}
		if page.State == "Succeeded" {
			state.state = committed
			if !readData {
				c.finalize(ctx, page.FinalURI, id, node, route)
				return nil, state, nil
			}
		}
		if readData && len(page.Data) > 0 {
			var data [][]*string
			if err := json.Unmarshal(page.Data, &data); err != nil {
				return nil, state, errors.ErrLakeProtocol.GenWithStackByArgs(id, "invalid result data")
			}
			// Administrative reads are intentionally bounded, independently of paging.
			total += len(page.Data)
			if total > maxResponseBytes {
				return nil, state, errors.ErrLakeProtocol.GenWithStackByArgs(id, "result exceeds metadata budget")
			}
			rows = append(rows, data...)
		}
		if page.NextURI == "" || strings.HasSuffix(page.NextURI, "/final") {
			if state.state != committed {
				return nil, state, errors.ErrLakeProtocol.GenWithStackByArgs(id, "no terminal execution evidence")
			}
			c.finalize(ctx, page.FinalURI, id, node, route)
			return rows, state, nil
		}
		method, path, body = http.MethodGet, page.NextURI, nil
		select {
		case <-ctx.Done():
			return nil, state, errors.ErrLakeProtocol.GenWithStackByArgs(id, "query polling interrupted; verify server termination before resuming")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func jsonInt(n int) string { b, _ := json.Marshal(n); return string(b) }

func (c *httpClient) request(ctx context.Context, method, path string, body []byte, id, node, route string) (queryPage, http.Header, error) {
	// Never forward credentials to an absolute or network-path URI supplied by a server.
	u, err := url.Parse(path)
	if err != nil || u.IsAbs() || u.Host != "" || !strings.HasPrefix(path, "/v1/query") {
		return queryPage{}, nil, errors.ErrLakeProtocol.GenWithStackByArgs(id, "invalid query continuation URI")
	}
	req, err := http.NewRequestWithContext(ctx, method, c.opts.endpoint+path, bytes.NewReader(body)) // #nosec G704 -- endpoint is operator configured; continuation above is restricted to the same origin.
	if err != nil {
		return queryPage{}, nil, errors.ErrLakeProtocol.GenWithStackByArgs(id, "invalid request")
	}
	req.SetBasicAuth(c.opts.username, c.opts.password)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-DATABEND-QUERY-ID", id)
	if c.opts.warehouse != "" {
		req.Header.Set("X-DATABEND-WAREHOUSE", c.opts.warehouse)
	}
	if node != "" {
		req.Header.Set("X-DATABEND-STICKY-NODE", node)
		req.Header.Set("X-DATABEND-NODE-ID", node)
	}
	if route != "" {
		req.Header.Set("X-DATABEND-ROUTE-HINT", route)
	}
	if method == http.MethodPost {
		if err := writelease.WaitForWrite(ctx, c.gate.Load()); err != nil {
			return queryPage{}, nil, errors.ErrLakeProtocol.GenWithStackByArgs(id, "write admission closed before request")
		}
	}
	resp, err := c.transport.Do(req) // #nosec G704 -- intentional configured sink endpoint, same origin only and redirects disabled.
	if err != nil {
		return queryPage{}, nil, errors.ErrLakeProtocol.GenWithStackByArgs(id, "transport failed; verify server termination before resuming")
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return queryPage{}, nil, errors.ErrLakeProtocol.GenWithStackByArgs(id, "HTTP status "+jsonInt(resp.StatusCode))
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil || len(data) > maxResponseBytes {
		return queryPage{}, nil, errors.ErrLakeProtocol.GenWithStackByArgs(id, "truncated or oversized response")
	}
	var page queryPage
	if err := json.Unmarshal(data, &page); err != nil {
		return queryPage{}, nil, errors.ErrLakeProtocol.GenWithStackByArgs(id, "invalid execution response")
	}
	return page, resp.Header, nil
}

func (c *httpClient) finalize(ctx context.Context, path, id, node, route string) {
	if path == "" {
		return
	}
	// Releasing result buffers is observation/cleanup only, never a new mutation.
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	_, _, _ = c.request(ctx, http.MethodGet, path, nil, id, node, route)
}
