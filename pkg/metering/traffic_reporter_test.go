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

package metering_test

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/metering_sdk/common"
	"github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/writer"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metering"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

const testCategory = "custom-category"

func TestReporterRecords(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	dir := t.TempDir()
	cfg := config.NewMeteringConfig().WithLocalFS(dir).WithPrefix("test").WithSharedPoolID("pool1")
	const total = uint64(1<<53 + 1)
	records := []map[string]any{
		{
			"changefeed_gid": "gid1",
			"traffic_bytes":  common.MeteringValue{Value: total, Unit: "Bytes"},
			"extra":          "kept as provided",
		},
		{
			"changefeed_gid": "gid2",
			"traffic_bytes":  common.MeteringValue{Value: 0, Unit: "Bytes"},
		},
	}
	batch := metering.Batch{Category: testCategory, Timestamp: time.Unix(1788912010, 0), Records: records}
	r, err := metering.NewReporter(cfg)
	require.NoError(t, err)
	defer r.Close()
	require.NoError(t, r.Report(context.Background(), batch))
	// Same-minute reports and a restarted reporter must use separate paths.
	require.NoError(t, r.Report(context.Background(), batch))
	other, err := metering.NewReporter(cfg)
	require.NoError(t, err)
	require.NoError(t, other.Report(context.Background(), batch))
	other.Close()

	paths, err := filepath.Glob(filepath.Join(dir, "test/metering/ru/1788912000", testCategory, "pool1/*.json.gz"))
	require.NoError(t, err)
	require.Len(t, paths, 3)
	for _, path := range paths {
		f, err := os.Open(path)
		require.NoError(t, err)
		gz, err := gzip.NewReader(f)
		require.NoError(t, err)
		var payload struct {
			Timestamp    int64  `json:"timestamp"`
			Category     string `json:"category"`
			SelfID       string `json:"self_id"`
			SharedPoolID string `json:"shared_pool_id"`
			Part         int    `json:"part"`
			Data         []struct {
				GID     string               `json:"changefeed_gid"`
				Traffic common.MeteringValue `json:"traffic_bytes"`
				Extra   string               `json:"extra"`
			} `json:"data"`
		}
		require.NoError(t, json.NewDecoder(gz).Decode(&payload))
		require.NoError(t, gz.Close())
		require.NoError(t, f.Close())
		require.Equal(t, int64(1788912000), payload.Timestamp)
		require.Equal(t, testCategory, payload.Category)
		require.NotEmpty(t, payload.SelfID)
		require.NotContains(t, payload.SelfID, "-")
		require.Equal(t, "pool1", payload.SharedPoolID)
		require.Zero(t, payload.Part)
		require.Len(t, payload.Data, 2)
		require.Equal(t, "gid1", payload.Data[0].GID)
		require.Equal(t, "kept as provided", payload.Data[0].Extra)
		require.Equal(t, common.MeteringValue{Value: total, Unit: "Bytes"}, payload.Data[0].Traffic)
		require.Equal(t, "gid2", payload.Data[1].GID)
		require.Equal(t, common.MeteringValue{Value: 0, Unit: "Bytes"}, payload.Data[1].Traffic)
	}

	r.Close()
	r.Close()
	require.ErrorIs(t,
		r.Report(context.Background(), batch),
		errors.ErrMeteringReporterClosed)
}

// Exercise the public reporter through an HTTP storage boundary. Returning 400
// prevents the AWS SDK's own retries from masking a missing reporter retry.
func TestReporterUploadFailures(t *testing.T) {
	for _, tc := range []struct {
		name         string
		failures     int
		exists       bool
		cancelUpload bool
	}{
		{name: "transient failure", failures: 1},
		{name: "retry exhausted", failures: 6},
		{name: "existing object", exists: true},
		{name: "cancel in-flight upload", cancelUpload: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			var mu sync.Mutex
			var paths []string
			var payloads [][]byte
			heads := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				switch r.Method {
				case http.MethodHead:
					heads++
					if tc.exists {
						w.WriteHeader(http.StatusOK)
					} else {
						w.WriteHeader(http.StatusNotFound)
					}
				case http.MethodPut:
					payload, err := io.ReadAll(r.Body)
					if err != nil {
						t.Errorf("read upload: %v", err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					paths = append(paths, r.URL.Path)
					payloads = append(payloads, payload)
					if tc.cancelUpload {
						cancel()
						<-r.Context().Done()
						return
					}
					if len(paths) <= tc.failures {
						w.WriteHeader(http.StatusBadRequest)
						_, _ = io.WriteString(w, "<Error><Code>InvalidRequest</Code><Message>injected upload failure</Message></Error>")
						return
					}
					w.WriteHeader(http.StatusOK)
				default:
					t.Errorf("unexpected storage operation: %s", r.Method)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer server.Close()
			cfg := config.NewMeteringConfig().WithS3("us-east-1", "bucket").WithEndpoint(server.URL).
				WithAWSConfig(&config.MeteringAWSConfig{AccessKey: "test", SecretAccessKey: "test", S3ForcePathStyle: true})
			reporter, err := metering.NewReporter(cfg)
			require.NoError(t, err)
			defer reporter.Close()
			err = reporter.Report(ctx, metering.Batch{
				Category:  testCategory,
				Timestamp: time.Now(),
				Records:   []map[string]any{{"changefeed_gid": "gid1", "traffic_bytes": common.MeteringValue{Value: 42, Unit: "Bytes"}}},
			})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case tc.exists:
				require.ErrorIs(t, err, writer.ErrFileExists)
				require.Equal(t, 1, heads)
				require.Empty(t, paths, "existing object must not be overwritten")
			case tc.cancelUpload:
				require.ErrorIs(t, err, context.Canceled)
				require.Len(t, paths, 1, "cancellation must stop further uploads")
			case tc.failures == 6:
				require.ErrorContains(t, err, "injected upload failure")
				require.Len(t, paths, 6, "report must terminate after the configured retry budget")
			default:
				require.NoError(t, err)
				require.Len(t, paths, 2)
			}
			for i := 1; i < len(paths); i++ {
				require.Equal(t, paths[0], paths[i], "retry must retain the batch identity")
				require.Equal(t, payloads[0], payloads[i], "retry must retain the batch content")
			}
		})
	}
}

func TestReporterCancellationAndEmpty(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	dir := t.TempDir()
	r, err := metering.NewReporter(config.NewMeteringConfig().WithLocalFS(dir))
	require.NoError(t, err)
	defer r.Close()
	now := time.Now()
	records := []map[string]any{{"changefeed_gid": "gid1"}}
	require.NoError(t, r.Report(context.Background(), metering.Batch{Category: testCategory, Timestamp: now}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t,
		r.Report(ctx, metering.Batch{Category: testCategory, Timestamp: now, Records: records}),
		context.Canceled)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestReporterRejectsInvalidBatch(t *testing.T) {
	dir := t.TempDir()
	r, err := metering.NewReporter(config.NewMeteringConfig().WithLocalFS(dir))
	require.NoError(t, err)
	defer r.Close()
	now := time.Unix(1788912000, 0)
	records := []map[string]any{{"changefeed_gid": "gid1"}}
	for _, tc := range []struct {
		name  string
		batch metering.Batch
	}{
		{name: "missing category", batch: metering.Batch{Timestamp: now, Records: records}},
		{name: "category traversal", batch: metering.Batch{Category: "../ticdc", Timestamp: now, Records: records}},
		{name: "zero timestamp", batch: metering.Batch{Category: testCategory, Records: records}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorIs(t, r.Report(context.Background(), tc.batch), errors.ErrMeteringInvalidBatch)
		})
	}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestReporterConcurrentCalls(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	dir := t.TempDir()
	r, err := metering.NewReporter(config.NewMeteringConfig().WithLocalFS(dir))
	require.NoError(t, err)
	defer r.Close()
	const calls = 4
	start := make(chan struct{})
	errCh := make(chan error, calls)
	var wg sync.WaitGroup
	for i := 0; i < calls; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errCh <- r.Report(context.Background(), metering.Batch{
				Category:  testCategory,
				Timestamp: time.Unix(1788912000, 0),
				Records:   []map[string]any{{"call": i}},
			})
		}(i)
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
	paths, err := filepath.Glob(filepath.Join(dir, "metering/ru/1788912000", testCategory, "default-shared-pool/*.json.gz"))
	require.NoError(t, err)
	require.Len(t, paths, calls)
}
