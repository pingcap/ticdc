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

package metering_test

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/reader/metering"
	"github.com/pingcap/metering_sdk/storage"
	"github.com/pingcap/metering_sdk/writer"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metering"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestReporterRecords(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		cfg := config.NewMeteringConfig().WithLocalFS(dir).WithPrefix("test").WithSharedPoolID("1234567890123456789")
		const total = uint64(1<<53 + 1)
		records := []metering.Record{
			{
				KeyspaceName:   "12345678901234567890",
				ChangefeedName: "changefeed-1",
				EffectiveBytes: total,
			},
			{
				KeyspaceName:   "12345678901234567891",
				ChangefeedName: "changefeed-2",
				EffectiveBytes: 42,
			},
		}
		r, err := metering.NewReporter(cfg, "cluster-a")
		require.NoError(t, err)
		defer r.Close()

		ts := time.Now().Truncate(time.Minute).Unix()
		require.NoError(t, r.Report(t.Context(), ts, records))

		checkRecords := func(timestamp int64, expected []metering.Record) {
			t.Helper()
			base := filepath.Join(dir, "test/metering/ru", strconv.FormatInt(timestamp, 10), "ticdc/1234567890123456789")
			paths, globErr := filepath.Glob(filepath.Join(base, "*.json.gz"))
			require.NoError(t, globErr)
			require.Len(t, paths, 1)

			f, openErr := os.Open(filepath.Join(base, "cluster_a-0.json.gz"))
			require.NoError(t, openErr)
			gz, gzErr := gzip.NewReader(f)
			require.NoError(t, gzErr)
			var payload struct {
				Timestamp    int64  `json:"timestamp"`
				Category     string `json:"category"`
				SelfID       string `json:"self_id"`
				SharedPoolID string `json:"shared_pool_id"`
				Part         int    `json:"part"`
				Data         []struct {
					Version        string `json:"version"`
					ClusterID      string `json:"cluster_id"`
					Changefeed     string `json:"changefeed"`
					EffectiveBytes struct {
						Value uint64 `json:"value"`
						Unit  string `json:"unit"`
					} `json:"effective_bytes"`
				} `json:"data"`
			}
			decoder := json.NewDecoder(gz)
			decoder.DisallowUnknownFields()
			require.NoError(t, decoder.Decode(&payload))
			require.NoError(t, gz.Close())
			require.NoError(t, f.Close())
			require.Equal(t, timestamp, payload.Timestamp)
			require.Equal(t, "ticdc", payload.Category)
			require.Equal(t, "cluster_a", payload.SelfID)
			require.Equal(t, "1234567890123456789", payload.SharedPoolID)
			require.Zero(t, payload.Part)
			require.Len(t, payload.Data, len(expected))
			for i, rec := range expected {
				require.Equal(t, metering.RecordVersion1, payload.Data[i].Version)
				require.Equal(t, rec.KeyspaceName, payload.Data[i].ClusterID)
				require.Equal(t, rec.ChangefeedName, payload.Data[i].Changefeed)
				require.Equal(t, rec.EffectiveBytes, payload.Data[i].EffectiveBytes.Value)
				require.Equal(t, metering.BytesUnit, payload.Data[i].EffectiveBytes.Unit)
			}
		}
		checkRecords(ts, records)

		provider, err := storage.NewObjectStorageProvider(cfg.ToProviderConfig())
		require.NoError(t, err)
		reader := meteringreader.NewMeteringReader(provider, config.DefaultConfig())
		defer reader.Close()
		path := fmt.Sprintf("metering/ru/%d/ticdc/1234567890123456789/cluster_a-0.json.gz", ts)
		info, err := reader.GetFileInfo(path)
		require.NoError(t, err)
		require.Equal(t, "cluster_a", info.SelfID)
		data, err := reader.ReadFile(t.Context(), path)
		require.NoError(t, err)
		require.Len(t, data.Data, 2)
		require.Equal(t, "12345678901234567890", data.Data[0]["cluster_id"])

		// Same-minute overwrite must fail immediately with ErrFileExists and preserve original data.
		updated := []metering.Record{{
			KeyspaceName:   "12345678901234567890",
			ChangefeedName: "changefeed-1",
			EffectiveBytes: 99,
		}}
		err = r.Report(t.Context(), ts, updated)
		require.ErrorIs(t, err, writer.ErrFileExists)
		checkRecords(ts, records)

		// Advancing to the next whole-minute timestamp succeeds and retains the previous object.
		nextTS := ts + 60
		require.NoError(t, r.Report(t.Context(), nextTS, updated))
		checkRecords(ts, records)
		checkRecords(nextTS, updated)

		r.Close()
		r.Close()
		require.ErrorIs(t, r.Report(t.Context(), nextTS+60, records), errors.ErrMeteringReporterClosed)
	})
}

func TestReporterUploadFailuresAndExists(t *testing.T) {
	for _, tc := range []struct {
		name          string
		failures      int
		headFailures  int
		fileExists    bool
		putPersisted  bool
		persistOnPutN int
		cancelUpload  bool
	}{
		{name: "transient failure", failures: 1},
		{name: "last attempt succeeds", failures: 2},
		{name: "retry exhausted", failures: 3},
		{name: "last attempt persisted before transient error", failures: 3, persistOnPutN: 3},
		{name: "file already exists", fileExists: true},
		{name: "head failure before pre-existing file", headFailures: 1, fileExists: true},
		{name: "put persisted before transient error", failures: 1, putPersisted: true},
		{name: "cancel in-flight upload", cancelUpload: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			var mu sync.Mutex
			var paths []string
			var payloads [][]byte
			heads := 0
			persistedOnPut := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				switch r.Method {
				case http.MethodHead:
					heads++
					if heads <= tc.headFailures {
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
					if tc.fileExists || persistedOnPut {
						w.WriteHeader(http.StatusOK)
						return
					}
					w.WriteHeader(http.StatusNotFound)
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
					if tc.putPersisted || (tc.persistOnPutN > 0 && len(paths) >= tc.persistOnPutN) {
						persistedOnPut = true
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
			reporter, err := metering.NewReporter(cfg, "default")
			require.NoError(t, err)
			defer reporter.Close()
			ts := time.Now().Truncate(time.Minute).Unix()
			err = reporter.Report(ctx, ts, []metering.Record{
				{KeyspaceName: "tenant-a", ChangefeedName: "payments", EffectiveBytes: 42},
			})
			mu.Lock()
			gotHeads := heads
			gotPaths := append([]string(nil), paths...)
			gotPayloads := append([][]byte(nil), payloads...)
			mu.Unlock()
			require.Greater(t, gotHeads, 0, "OverwriteExisting = false must check object existence first")
			switch {
			case tc.fileExists:
				require.ErrorIs(t, err, writer.ErrFileExists)
				require.Equal(t, tc.headFailures+1, gotHeads, "ErrFileExists must not be retried")
				require.Empty(t, gotPaths)
				require.False(t, reporter.HasUncertainUpload())
			case tc.putPersisted:
				require.NoError(t, err)
				require.Equal(t, 2, gotHeads)
				require.Len(t, gotPaths, 1, "already-persisted batch must commit without duplicate PUT")
				require.False(t, reporter.HasUncertainUpload())
			case tc.cancelUpload:
				require.ErrorIs(t, err, context.Canceled)
				require.Len(t, gotPaths, 1, "cancellation must stop further uploads")
				require.True(t, reporter.HasUncertainUpload())
			case tc.failures == 3:
				require.ErrorContains(t, err, "injected upload failure")
				require.Len(t, gotPaths, 3, "report must terminate after the configured retry budget")
				require.True(t, reporter.HasUncertainUpload())
				persisted, probeErr := reporter.ResolveUncertainUpload(t.Context())
				require.NoError(t, probeErr)
				require.Equal(t, tc.persistOnPutN > 0, persisted)
				require.False(t, reporter.HasUncertainUpload())
			default:
				require.NoError(t, err)
				require.Len(t, gotPaths, tc.failures+1)
				require.False(t, reporter.HasUncertainUpload())
			}
			for i := 1; i < len(gotPaths); i++ {
				require.Equal(t, gotPaths[0], gotPaths[i], "retry must retain the batch identity")
				require.Equal(t, gotPayloads[0], gotPayloads[i], "retry must retain the batch content")
			}
		})
	}
}

func TestReporterCancellationAndValidation(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	dir := t.TempDir()
	_, err := metering.NewReporter(config.NewMeteringConfig().WithLocalFS(dir), "   ")
	require.ErrorIs(t, err, errors.ErrInvalidServerOption)

	r, err := metering.NewReporter(config.NewMeteringConfig().WithLocalFS(dir), "default")
	require.NoError(t, err)
	defer r.Close()

	ts := time.Now().Truncate(time.Minute).Unix()
	records := []metering.Record{{KeyspaceName: "tenant-a", ChangefeedName: "payments", EffectiveBytes: 10}}
	require.NoError(t, r.Report(t.Context(), ts, nil))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, r.Report(ctx, ts, records), context.Canceled)

	require.ErrorIs(t, r.Report(t.Context(), ts, []metering.Record{{ChangefeedName: "payments"}}), errors.ErrMeteringInvalidBatch)
	require.ErrorIs(t, r.Report(t.Context(), ts, []metering.Record{{KeyspaceName: "tenant-a"}}), errors.ErrMeteringInvalidBatch)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestReporterConcurrentCalls(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	dir := t.TempDir()
	r, err := metering.NewReporter(config.NewMeteringConfig().WithLocalFS(dir), "prod-1")
	require.NoError(t, err)
	defer r.Close()
	const calls = 4
	baseTS := time.Now().Truncate(time.Minute).Unix()
	start := make(chan struct{})
	errCh := make(chan error, calls)
	var wg sync.WaitGroup
	for i := range calls {
		wg.Go(func() {
			<-start
			errCh <- r.Report(t.Context(), baseTS+int64(i)*60, []metering.Record{
				{KeyspaceName: fmt.Sprintf("tenant-%d", i), ChangefeedName: "payments", EffectiveBytes: uint64(i + 1)},
			})
		})
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
	paths, err := filepath.Glob(filepath.Join(dir, "metering/ru/*/ticdc/default-shared-pool/prod_1-0.json.gz"))
	require.NoError(t, err)
	require.Len(t, paths, calls)
}
