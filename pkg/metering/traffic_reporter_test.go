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
	meteringreader "github.com/pingcap/metering_sdk/reader/metering"
	"github.com/pingcap/metering_sdk/storage"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metering"
	"github.com/pingcap/ticdc/pkg/metering/traffic"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestReporterRecords(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		cfg := config.NewMeteringConfig().WithLocalFS(dir).WithPrefix("test").WithSharedPoolID("pool1")
		const total = uint64(1<<53 + 1)
		first := common.ChangeFeedID{
			Id:          common.NewGIDWithValue(total, 101),
			DisplayName: common.NewChangeFeedDisplayName("payments", "tenant-a"),
		}
		second := common.ChangeFeedID{
			Id:          common.NewGIDWithValue(789, total),
			DisplayName: common.NewChangeFeedDisplayName("payments", "tenant-a"),
		}
		records := []traffic.ChangefeedMetering{
			{KeyspaceID: 12345, ChangefeedID: first, EffectiveBytes: total},
			{KeyspaceID: 67890, ChangefeedID: common.NewChangeFeedIDWithName("orders", "tenant-a"), EffectiveBytes: 42},
			{KeyspaceID: 12345, ChangefeedID: second, EffectiveBytes: 0},
		}
		r, err := metering.NewReporter(cfg)
		require.NoError(t, err)
		defer r.Close()
		ts := time.Now().Truncate(time.Minute).Unix()
		require.NoError(t, r.Report(t.Context(), records))

		checkRecords := func(timestamp int64) {
			t.Helper()
			expectedKeyspaces := []traffic.KeyspaceMetering{
				{KeyspaceName: "tenant-a", Changefeeds: []traffic.ChangefeedMetering{records[0], records[2]}},
				{KeyspaceName: "tenant-a", Changefeeds: []traffic.ChangefeedMetering{records[1]}},
			}
			base := filepath.Join(dir, "test/metering/ru", strconv.FormatInt(timestamp, 10), "ticdc/pool1")
			paths, err := filepath.Glob(filepath.Join(base, "*.json.gz"))
			require.NoError(t, err)
			require.Len(t, paths, 2)
			for i, selfID := range []string{"12345", "67890"} {
				f, err := os.Open(filepath.Join(base, selfID+"-0.json.gz"))
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
						KeyspaceName string `json:"keyspace_name"`
						Changefeeds  []struct {
							ChangefeedID struct {
								ID   common.GID `json:"id"`
								Name string     `json:"name"`
							} `json:"changefeed_id"`
							EffectiveBytes uint64 `json:"effective_bytes"`
						} `json:"changefeeds"`
					} `json:"data"`
				}
				decoder := json.NewDecoder(gz)
				decoder.DisallowUnknownFields()
				require.NoError(t, decoder.Decode(&payload))
				require.NoError(t, gz.Close())
				require.NoError(t, f.Close())
				require.Equal(t, timestamp, payload.Timestamp)
				require.Equal(t, "ticdc", payload.Category)
				require.Equal(t, selfID, payload.SelfID)
				require.Equal(t, "pool1", payload.SharedPoolID)
				require.Zero(t, payload.Part)
				require.Len(t, payload.Data, 1)
				require.Equal(t, expectedKeyspaces[i].KeyspaceName, payload.Data[0].KeyspaceName)
				require.Len(t, payload.Data[0].Changefeeds, len(expectedKeyspaces[i].Changefeeds))
				for j, cf := range payload.Data[0].Changefeeds {
					expected := expectedKeyspaces[i].Changefeeds[j]
					require.Equal(t, expected.ChangefeedID.ID(), cf.ChangefeedID.ID)
					require.Equal(t, expected.ChangefeedID.Name(), cf.ChangefeedID.Name)
					require.Equal(t, expected.EffectiveBytes, cf.EffectiveBytes)
				}
			}
		}
		checkRecords(ts)

		provider, err := storage.NewObjectStorageProvider(cfg.ToProviderConfig())
		require.NoError(t, err)
		reader := meteringreader.NewMeteringReader(provider, config.DefaultConfig())
		defer reader.Close()
		for i, selfID := range []string{"12345", "67890"} {
			path := fmt.Sprintf("metering/ru/%d/ticdc/pool1/%s-0.json.gz", ts, selfID)
			info, err := reader.GetFileInfo(path)
			require.NoError(t, err)
			require.Equal(t, selfID, info.SelfID)
			data, err := reader.ReadFile(t.Context(), path)
			require.NoError(t, err)
			require.Len(t, data.Data, 1)
			require.Equal(t, records[i].ChangefeedID.Keyspace(), data.Data[0]["keyspace_name"])
		}

		time.Sleep(5 * time.Second)
		records[0].EffectiveBytes++
		require.NoError(t, r.Report(t.Context(), records))
		checkRecords(ts)
		other, err := metering.NewReporter(cfg)
		require.NoError(t, err)
		time.Sleep(5 * time.Second)
		records[0].EffectiveBytes++
		require.NoError(t, other.Report(t.Context(), records))
		other.Close()
		checkRecords(ts)
		previousPath := filepath.Join(dir, "test/metering/ru", strconv.FormatInt(ts, 10), "ticdc/pool1/12345-0.json.gz")
		previousData, err := os.ReadFile(previousPath)
		require.NoError(t, err)

		time.Sleep(time.Minute)
		records[0].EffectiveBytes++
		require.NoError(t, r.Report(t.Context(), records))
		checkRecords(ts + 60)
		retainedData, err := os.ReadFile(previousPath)
		require.NoError(t, err)
		require.Equal(t, previousData, retainedData)

		r.Close()
		r.Close()
		require.ErrorIs(t, r.Report(t.Context(), records), errors.ErrMeteringReporterClosed)
	})
}

func TestReporterPartialFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		r, err := metering.NewReporter(config.NewMeteringConfig().WithLocalFS(dir))
		require.NoError(t, err)
		defer r.Close()
		records := []traffic.ChangefeedMetering{
			{KeyspaceID: 12345, ChangefeedID: common.NewChangeFeedIDWithName("payments", "tenant-a")},
			{KeyspaceID: 67890, ChangefeedID: common.NewChangeFeedIDWithName("payments", "tenant-b")},
			{KeyspaceID: 99999, ChangefeedID: common.NewChangeFeedIDWithName("payments", "tenant-c")},
		}
		ts := time.Now().Truncate(time.Minute).Unix()
		base := filepath.Join(dir, "metering/ru", strconv.FormatInt(ts, 10), "ticdc/default-shared-pool")
		blockedPath := filepath.Join(base, "67890-0.json.gz")
		require.NoError(t, os.MkdirAll(blockedPath, 0o700))
		require.Error(t, r.Report(t.Context(), records))
		require.FileExists(t, filepath.Join(base, "12345-0.json.gz"))
		require.DirExists(t, blockedPath)
		require.NoFileExists(t, filepath.Join(base, "99999-0.json.gz"))
		require.NoError(t, os.Remove(blockedPath))
		require.NoError(t, r.Report(t.Context(), records))
		require.FileExists(t, blockedPath)
		require.FileExists(t, filepath.Join(base, "99999-0.json.gz"))
	})
}

// Exercise the public reporter through an HTTP storage boundary. Returning 400
// prevents the AWS SDK's own retries from masking a missing reporter retry.
func TestReporterUploadFailures(t *testing.T) {
	for _, tc := range []struct {
		name         string
		failures     int
		cancelUpload bool
	}{
		{name: "transient failure", failures: 1},
		{name: "last attempt succeeds", failures: 2},
		{name: "retry exhausted", failures: 3},
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
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				switch r.Method {
				case http.MethodHead:
					heads++
					w.WriteHeader(http.StatusOK)
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
			err = reporter.Report(ctx, []traffic.ChangefeedMetering{
				{KeyspaceID: 12345, ChangefeedID: common.NewChangeFeedIDWithName("payments", "tenant-a"), EffectiveBytes: 42},
			})
			mu.Lock()
			defer mu.Unlock()
			require.Zero(t, heads, "overwrite must upload directly without checking existence")
			switch {
			case tc.cancelUpload:
				require.ErrorIs(t, err, context.Canceled)
				require.Len(t, paths, 1, "cancellation must stop further uploads")
			case tc.failures == 3:
				require.ErrorContains(t, err, "injected upload failure")
				require.Len(t, paths, 3, "report must terminate after the configured retry budget")
			default:
				require.NoError(t, err)
				require.Len(t, paths, tc.failures+1)
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
	records := []traffic.ChangefeedMetering{{KeyspaceID: 12345, ChangefeedID: common.NewChangeFeedIDWithName("payments", "tenant-a")}}
	require.NoError(t, r.Report(t.Context(), nil))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, r.Report(ctx, records), context.Canceled)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestReporterRejectsEmptyKeyspace(t *testing.T) {
	dir := t.TempDir()
	r, err := metering.NewReporter(config.NewMeteringConfig().WithLocalFS(dir))
	require.NoError(t, err)
	defer r.Close()
	records := []traffic.ChangefeedMetering{{KeyspaceID: 12345, ChangefeedID: common.NewChangeFeedIDWithName("payments", "tenant-a")}, {}}
	require.ErrorIs(t, r.Report(t.Context(), records), errors.ErrMeteringInvalidBatch)
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
	for i := range calls {
		wg.Go(func() {
			<-start
			errCh <- r.Report(t.Context(), []traffic.ChangefeedMetering{
				{KeyspaceID: uint32(i), ChangefeedID: common.NewChangeFeedIDWithName("payments", fmt.Sprintf("tenant-%d", i))},
			})
		})
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
	paths, err := filepath.Glob(filepath.Join(dir, "metering/ru/*/ticdc/default-shared-pool/*.json.gz"))
	require.NoError(t, err)
	require.Len(t, paths, calls)
	for i := range calls {
		require.Contains(t, paths, filepath.Join(filepath.Dir(paths[0]), fmt.Sprintf("%d-0.json.gz", i)))
	}
}
