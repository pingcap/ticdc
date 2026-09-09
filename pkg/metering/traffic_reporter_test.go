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

package metering

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/metering_sdk/common"
	"github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/writer"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestTrafficReporterRecords(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	dir := t.TempDir()
	cfg := config.NewMeteringConfig().WithLocalFS(dir).WithPrefix("test").WithSharedPoolID("pool1")
	const total = uint64(1<<53 + 1)
	records := []TrafficRecord{{ChangefeedGID: "gid1", TrafficBytes: total}, {ChangefeedGID: "gid2", TrafficBytes: 0}}
	w, err := NewTrafficReporter(cfg)
	require.NoError(t, err)
	defer w.Close()
	now := time.Unix(1788912010, 0)
	require.NoError(t, w.Report(context.Background(), now, records))
	// Same-minute reports and a restarted reporter must use separate paths.
	require.NoError(t, w.Report(context.Background(), now, records))
	other, err := NewTrafficReporter(cfg)
	require.NoError(t, err)
	require.NoError(t, other.Report(context.Background(), now, records))
	other.Close()

	paths, err := filepath.Glob(filepath.Join(dir, "test/metering/ru/1788912000/ticdc/pool1/*.json.gz"))
	require.NoError(t, err)
	require.Len(t, paths, 3)
	for _, path := range paths {
		f, err := os.Open(path)
		require.NoError(t, err)
		gz, err := gzip.NewReader(f)
		require.NoError(t, err)
		var payload struct {
			Timestamp    int64  `json:"timestamp"`
			SelfID       string `json:"self_id"`
			SharedPoolID string `json:"shared_pool_id"`
			Part         int    `json:"part"`
			Data         []struct {
				GID     string               `json:"changefeed_gid"`
				Traffic common.MeteringValue `json:"traffic_bytes"`
			} `json:"data"`
		}
		require.NoError(t, json.NewDecoder(gz).Decode(&payload))
		require.NoError(t, gz.Close())
		require.NoError(t, f.Close())
		require.Equal(t, int64(1788912000), payload.Timestamp)
		require.NotContains(t, payload.SelfID, "-")
		require.Equal(t, "pool1", payload.SharedPoolID)
		require.Zero(t, payload.Part)
		require.Len(t, payload.Data, 2)
		require.Equal(t, "gid1", payload.Data[0].GID)
		require.Equal(t, "gid2", payload.Data[1].GID)
		require.Equal(t, common.MeteringValue{Value: 0, Unit: "Bytes"}, payload.Data[1].Traffic)
		require.Equal(t, common.MeteringValue{Value: total, Unit: "Bytes"}, payload.Data[0].Traffic)
	}
	// An existing unrelated object cannot be acknowledged as this batch.
	collision := filepath.Join(filepath.Dir(paths[0]), w.id+"b"+strconv.FormatUint(w.sequence+1, 16)+"-0.json.gz")
	require.NoError(t, os.WriteFile(collision, []byte("unrelated object"), 0o600))
	require.ErrorIs(t, w.Report(context.Background(), now, records), writer.ErrFileExists)
	unchanged, err := os.ReadFile(collision)
	require.NoError(t, err)
	require.Equal(t, "unrelated object", string(unchanged))
	require.NoError(t, w.Report(context.Background(), now, records))
	require.NoError(t, w.Report(context.Background(), now, nil))
	require.Equal(t, uint64(4), w.sequence)
	w.Close()
	w.Close()
	require.Error(t, w.Report(context.Background(), now, records))
}

func TestWriteRetry(t *testing.T) {
	for _, tc := range []struct {
		name     string
		failures int
		exists   bool
		want     int
	}{
		{"success", 0, false, 1},
		{"transient", 2, false, 3},
		{"exhausted", 6, false, 6},
		{"existing file is not success", 1, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			attempts := 0
			err := writeWithRetry(context.Background(), func(ctx context.Context) error {
				attempts++
				deadline, ok := ctx.Deadline()
				require.True(t, ok)
				require.LessOrEqual(t, time.Until(deadline), writeTimeout)
				if tc.exists {
					return writer.ErrFileExists
				}
				if attempts <= tc.failures {
					return errors.ErrUnexpected.GenWithStackByArgs("upload failed")
				}
				return nil
			}, 0)
			require.Equal(t, tc.want, attempts)
			if tc.failures >= writeAttempts || tc.exists {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	err := writeWithRetry(ctx, func(context.Context) error {
		cancel()
		return context.Canceled
	}, time.Hour)
	require.ErrorIs(t, err, context.Canceled)
}

func TestTrafficReporterCancellationAndEmpty(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	dir := t.TempDir()
	w, err := NewTrafficReporter(config.NewMeteringConfig().WithLocalFS(dir))
	require.NoError(t, err)
	defer w.Close()
	now := time.Now()
	records := []TrafficRecord{{ChangefeedGID: "gid1", TrafficBytes: 1}}
	require.NoError(t, w.Report(context.Background(), now, nil))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, w.Report(ctx, now, records), context.Canceled)
	require.Error(t, w.Report(context.Background(), time.Unix(0, 0), records))
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
}
