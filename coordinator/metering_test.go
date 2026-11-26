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

package coordinator

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	meteringconfig "github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/coordinator/gccleaner"
	"github.com/pingcap/ticdc/coordinator/operator"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/stretchr/testify/require"
)

type decodedMeteringFile struct {
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

func readMeteringFile(t *testing.T, path string) decodedMeteringFile {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	gz, err := gzip.NewReader(f)
	require.NoError(t, err)
	defer gz.Close()
	var payload decodedMeteringFile
	dec := json.NewDecoder(gz)
	dec.DisallowUnknownFields()
	require.NoError(t, dec.Decode(&payload))
	return payload
}

func TestMeteringInitializationFailure(t *testing.T) {
	original := config.GetGlobalServerConfig()
	t.Cleanup(func() { config.StoreGlobalServerConfig(original) })
	base := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(base, []byte("existing file"), 0o600))
	for _, tc := range []struct {
		name string
		cfg  *meteringconfig.MeteringConfig
		code string
	}{
		{"unsupported provider", &meteringconfig.MeteringConfig{Type: "unknown"}, string(errors.ErrInvalidServerOption.RFCCode())},
		{"provider initialization", meteringconfig.NewMeteringConfig().WithLocalFS(filepath.Join(base, "child")), string(errors.ErrExternalStorageAPI.RFCCode())},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := original.Clone()
			cfg.Metering = tc.cfg
			config.StoreGlobalServerConfig(cfg)
			c := &coordinator{}
			// Initialization must fail before starting any coordinator loops.
			err := c.Run(t.Context())
			require.ErrorContains(t, err, tc.code)
			require.Nil(t, c.metering)
		})
	}
}

func TestMeteringCoordinatorTerm(t *testing.T) {
	original := config.GetGlobalServerConfig()
	t.Cleanup(func() { config.StoreGlobalServerConfig(original) })
	dir := filepath.Join(t.TempDir(), "metering")
	for _, tc := range []struct {
		name string
		cfg  *meteringconfig.MeteringConfig
	}{
		{"absent", nil},
		{"empty type", &meteringconfig.MeteringConfig{Bucket: "unused"}},
		{"enabled", meteringconfig.NewMeteringConfig().WithLocalFS(dir)},
		{"next term", meteringconfig.NewMeteringConfig().WithLocalFS(dir)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := original.Clone()
			cfg.ClusterID = "prod-us-1"
			cfg.Metering = tc.cfg
			config.StoreGlobalServerConfig(cfg)
			c := &coordinator{
				gcTickInterval:     time.Hour,
				controller:         &Controller{},
				gcCleaner:          gccleaner.New(nil, "test"),
				eventCh:            chann.NewAutoDrainChann[*Event](),
				changefeedChangeCh: make(chan []*changefeedChange),
			}
			defer c.eventCh.CloseAndDrain()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- c.Run(ctx) }()
			// An unbuffered submission completes only after the coordinator has
			// initialized and entered its event loop. No polling or sleep is needed.
			select {
			case c.changefeedChangeCh <- nil:
			case err := <-done:
				t.Fatalf("coordinator exited before handling a submission: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("coordinator did not start")
			}
			cancel()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(10 * time.Second):
				t.Fatal("coordinator did not stop")
			}
			if tc.cfg == nil || tc.cfg.Type == "" {
				require.Nil(t, c.metering)
				require.NoDirExists(t, dir)
			} else {
				require.NotNil(t, c.metering)
				require.Equal(t, "prod_us_1", c.metering.reporter.SelfID())
				_, err := os.Stat(dir)
				require.NoError(t, err)
				require.ErrorContains(t, c.metering.reporter.Report(t.Context(), 0, nil), "closed")
			}
		})
	}
}

func TestMeteringAccumulatorConcurrencyAndEviction(t *testing.T) {
	acc := newMeteringAccumulator()
	acc.maxPendingFinal = 2

	cf1 := common.NewChangeFeedIDWithName("cf-1", "ks-1")
	// Out-of-order seq must be rejected.
	acked, ok := acc.Accept(cf1, 2, 100)
	require.False(t, ok)
	require.Zero(t, acked)

	// Seq 1 succeeds.
	acked, ok = acc.Accept(cf1, 1, 100)
	require.True(t, ok)
	require.Equal(t, uint64(1), acked)

	// Duplicate seq 1 returns ACK idempotently without double-counting.
	acked, ok = acc.Accept(cf1, 1, 100)
	require.True(t, ok)
	require.Equal(t, uint64(1), acked)
	require.Equal(t, uint64(100), acc.PendingBytesTotal())

	// Snapshot pending state, then concurrently accept more bytes AND delete cf1 before Commit.
	snap := acc.Snapshot()
	require.Len(t, snap, 1)
	require.Equal(t, uint64(100), snap[0].EffectiveBytes)

	acked, ok = acc.Accept(cf1, 2, 25)
	require.True(t, ok)
	require.Equal(t, uint64(2), acked)
	acc.OnChangefeedRemoved(cf1)
	require.Equal(t, uint64(125), acc.PendingBytesTotal())

	// Commit must deduct only the snapshotted 100 bytes, leaving 25.
	acc.Commit(snap)
	require.Equal(t, uint64(25), acc.PendingBytesTotal())

	// Add 2 more removed records -> exceeds maxPendingFinal=2, evicting cf1's 25-byte record.
	cf2 := common.NewChangeFeedIDWithName("cf-2", "ks-1")
	cf3 := common.NewChangeFeedIDWithName("cf-3", "ks-1")
	_, ok = acc.Accept(cf2, 1, 200)
	require.True(t, ok)
	acc.OnChangefeedRemoved(cf2)
	_, ok = acc.Accept(cf3, 1, 300)
	require.True(t, ok)
	acc.OnChangefeedRemoved(cf3)

	require.Equal(t, uint64(500), acc.PendingBytesTotal())
	snap = acc.Snapshot()
	require.Len(t, snap, 2)
	acc.Commit(snap)
	require.Zero(t, acc.PendingBytesTotal())
}

func TestCoordinatorMeteringReportingAndShutdownFlush(t *testing.T) {
	original := config.GetGlobalServerConfig()
	t.Cleanup(func() { config.StoreGlobalServerConfig(original) })

	dir := t.TempDir()
	cfg := original.Clone()
	cfg.ClusterID = "cluster-1"
	cfg.Metering = meteringconfig.NewMeteringConfig().WithLocalFS(dir).WithSharedPoolID("pool1")
	config.StoreGlobalServerConfig(cfg)

	synctest.Test(t, func(t *testing.T) {
		ctrl := &Controller{
			changefeedDB:       changefeed.NewChangefeedDB(1),
			operatorController: &operator.Controller{},
			metering:           newMeteringAccumulator(),
		}
		ctrl.EnableMetering()

		c := &coordinator{
			gcTickInterval:     time.Hour,
			controller:         ctrl,
			gcCleaner:          gccleaner.New(nil, "test"),
			eventCh:            chann.NewAutoDrainChann[*Event](),
			changefeedChangeCh: make(chan []*changefeedChange),
		}
		defer c.eventCh.CloseAndDrain()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- c.Run(ctx) }()

		select {
		case c.changefeedChangeCh <- nil:
		case err := <-done:
			t.Fatalf("coordinator exited early: %v", err)
		}

		// Tick 1 with zero delta must not create any object.
		time.Sleep(time.Minute)
		synctest.Wait()
		paths, err := filepath.Glob(filepath.Join(dir, "metering/ru/*/ticdc/pool1/*.json.gz"))
		require.NoError(t, err)
		require.Empty(t, paths)

		// Pre-create object for the upcoming minute to simulate ErrFileExists collision.
		nextMinute := time.Now().Truncate(time.Minute).Add(time.Minute).Unix()
		conflictPath := filepath.Join(dir, "metering/ru", strconv.FormatInt(nextMinute, 10), "ticdc/pool1/cluster_1-0.json.gz")
		require.NoError(t, os.MkdirAll(filepath.Dir(conflictPath), 0o755))
		require.NoError(t, os.WriteFile(conflictPath, []byte("existing"), 0o644))

		cf := common.NewChangeFeedIDWithName("cf-a", "ks-a")
		_, ok := ctrl.metering.Accept(cf, 1, 500)
		require.True(t, ok)

		// Tick 2 hits ErrFileExists -> retains 500 bytes in memory.
		time.Sleep(time.Minute)
		synctest.Wait()
		require.Equal(t, uint64(500), ctrl.metering.PendingBytesTotal())

		// Add 250 more bytes -> next whole minute merges all 750 bytes.
		_, ok = ctrl.metering.Accept(cf, 2, 250)
		require.True(t, ok)
		time.Sleep(time.Minute)
		synctest.Wait()
		require.Zero(t, ctrl.metering.PendingBytesTotal())

		uploadedPath := filepath.Join(dir, "metering/ru", strconv.FormatInt(nextMinute+60, 10), "ticdc/pool1/cluster_1-0.json.gz")
		payload := readMeteringFile(t, uploadedPath)
		require.Len(t, payload.Data, 1)
		require.Equal(t, "ks-a", payload.Data[0].ClusterID)
		require.Equal(t, "cf-a", payload.Data[0].Changefeed)
		require.Equal(t, uint64(750), payload.Data[0].EffectiveBytes.Value)

		// Add final bytes and delete before shutdown -> shutdown flushes to next unused minute.
		_, ok = ctrl.metering.Accept(cf, 3, 120)
		require.True(t, ok)
		ctrl.metering.OnChangefeedRemoved(cf)

		cancel()
		err = <-done
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, ctrl.metering.PendingBytesTotal())

		shutdownPath := filepath.Join(dir, "metering/ru", strconv.FormatInt(nextMinute+120, 10), "ticdc/pool1/cluster_1-0.json.gz")
		shutdownPayload := readMeteringFile(t, shutdownPath)
		require.Len(t, shutdownPayload.Data, 1)
		require.Equal(t, uint64(120), shutdownPayload.Data[0].EffectiveBytes.Value)
	})
}

func TestControllerMeteringAckAndAddMaintainerRecovery(t *testing.T) {
	mc, _, _ := messaging.NewMessageCenterForTest(t)
	cfID := common.NewChangeFeedIDWithName("payments", "tenant-a")
	owner := node.ID("maintainer-node")
	info := &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Epoch:        7,
		SinkURI:      "blackhole://",
		Config:       config.GetDefaultReplicaConfig(),
	}
	cf := changefeed.NewChangefeed(cfID, info, 100, false)
	cf.SetNodeID(owner)

	db := changefeed.NewChangefeedDB(42)
	db.AddReplicatingMaintainer(cf, owner)

	ctrl := &Controller{
		version:            42,
		changefeedDB:       db,
		messageCenter:      mc,
		operatorController: &operator.Controller{},
		metering:           newMeteringAccumulator(),
	}
	ctrl.EnableMetering()

	// handleSingleMaintainerStatus accepts matching epoch and updates cf's AddMaintainerRequest state.
	_ = ctrl.handleSingleMaintainerStatus(owner, &heartbeatpb.MaintainerStatus{
		ChangefeedID:    cfID.ToPB(),
		MaintainerEpoch: 7,
		CheckpointTs:    110,
		State:           heartbeatpb.ComponentState_Working,
		MeteringEpoch:   42,
		MeteringSeq:     1,
		MeteringBytes:   640,
	}, cfID)
	require.Equal(t, uint64(640), ctrl.metering.PendingBytesTotal())

	addMsg := cf.NewAddMaintainerMessage(node.ID("new-maintainer")).Message[0].(*heartbeatpb.AddMaintainerRequest)
	require.Equal(t, uint64(42), addMsg.MeteringEpoch)
	require.Equal(t, uint64(1), addMsg.MeteringLastSeq)

	// Stale metering epoch must be ignored.
	_ = ctrl.handleSingleMaintainerStatus(owner, &heartbeatpb.MaintainerStatus{
		ChangefeedID:    cfID.ToPB(),
		MaintainerEpoch: 7,
		CheckpointTs:    115,
		State:           heartbeatpb.ComponentState_Working,
		MeteringEpoch:   41,
		MeteringSeq:     1,
		MeteringBytes:   999,
	}, cfID)
	require.Equal(t, uint64(640), ctrl.metering.PendingBytesTotal())
}
