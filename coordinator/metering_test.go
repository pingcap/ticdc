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
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	meteringconfig "github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/coordinator/gccleaner"
	"github.com/pingcap/ticdc/coordinator/operator"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metering"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/prometheus/client_golang/prometheus/testutil"
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

func TestMeteringInitializationDegrades(t *testing.T) {
	original := config.GetGlobalServerConfig()
	t.Cleanup(func() { config.StoreGlobalServerConfig(original) })
	base := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(base, []byte("existing file"), 0o600))
	for _, tc := range []struct {
		name string
		cfg  *meteringconfig.MeteringConfig
	}{
		{"unsupported provider", &meteringconfig.MeteringConfig{Type: "unknown"}},
		{"provider initialization", meteringconfig.NewMeteringConfig().WithLocalFS(filepath.Join(base, "child"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := original.Clone()
			cfg.Metering = tc.cfg
			config.StoreGlobalServerConfig(cfg)
			meteringEnabledGauge.Set(1)
			c := &coordinator{
				gcTickInterval:     time.Hour,
				controller:         &Controller{},
				gcCleaner:          gccleaner.New(nil, "test"),
				eventCh:            chann.NewAutoDrainChann[*Event](),
				changefeedChangeCh: make(chan []*changefeedChange),
			}
			if reporter := createMeteringReporter(); reporter != nil {
				c.metering = newMeteringReporter(reporter, c.controller.metering)
			}
			defer c.eventCh.CloseAndDrain()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- c.Run(ctx) }()
			select {
			case c.changefeedChangeCh <- nil:
			case err := <-done:
				t.Fatalf("coordinator exited instead of degrading metering: %v", err)
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
			require.Nil(t, c.metering)
			require.Zero(t, testutil.ToFloat64(meteringEnabledGauge))
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
				controller:         &Controller{metering: newMeteringAccumulator()},
				gcCleaner:          gccleaner.New(nil, "test"),
				eventCh:            chann.NewAutoDrainChann[*Event](),
				changefeedChangeCh: make(chan []*changefeedChange),
			}
			if reporter := createMeteringReporter(); reporter != nil {
				c.metering = newMeteringReporter(reporter, c.controller.metering)
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
				_, err := os.Stat(dir)
				require.NoError(t, err)
				require.ErrorContains(t, c.metering.reporter.Report(t.Context(), 0, nil), "closed")
			}
		})
	}
}

func TestMeteringAccumulatorConcurrencyAndDeletion(t *testing.T) {
	acc := newMeteringAccumulator()

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

	// Snapshot pending state, then concurrently accept more bytes AND delete cf1 before Commit.
	snap, total := acc.Snapshot()
	require.Equal(t, uint64(100), total)
	require.Len(t, snap, 1)
	require.Equal(t, uint64(100), snap[0].EffectiveBytes)

	acked, ok = acc.Accept(cf1, 2, 25)
	require.True(t, ok)
	require.Equal(t, uint64(2), acked)
	acc.Forget(cf1)

	// Commit must deduct only the snapshotted 100 bytes, leaving 25.
	require.Equal(t, uint64(25), acc.Commit(snap))

	// Next upload commits the remaining 25 bytes and cleans up the pending entry.
	snap, total = acc.Snapshot()
	require.Equal(t, uint64(25), total)
	require.Len(t, snap, 1)
	require.Equal(t, uint64(25), snap[0].EffectiveBytes)
	require.Zero(t, acc.Commit(snap))
	snap, total = acc.Snapshot()
	require.Zero(t, total)
	require.Empty(t, snap)
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

		c := &coordinator{
			gcTickInterval:     time.Hour,
			controller:         ctrl,
			metering:           newMeteringReporter(createMeteringReporter(), ctrl.metering),
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
		_, total := ctrl.metering.Snapshot()
		require.Equal(t, uint64(500), total)

		// Add 250 more bytes -> next whole minute merges all 750 bytes.
		_, ok = ctrl.metering.Accept(cf, 2, 250)
		require.True(t, ok)
		time.Sleep(time.Minute)
		synctest.Wait()
		_, total = ctrl.metering.Snapshot()
		require.Zero(t, total)

		uploadedPath := filepath.Join(dir, "metering/ru", strconv.FormatInt(nextMinute+60, 10), "ticdc/pool1/cluster_1-0.json.gz")
		payload := readMeteringFile(t, uploadedPath)
		require.Len(t, payload.Data, 1)
		require.Equal(t, "ks-a", payload.Data[0].ClusterID)
		require.Equal(t, "cf-a", payload.Data[0].Changefeed)
		require.Equal(t, uint64(750), payload.Data[0].EffectiveBytes.Value)

		// Add final bytes and delete before shutdown; pre-create next unused minute so
		// shutdown flush retries and advances cleanly to nextMinute+180 within its budget.
		_, ok = ctrl.metering.Accept(cf, 3, 120)
		require.True(t, ok)
		ctrl.metering.Forget(cf)

		shutdownCollisionPath := filepath.Join(dir, "metering/ru", strconv.FormatInt(nextMinute+120, 10), "ticdc/pool1/cluster_1-0.json.gz")
		require.NoError(t, os.MkdirAll(filepath.Dir(shutdownCollisionPath), 0o755))
		require.NoError(t, os.WriteFile(shutdownCollisionPath, []byte("prior-term"), 0o644))

		cancel()
		err = <-done
		require.ErrorIs(t, err, context.Canceled)
		_, total = ctrl.metering.Snapshot()
		require.Zero(t, total)

		shutdownPath := filepath.Join(dir, "metering/ru", strconv.FormatInt(nextMinute+180, 10), "ticdc/pool1/cluster_1-0.json.gz")
		shutdownPayload := readMeteringFile(t, shutdownPath)
		require.Len(t, shutdownPayload.Data, 1)
		require.Equal(t, uint64(120), shutdownPayload.Data[0].EffectiveBytes.Value)
	})
}

func TestNewControllerBootstrapAdvertisesMetering(t *testing.T) {
	self := node.NewInfo("127.0.0.1:8300", "")
	nodeManager := watcher.NewNodeManager(nil, nil)
	nodeManager.GetAliveNodes()[self.ID] = self
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)
	appcontext.SetService(appcontext.DefaultPDClock, pdutil.NewClock4Test())

	ctrl := NewController(
		77,
		self,
		make(chan []*changefeedChange, 1),
		nil,
		chann.NewAutoDrainChann[*Event](),
		1,
		time.Minute,
		nil,
		newMeteringAccumulator(),
	)
	t.Cleanup(func() {
		ctrl.taskScheduler.Stop()
		ctrl.eventCh.CloseAndDrain()
	})

	sent := <-mc.GetMessageChannel()
	msg := sent.Message[0].(*heartbeatpb.CoordinatorBootstrapRequest)
	require.True(t, msg.MeteringEnabled)
	require.Equal(t, int64(77), msg.Version)
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
	db.SetMeteringEpoch(42)
	db.AddReplicatingMaintainer(cf, owner)

	ctrl := &Controller{
		version:            42,
		changefeedDB:       db,
		messageCenter:      mc,
		operatorController: &operator.Controller{},
		metering:           newMeteringAccumulator(),
	}

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
	_, total := ctrl.metering.Snapshot()
	require.Equal(t, uint64(640), total)

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
	_, total = ctrl.metering.Snapshot()
	require.Equal(t, uint64(640), total)

	// Out-of-order seq must be dropped cleanly and increment meteringRejectedStatusCounter.
	rejectedBefore := testutil.ToFloat64(meteringRejectedStatusCounter)
	_ = ctrl.handleSingleMaintainerStatus(owner, &heartbeatpb.MaintainerStatus{
		ChangefeedID:    cfID.ToPB(),
		MaintainerEpoch: 7,
		CheckpointTs:    120,
		State:           heartbeatpb.ComponentState_Working,
		MeteringEpoch:   42,
		MeteringSeq:     3,
		MeteringBytes:   500,
	}, cfID)
	_, total = ctrl.metering.Snapshot()
	require.Equal(t, uint64(640), total)
	require.Equal(t, rejectedBefore+1, testutil.ToFloat64(meteringRejectedStatusCounter))
}

func TestCoordinatorMeteringResolvesUncertainUpload(t *testing.T) {
	for _, tc := range []struct {
		name            string
		persistOnFinal  bool
		expectedNextVal uint64
	}{
		{name: "persisted on final attempt commits prior batch", persistOnFinal: true, expectedNextVal: 250},
		{name: "not persisted merges prior batch into next minute", persistOnFinal: false, expectedNextVal: 750},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t1 := time.Now().Truncate(time.Minute)
			t2 := t1.Add(time.Minute)
			t1Seg := "/" + strconv.FormatInt(t1.Unix(), 10) + "/"
			t2Seg := "/" + strconv.FormatInt(t2.Unix(), 10) + "/"

			var mu sync.Mutex
			objects := make(map[string]decodedMeteringFile)
			t1PutCount := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				switch r.Method {
				case http.MethodHead:
					if _, ok := objects[r.URL.Path]; ok {
						w.WriteHeader(http.StatusOK)
					} else {
						w.WriteHeader(http.StatusNotFound)
					}
				case http.MethodPut:
					body, err := io.ReadAll(r.Body)
					require.NoError(t, err)
					gz, err := gzip.NewReader(bytes.NewReader(body))
					require.NoError(t, err)
					var payload decodedMeteringFile
					require.NoError(t, json.NewDecoder(gz).Decode(&payload))
					require.NoError(t, gz.Close())

					if strings.Contains(r.URL.Path, t1Seg) {
						t1PutCount++
						if tc.persistOnFinal && t1PutCount == 3 {
							objects[r.URL.Path] = payload
						}
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					objects[r.URL.Path] = payload
					w.WriteHeader(http.StatusOK)
				default:
					w.WriteHeader(http.StatusMethodNotAllowed)
				}
			}))
			defer server.Close()

			cfg := meteringconfig.NewMeteringConfig().WithS3("us-east-1", "bucket").WithEndpoint(server.URL).
				WithAWSConfig(&meteringconfig.MeteringAWSConfig{AccessKey: "test", SecretAccessKey: "test", S3ForcePathStyle: true})
			rep, err := metering.NewReporter(cfg, "cluster-1")
			require.NoError(t, err)
			defer rep.Close()

			acc := newMeteringAccumulator()
			mr := newMeteringReporter(rep, acc)
			cf := common.NewChangeFeedIDWithName("cf-a", "ks-a")
			_, ok := acc.Accept(cf, 1, 500)
			require.True(t, ok)

			// All attempts fail on t1, leaving upload persistence uncertain.
			err = mr.reportAt(t.Context(), t1)
			require.Error(t, err)
			require.True(t, rep.HasUncertainUpload())
			require.Len(t, mr.uncertainRecords, 1)

			// Additional bytes arrive before t2; reportAt(t2) must probe t1 first.
			_, ok = acc.Accept(cf, 2, 250)
			require.True(t, ok)
			require.NoError(t, mr.reportAt(t.Context(), t2))
			require.False(t, rep.HasUncertainUpload())
			require.Empty(t, mr.uncertainRecords)
			_, remaining := acc.Snapshot()
			require.Zero(t, remaining)

			mu.Lock()
			defer mu.Unlock()
			var t2Payload *decodedMeteringFile
			for p, f := range objects {
				if strings.Contains(p, t2Seg) {
					cp := f
					t2Payload = &cp
				}
			}
			require.NotNil(t, t2Payload)
			require.Len(t, t2Payload.Data, 1)
			require.Equal(t, tc.expectedNextVal, t2Payload.Data[0].EffectiveBytes.Value)
		})
	}
}
