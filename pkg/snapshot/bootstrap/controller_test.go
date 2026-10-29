// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
package bootstrap

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/snapshot/protocol"
	"github.com/pingcap/ticdc/pkg/snapshot/store"
	"github.com/stretchr/testify/require"
)

func TestApplyReadyBoundedReceiptsAndCancellation(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("failure=%v", fail), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			objects := store.Local{Root: t.TempDir()}
			spec := protocol.Spec{ProtocolVersion: 1, JobID: "j", SnapshotID: "s", SnapshotTS: 42}
			plan := protocol.Plan{ProtocolVersion: 1, JobID: "j", SnapshotID: "s", SnapshotTS: 42}
			for i := 0; i < 6; i++ {
				plan.Ranges = append(plan.Ranges, protocol.Span{RangeID: fmt.Sprint(i), TableID: int64(i + 1), Start: []byte("a"), End: []byte("z")})
			}
			ref, err := objects.Put("plan", plan)
			require.NoError(t, err)
			tasks := store.Tasks{Objects: objects, Job: "j", Digest: ref.Digest, Epoch: 1, Plan: plan}
			require.NoError(t, tasks.Init())
			for _, r := range plan.Ranges {
				ex := protocol.Export{ProtocolVersion: 1, JobID: "j", SnapshotID: "s", SnapshotTS: 42, PlanDigest: ref.Digest, Range: r, AttemptID: "a"}
				export, err := objects.Put("export-"+r.RangeID, ex)
				require.NoError(t, err)
				require.NoError(t, tasks.Ready(r.RangeID, export))
			}
			ready, err := tasks.List()
			require.NoError(t, err)
			capture := NewMockCapture(gomock.NewController(t))
			started := make(chan struct{}, 6)
			release := make(chan struct{})
			var active, peak atomic.Int32
			failure := protocol.Invalid("injected sink failure")
			capture.EXPECT().ApplySnapshot(gomock.Any(), gomock.Any(), uint64(1), spec, ref, gomock.Any()).DoAndReturn(func(ctx context.Context, _ common.ChangeFeedID, _ uint64, _ protocol.Spec, _ protocol.ObjectRef, ex protocol.Export) (uint64, error) {
				n := active.Add(1)
				defer active.Add(-1)
				for old := peak.Load(); n > old; old = peak.Load() {
					if peak.CompareAndSwap(old, n) {
						break
					}
				}
				started <- struct{}{}
				if fail && ex.Range.RangeID != "0" {
					<-ctx.Done()
					return 0, protocol.Wrap(ctx.Err())
				}
				select {
				case <-release:
				case <-ctx.Done():
					return 0, protocol.Wrap(ctx.Err())
				}
				if fail {
					return 0, failure
				}
				return 0, nil
			}).AnyTimes()
			c := Controller{Epoch: 1, Config: &protocol.Config{ApplyWorkers: 2}, Capture: capture}
			done := make(chan error, 1)
			go func() { done <- c.applyReady(ctx, tasks, spec, ref, ready) }()
			for i := 0; i < 2; i++ {
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatal("workers did not start")
				}
			}
			require.EqualValues(t, 2, active.Load())
			inProgress, err := tasks.List()
			require.NoError(t, err)
			for _, task := range inProgress {
				require.NotEqual(t, "APPLIED", task.State)
			}
			close(release)
			err = <-done
			if fail {
				require.ErrorIs(t, err, failure)
			} else {
				require.NoError(t, err)
			}
			require.Zero(t, active.Load())
			require.EqualValues(t, 2, peak.Load())
			final, err := tasks.List()
			require.NoError(t, err)
			for _, task := range final {
				if fail {
					require.NotEqual(t, "APPLIED", task.State)
				} else {
					require.Equal(t, "APPLIED", task.State)
				}
			}
		})
	}
}
