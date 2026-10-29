// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
package provider

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/snapshot/protocol"
	"github.com/stretchr/testify/require"
)

func TestPublishedExportRecoveredAndCursorFinalTail(t *testing.T) {
	s := New(t.TempDir(), "must-not-run")
	s.Workers = 4
	spec := protocol.Spec{ProtocolVersion: 1, JobID: "job", SnapshotID: "s", SnapshotTS: 42, SelectedSpans: []protocol.Span{{TableID: 1, Start: []byte("a"), End: []byte("z")}}}
	plan := protocol.Plan{ProtocolVersion: 1, JobID: "job", SnapshotID: "s", SnapshotTS: 42, Ranges: []protocol.Span{{RangeID: "r0", TableID: 1, Start: []byte("a"), End: []byte("m")}, {RangeID: "r1", TableID: 1, Start: []byte("m"), End: []byte("z")}}}
	ref, e := s.Objects.Put("plan", plan)
	require.NoError(t, e)
	specRef, e := s.Objects.Put("spec", spec)
	require.NoError(t, e)
	_, e = s.Objects.Put(key("job"), Job{Spec: spec, SpecRef: specRef, Phase: "MATERIALIZING", PlanRef: ref})
	require.NoError(t, e)
	for _, r := range plan.Ranges {
		_, e = s.Objects.Put("br/job/range-exports/"+r.RangeID+".json", protocol.Export{ProtocolVersion: 1, JobID: "job", SnapshotID: "s", SnapshotTS: 42, PlanDigest: ref.Digest, Range: r, AttemptID: "a"})
		require.NoError(t, e)
	}
	server := httptest.NewServer(s)
	defer server.Close()
	client := Client{URL: server.URL}
	// Simulate Provider restart after export publication but before journal
	// update. A status poll must restart reconciliation without another Ensure.
	var recovered Job
	require.Eventually(t, func() bool {
		err := client.Call(context.Background(), "status", Request{JobID: "job"}, &recovered)
		return err == nil && recovered.Phase == "SOURCE_COMPLETE"
	}, time.Second, 10*time.Millisecond)
	var page Page
	require.NoError(t, client.Call(context.Background(), "list", Request{JobID: "job", Limit: 1}, &page))
	require.Len(t, page.Exports, 1)
	require.False(t, page.Complete)
	first := page.Cursor
	require.NoError(t, client.Call(context.Background(), "list", Request{JobID: "job", Limit: 1, Cursor: first}, &page))
	require.True(t, page.Complete)
	require.Len(t, page.Exports, 1)
	require.NoError(t, (LocalClient{Server: s}).Call(context.Background(), "list", Request{JobID: "job", Limit: 1, Cursor: first}, &page))
	require.True(t, page.Complete)
	var job Job
	require.NoError(t, client.Call(context.Background(), "ensure", Request{SpecRef: specRef}, &job))
	require.Equal(t, "SOURCE_COMPLETE", job.Phase)
	spec.BackupDigest = "different"
	other, e := s.Objects.Put("other-spec", spec)
	require.NoError(t, e)
	require.Error(t, client.Call(context.Background(), "ensure", Request{SpecRef: other}, &job))
}

func TestEmbeddedCloseStopsExportWithoutFailingJob(t *testing.T) {
	root := t.TempDir()
	marker := filepath.Join(root, "started")
	t.Setenv("SNAPSHOT_TEST_MARKER", marker)
	binary := filepath.Join(root, "cse-ctl")
	require.NoError(t, os.WriteFile(binary, []byte("#!/bin/sh\nprintf started > \"$SNAPSHOT_TEST_MARKER\"\nexec sleep 30\n"), 0o700))
	s := New(root, binary)
	spec := protocol.Spec{ProtocolVersion: 1, JobID: "job", SnapshotID: "s", SnapshotTS: 42, SelectedSpans: []protocol.Span{{TableID: 1, Start: []byte("a"), End: []byte("z")}}}
	ref, err := s.Objects.Put("spec", spec)
	require.NoError(t, err)
	var job Job
	require.NoError(t, (LocalClient{Server: s}).Call(context.Background(), "ensure", Request{SpecRef: ref}, &job))
	require.Eventually(t, func() bool {
		_, err := os.Stat(marker)
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
	s.Close()
	_, err = s.Objects.Read(key("job"), &job)
	require.NoError(t, err)
	require.Equal(t, "PLANNING", job.Phase)
}

func TestLocalExportPausesWhenArtifactDiskIsLow(t *testing.T) {
	s := New(t.TempDir(), "unused")
	s.MinFreeBytes = 1 << 62
	_, err := s.Objects.Put(key("job"), Job{Phase: "MATERIALIZING"})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.Error(t, s.waitForSpace(ctx, "job"))
	var job Job
	_, err = s.Objects.Read(key("job"), &job)
	require.NoError(t, err)
	require.Equal(t, "BLOCKED_SPACE", job.Phase)
}

func TestRetryBatchUsesFreshAttemptsAndStopsOnCancellation(t *testing.T) {
	attempts := 0
	want := protocol.ObjectRef{URI: "candidate.json"}
	got, err := retryBatch(context.Background(), func() (protocol.ObjectRef, error) {
		attempts++
		if attempts < 3 {
			return protocol.ObjectRef{}, protocol.Invalid("temporary CSE failure")
		}
		return want, nil
	})
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, 3, attempts)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	attempts = 0
	_, err = retryBatch(ctx, func() (protocol.ObjectRef, error) {
		attempts++
		return protocol.ObjectRef{}, nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, attempts)
}

func TestMaterializeRangesBoundedAndComplete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ranges := make([]protocol.Span, 20)
	for i := range ranges {
		ranges[i].TableID = int64(i)
	}
	var active, peak atomic.Int32
	var mu sync.Mutex
	seen := make(map[int64]int)
	started := make(chan struct{}, 20)
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- materializeRanges(ctx, 4, ranges, func(ctx context.Context, r protocol.Span) error {
			n := active.Add(1)
			defer active.Add(-1)
			for old := peak.Load(); n > old; old = peak.Load() {
				if peak.CompareAndSwap(old, n) {
					break
				}
			}
			started <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
			mu.Lock()
			seen[r.TableID]++
			mu.Unlock()
			return nil
		})
	}()
	for i := 0; i < 4; i++ {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("four workers did not start")
		}
	}
	require.EqualValues(t, 4, active.Load())
	close(release)
	require.NoError(t, <-done)
	require.EqualValues(t, 4, peak.Load())
	require.Zero(t, active.Load())
	require.Len(t, seen, len(ranges))
	for _, count := range seen {
		require.Equal(t, 1, count)
	}
}

func TestMaterializeRangesFailureCancelsAndJoins(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ranges := make([]protocol.Span, 20)
	for i := range ranges {
		ranges[i].TableID = int64(i)
	}
	failure := protocol.Invalid("worker failed")
	started := make(chan struct{}, 4)
	fail := make(chan struct{})
	var active atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- materializeRanges(ctx, 4, ranges, func(ctx context.Context, r protocol.Span) error {
			active.Add(1)
			defer active.Add(-1)
			started <- struct{}{}
			if r.TableID == 0 {
				select {
				case <-fail:
					return failure
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	for i := 0; i < 4; i++ {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("four workers did not start")
		}
	}
	close(fail)
	require.ErrorIs(t, <-done, failure)
	require.Zero(t, active.Load())
}
