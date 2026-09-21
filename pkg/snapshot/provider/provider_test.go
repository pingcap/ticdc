// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
package provider

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/snapshot/protocol"
	"github.com/stretchr/testify/require"
)

func TestPublishedExportRecoveredAndCursorFinalTail(t *testing.T) {
	s := New(t.TempDir(), "must-not-run")
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
	var job Job
	require.NoError(t, client.Call(context.Background(), "ensure", Request{SpecRef: specRef}, &job))
	require.Equal(t, "SOURCE_COMPLETE", job.Phase)
	spec.BackupDigest = "different"
	other, e := s.Objects.Put("other-spec", spec)
	require.NoError(t, e)
	require.Error(t, client.Call(context.Background(), "ensure", Request{SpecRef: other}, &job))
}
