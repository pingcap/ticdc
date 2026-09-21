// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
package store

import (
	"sync"
	"testing"

	"github.com/pingcap/ticdc/pkg/snapshot/protocol"
	"github.com/stretchr/testify/require"
)

func TestCASAndTaskRecovery(t *testing.T) {
	s := Local{Root: t.TempDir()}
	ok, e := s.CAS("counter", "", 0)
	require.NoError(t, e)
	require.True(t, ok)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				var n int
				v, e := s.Read("counter", &n)
				if e != nil {
					t.Error(e)
					return
				}
				ok, e := s.CAS("counter", v, n+1)
				if e != nil {
					t.Error(e)
					return
				}
				if ok {
					return
				}
			}
		}()
	}
	wg.Wait()
	var n int
	_, e = s.Read("counter", &n)
	require.NoError(t, e)
	require.Equal(t, 16, n)
	plan := protocol.Plan{Ranges: []protocol.Span{{RangeID: "r0"}, {RangeID: "empty"}}}
	tasks := Tasks{Objects: s, Job: "job", Digest: "plan", Epoch: 1, Plan: plan}
	require.NoError(t, tasks.Init())
	ref := protocol.ObjectRef{URI: "x", Digest: "d"}
	require.NoError(t, tasks.Ready("r0", ref))
	first, e := tasks.Assign("r0")
	require.NoError(t, e)
	next := tasks
	next.Epoch = 2
	require.NoError(t, next.Init())
	require.Error(t, tasks.Commit("r0", first.Lease, 5))
	second, e := next.Assign("r0")
	require.NoError(t, e)
	require.Greater(t, second.Lease, first.Lease)
	require.Error(t, next.Commit("r0", first.Lease, 5))
	require.NoError(t, next.Commit("r0", second.Lease, 5))
	require.NoError(t, next.Ready("r0", ref))
	require.Error(t, next.Ready("r0", protocol.ObjectRef{URI: "other"}))
	require.NoError(t, next.Ready("empty", ref))
	empty, e := next.Assign("empty")
	require.NoError(t, e)
	require.NoError(t, next.Commit("empty", empty.Lease, 0))
	require.NoError(t, next.Init())
	all, e := next.List()
	require.NoError(t, e)
	require.Equal(t, "APPLIED", all[0].State)
	require.Equal(t, "APPLIED", all[1].State)
}
