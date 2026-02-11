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

package recoverable

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestReportOncePerDispatcherEpoch(t *testing.T) {
	ch := make(chan *ErrorEvent, 2)
	reporter := NewReporter(ch)

	dispatcherID := common.NewDispatcherID()
	dispatchers := []DispatcherEpoch{{DispatcherID: dispatcherID, Epoch: 1}}

	reported, handled := reporter.Report(dispatchers)
	require.True(t, handled)
	require.Equal(t, []common.DispatcherID{dispatcherID}, reported)
	select {
	case event := <-ch:
		require.Equal(t, []common.DispatcherID{dispatcherID}, event.DispatcherIDs)
	default:
		t.Fatal("expected recoverable event to be sent")
	}

	reported, handled = reporter.Report(dispatchers)
	require.True(t, handled)
	require.Empty(t, reported)
	select {
	case event := <-ch:
		t.Fatalf("unexpected duplicate event: %+v", event)
	default:
	}

	dispatchers[0].Epoch = 2
	reported, handled = reporter.Report(dispatchers)
	require.True(t, handled)
	require.Equal(t, []common.DispatcherID{dispatcherID}, reported)
	select {
	case event := <-ch:
		require.Equal(t, []common.DispatcherID{dispatcherID}, event.DispatcherIDs)
	default:
		t.Fatal("expected recoverable event for new epoch")
	}
}

func TestReportReturnsFalseWhenOutputUnavailable(t *testing.T) {
	reporter := NewReporter(nil)
	dispatcherID := common.NewDispatcherID()
	reported, handled := reporter.Report(
		[]DispatcherEpoch{{DispatcherID: dispatcherID, Epoch: 1}},
	)
	require.False(t, handled)
	require.Empty(t, reported)
}

func TestIgnoreStaleEpochAfterNewerEpochReported(t *testing.T) {
	ch := make(chan *ErrorEvent, 4)
	reporter := NewReporter(ch)

	dispatcherID := common.NewDispatcherID()

	// Report newer epoch first.
	reported, handled := reporter.Report(
		[]DispatcherEpoch{{DispatcherID: dispatcherID, Epoch: 3}},
	)
	require.True(t, handled)
	require.Equal(t, []common.DispatcherID{dispatcherID}, reported)
	<-ch

	// Stale epoch should be ignored.
	reported, handled = reporter.Report(
		[]DispatcherEpoch{{DispatcherID: dispatcherID, Epoch: 2}},
	)
	require.True(t, handled)
	require.Empty(t, reported)
	select {
	case event := <-ch:
		t.Fatalf("unexpected event for stale epoch: %+v", event)
	default:
	}
}
