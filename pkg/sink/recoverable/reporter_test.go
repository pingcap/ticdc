package recoverable

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestTransientErrorReporter_ReportOncePerDispatcherEpoch(t *testing.T) {
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

func TestTransientErrorReporter_ReportReturnsFalseWhenOutputUnavailable(t *testing.T) {
	reporter := NewReporter(nil)
	dispatcherID := common.NewDispatcherID()
	reported, handled := reporter.Report(
		[]DispatcherEpoch{{DispatcherID: dispatcherID, Epoch: 1}},
	)
	require.False(t, handled)
	require.Empty(t, reported)
}

func TestTransientErrorReporter_IgnoreStaleEpochAfterNewerEpochReported(t *testing.T) {
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
