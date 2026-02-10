package recoverable

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestTransientErrorReporter_ReportOncePerDispatcherEpoch(t *testing.T) {
	ch := make(chan *ErrorEvent, 2)
	reporter := NewTransientErrorReporter(ch)

	dispatcherID := common.NewDispatcherID()
	dispatcherIDs := []common.DispatcherID{dispatcherID}
	epochs := map[common.DispatcherID]uint64{dispatcherID: 1}

	reported, handled := reporter.Report(time.Now(), dispatcherIDs, epochs)
	require.True(t, handled)
	require.Equal(t, dispatcherIDs, reported)
	select {
	case event := <-ch:
		require.Equal(t, dispatcherIDs, event.DispatcherIDs)
	default:
		t.Fatal("expected recoverable event to be sent")
	}

	reported, handled = reporter.Report(time.Now(), dispatcherIDs, epochs)
	require.True(t, handled)
	require.Empty(t, reported)
	select {
	case event := <-ch:
		t.Fatalf("unexpected duplicate event: %+v", event)
	default:
	}

	epochs[dispatcherID] = 2
	reported, handled = reporter.Report(time.Now(), dispatcherIDs, epochs)
	require.True(t, handled)
	require.Equal(t, dispatcherIDs, reported)
	select {
	case event := <-ch:
		require.Equal(t, dispatcherIDs, event.DispatcherIDs)
	default:
		t.Fatal("expected recoverable event for new epoch")
	}
}

func TestTransientErrorReporter_AckAllowsReportingSameEpochAgain(t *testing.T) {
	ch := make(chan *ErrorEvent, 2)
	reporter := NewTransientErrorReporter(ch)

	dispatcherID := common.NewDispatcherID()
	dispatcherIDs := []common.DispatcherID{dispatcherID}
	epochs := map[common.DispatcherID]uint64{dispatcherID: 10}

	reported, handled := reporter.Report(time.Now(), dispatcherIDs, epochs)
	require.True(t, handled)
	require.Equal(t, dispatcherIDs, reported)
	<-ch

	reported, handled = reporter.Report(time.Now(), dispatcherIDs, epochs)
	require.True(t, handled)
	require.Empty(t, reported)

	reporter.Ack(dispatcherIDs, epochs)
	reported, handled = reporter.Report(time.Now(), dispatcherIDs, epochs)
	require.True(t, handled)
	require.Equal(t, dispatcherIDs, reported)
	select {
	case event := <-ch:
		require.Equal(t, dispatcherIDs, event.DispatcherIDs)
	default:
		t.Fatal("expected event after ack clears reported state")
	}
}

func TestTransientErrorReporter_ReportReturnsFalseWhenOutputUnavailable(t *testing.T) {
	reporter := NewTransientErrorReporter(nil)
	dispatcherID := common.NewDispatcherID()
	reported, handled := reporter.Report(
		time.Now(),
		[]common.DispatcherID{dispatcherID},
		map[common.DispatcherID]uint64{dispatcherID: 1},
	)
	require.False(t, handled)
	require.Empty(t, reported)
}
