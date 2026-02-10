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
	dispatchers := []DispatcherEpoch{{DispatcherID: dispatcherID, Epoch: 1}}

	reported, handled := reporter.Report(time.Now(), dispatchers)
	require.True(t, handled)
	require.Equal(t, []common.DispatcherID{dispatcherID}, reported)
	select {
	case event := <-ch:
		require.Equal(t, []common.DispatcherID{dispatcherID}, event.DispatcherIDs)
	default:
		t.Fatal("expected recoverable event to be sent")
	}

	reported, handled = reporter.Report(time.Now(), dispatchers)
	require.True(t, handled)
	require.Empty(t, reported)
	select {
	case event := <-ch:
		t.Fatalf("unexpected duplicate event: %+v", event)
	default:
	}

	dispatchers[0].Epoch = 2
	reported, handled = reporter.Report(time.Now(), dispatchers)
	require.True(t, handled)
	require.Equal(t, []common.DispatcherID{dispatcherID}, reported)
	select {
	case event := <-ch:
		require.Equal(t, []common.DispatcherID{dispatcherID}, event.DispatcherIDs)
	default:
		t.Fatal("expected recoverable event for new epoch")
	}
}

func TestTransientErrorReporter_AckAllowsReportingSameEpochAgain(t *testing.T) {
	ch := make(chan *ErrorEvent, 2)
	reporter := NewTransientErrorReporter(ch)

	dispatcherID := common.NewDispatcherID()
	dispatchers := []DispatcherEpoch{{DispatcherID: dispatcherID, Epoch: 10}}

	reported, handled := reporter.Report(time.Now(), dispatchers)
	require.True(t, handled)
	require.Equal(t, []common.DispatcherID{dispatcherID}, reported)
	<-ch

	reported, handled = reporter.Report(time.Now(), dispatchers)
	require.True(t, handled)
	require.Empty(t, reported)

	reporter.Ack(dispatchers)
	reported, handled = reporter.Report(time.Now(), dispatchers)
	require.True(t, handled)
	require.Equal(t, []common.DispatcherID{dispatcherID}, reported)
	select {
	case event := <-ch:
		require.Equal(t, []common.DispatcherID{dispatcherID}, event.DispatcherIDs)
	default:
		t.Fatal("expected event after ack clears reported state")
	}
}

func TestTransientErrorReporter_ReportReturnsFalseWhenOutputUnavailable(t *testing.T) {
	reporter := NewTransientErrorReporter(nil)
	dispatcherID := common.NewDispatcherID()
	reported, handled := reporter.Report(
		time.Now(),
		[]DispatcherEpoch{{DispatcherID: dispatcherID, Epoch: 1}},
	)
	require.False(t, handled)
	require.Empty(t, reported)
}
