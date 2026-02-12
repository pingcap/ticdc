package dispatchermanager

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestSendRecoverDispatcherMessagesSendFailureFallsBackToErrorChannel(t *testing.T) {
	recoverQueue := make(chan *RecoverDispatcherRequestWithTargetID, 1)
	fallbackErrCh := make(chan error, 1)
	sendErr := errors.New("send command failed")
	mc := messaging.NewMockMessageCenter()
	mc.SetSendCommandError(sendErr)
	c := &HeartBeatCollector{
		recoverRequestCh: recoverQueue,
		mc:               mc,
	}

	changefeedID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	recoverQueue <- &RecoverDispatcherRequestWithTargetID{
		TargetID: node.ID("maintainer"),
		Request: &heartbeatpb.RecoverDispatcherRequest{
			ChangefeedID:  changefeedID.ToPB(),
			DispatcherIDs: []*heartbeatpb.DispatcherID{common.NewDispatcherID().ToPB()},
		},
		FallbackErrCh: fallbackErrCh,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- c.sendRecoverDispatcherMessages(ctx)
	}()

	select {
	case err := <-fallbackErrCh:
		code, ok := errors.RFCCode(err)
		require.True(t, ok)
		require.Equal(t, errors.ErrChangefeedRetryable.RFCCode(), code)
	case <-time.After(2 * time.Second):
		t.Fatal("expected fallback error to be sent")
	}

	cancel()
	select {
	case err := <-done:
		require.Equal(t, context.Canceled, errors.Cause(err))
	case <-time.After(2 * time.Second):
		t.Fatal("sendRecoverDispatcherMessages did not exit after context canceled")
	}
}
