package dispatchermanager

import (
	"context"
	"testing"
	"time"

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

type failingCommandMessageCenter struct {
	sendCommandErr error
}

func (m *failingCommandMessageCenter) SendEvent(_ *messaging.TargetMessage) error {
	return nil
}

func (m *failingCommandMessageCenter) SendCommand(_ *messaging.TargetMessage) error {
	return m.sendCommandErr
}

func (m *failingCommandMessageCenter) IsReadyToSend(_ node.ID) bool {
	return true
}

func (m *failingCommandMessageCenter) RegisterHandler(_ string, _ messaging.MessageHandler) {}

func (m *failingCommandMessageCenter) DeRegisterHandler(_ string) {}

func (m *failingCommandMessageCenter) OnNodeChanges(_ map[node.ID]*node.Info) {}

func (m *failingCommandMessageCenter) Close() {}

func TestSendRecoverDispatcherMessagesSendFailureFallsBackToErrorChannel(t *testing.T) {
	recoverQueue := NewRecoverDispatcherRequestQueue()
	fallbackErrCh := make(chan error, 1)
	sendErr := perrors.New("send command failed")
	c := &HeartBeatCollector{
		recoverReqQueue: recoverQueue,
		mc:              &failingCommandMessageCenter{sendCommandErr: sendErr},
	}

	changefeedID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	recoverQueue.Enqueue(&RecoverDispatcherRequestWithTargetID{
		TargetID: node.ID("maintainer"),
		Request: &heartbeatpb.RecoverDispatcherRequest{
			ChangefeedID:  changefeedID.ToPB(),
			DispatcherIDs: []*heartbeatpb.DispatcherID{common.NewDispatcherID().ToPB()},
		},
		FallbackErrCh: fallbackErrCh,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- c.sendRecoverDispatcherMessages(ctx)
	}()

	select {
	case err := <-fallbackErrCh:
		code, ok := cerror.RFCCode(err)
		require.True(t, ok)
		require.Equal(t, cerror.ErrChangefeedRetryable.RFCCode(), code)
	case <-time.After(2 * time.Second):
		t.Fatal("expected fallback error to be sent")
	}

	cancel()
	select {
	case err := <-done:
		require.Equal(t, context.Canceled, perrors.Cause(err))
	case <-time.After(2 * time.Second):
		t.Fatal("sendRecoverDispatcherMessages did not exit after context canceled")
	}
}
