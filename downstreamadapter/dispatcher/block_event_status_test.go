package dispatcher

import (
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/stretchr/testify/require"
)

func assertBlockEventStatus(t *testing.T, status *BlockEventStatus, expectPending bool, expectedStage heartbeatpb.BlockStage) {
	t.Helper()

	event, stage := status.getEventAndStage()
	if expectPending {
		require.NotNil(t, event)
	} else {
		require.Nil(t, event)
	}
	require.Equal(t, expectedStage, stage)
}

func assertNoPendingBlockEvent(t *testing.T, status *BlockEventStatus) {
	t.Helper()
	require.Nil(t, status.getEvent())
}
