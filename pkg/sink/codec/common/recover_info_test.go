package common

import (
	"testing"

	commonPkg "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/sink/recoverable"
	"github.com/stretchr/testify/require"
)

func TestBuildMessageRecoverInfo(t *testing.T) {
	dispatcherID1 := commonPkg.NewDispatcherID()
	dispatcherID2 := commonPkg.NewDispatcherID()

	events := []*commonEvent.RowEvent{
		{DispatcherID: dispatcherID1, Epoch: 11},
		{DispatcherID: dispatcherID1, Epoch: 11},
		{DispatcherID: dispatcherID2, Epoch: 22},
	}

	info := recoverable.BuildRecoverInfo(events)
	require.NotNil(t, info)
	require.Equal(t, []recoverable.DispatcherEpoch{
		{DispatcherID: dispatcherID1, Epoch: 11},
		{DispatcherID: dispatcherID2, Epoch: 22},
	}, info.Dispatchers)
}

func TestBuildMessageRecoverInfo_UseMaxEpochForSameDispatcher(t *testing.T) {
	dispatcherID := commonPkg.NewDispatcherID()
	events := []*commonEvent.RowEvent{
		{DispatcherID: dispatcherID, Epoch: 11},
		{DispatcherID: dispatcherID, Epoch: 13},
		{DispatcherID: dispatcherID, Epoch: 12},
	}

	info := recoverable.BuildRecoverInfo(events)
	require.NotNil(t, info)
	require.Equal(t, []recoverable.DispatcherEpoch{
		{DispatcherID: dispatcherID, Epoch: 13},
	}, info.Dispatchers)
}

func TestAttachMessageRecoverInfo(t *testing.T) {
	dispatcherID := commonPkg.NewDispatcherID()
	events := []*commonEvent.RowEvent{
		{DispatcherID: dispatcherID, Epoch: 100},
	}

	message := NewMsg(nil, nil)
	message.SetRowsCount(1)

	err := AttachMessageRecoverInfo([]*Message{message}, events)
	require.NoError(t, err)
	require.NotNil(t, message.RecoverInfo)
	require.Equal(t, []recoverable.DispatcherEpoch{
		{DispatcherID: dispatcherID, Epoch: 100},
	}, message.RecoverInfo.Dispatchers)
}
