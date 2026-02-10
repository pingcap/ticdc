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

	info := buildMessageRecoverInfo(events)
	require.NotNil(t, info)
	require.Equal(t, []recoverable.DispatcherEpoch{
		{DispatcherID: dispatcherID1, Epoch: 11},
		{DispatcherID: dispatcherID2, Epoch: 22},
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

func TestAttachMessageRecoverInfo_WithZeroDispatcherID(t *testing.T) {
	events := []*commonEvent.RowEvent{
		{DispatcherID: commonPkg.DispatcherID{}, Epoch: 1},
	}

	message := NewMsg(nil, nil)
	message.SetRowsCount(1)

	err := AttachMessageRecoverInfo([]*Message{message}, events)
	require.NoError(t, err)
	require.Nil(t, message.RecoverInfo)
}
