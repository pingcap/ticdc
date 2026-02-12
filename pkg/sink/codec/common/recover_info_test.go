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
		nil,
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

func TestBuildMessageRecoverInfoUseMaxEpochForSameDispatcher(t *testing.T) {
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

	AttachMessageRecoverInfo([]*Message{message}, events)
	require.NotNil(t, message.RecoverInfo)
	require.Equal(t, []recoverable.DispatcherEpoch{
		{DispatcherID: dispatcherID, Epoch: 100},
	}, message.RecoverInfo.Dispatchers)
}

func TestAttachMessageRecoverInfoSplitByRowsCount(t *testing.T) {
	dispatcherID1 := commonPkg.NewDispatcherID()
	dispatcherID2 := commonPkg.NewDispatcherID()
	dispatcherID3 := commonPkg.NewDispatcherID()
	events := []*commonEvent.RowEvent{
		{DispatcherID: dispatcherID1, Epoch: 11},
		{DispatcherID: dispatcherID2, Epoch: 12},
		{DispatcherID: dispatcherID3, Epoch: 13},
	}

	firstMessage := NewMsg(nil, nil)
	firstMessage.SetRowsCount(2)
	secondMessage := NewMsg(nil, nil)
	secondMessage.SetRowsCount(1)

	AttachMessageRecoverInfo([]*Message{firstMessage, secondMessage}, events)
	require.Equal(t, []recoverable.DispatcherEpoch{
		{DispatcherID: dispatcherID1, Epoch: 11},
		{DispatcherID: dispatcherID2, Epoch: 12},
	}, firstMessage.RecoverInfo.Dispatchers)
	require.Equal(t, []recoverable.DispatcherEpoch{
		{DispatcherID: dispatcherID3, Epoch: 13},
	}, secondMessage.RecoverInfo.Dispatchers)
}

func TestAttachMessageRecoverInfoClampRowsToRemainingEvents(t *testing.T) {
	dispatcherID := commonPkg.NewDispatcherID()
	events := []*commonEvent.RowEvent{
		{DispatcherID: dispatcherID, Epoch: 9},
	}

	message := NewMsg(nil, nil)
	message.SetRowsCount(3)

	AttachMessageRecoverInfo([]*Message{message}, events)
	require.NotNil(t, message.RecoverInfo)
	require.Equal(t, []recoverable.DispatcherEpoch{
		{DispatcherID: dispatcherID, Epoch: 9},
	}, message.RecoverInfo.Dispatchers)
}

func TestAttachMessageRecoverInfoClearRecoverInfoWhenRowsCountIsZero(t *testing.T) {
	dispatcherID := commonPkg.NewDispatcherID()
	events := []*commonEvent.RowEvent{
		{DispatcherID: dispatcherID, Epoch: 9},
	}

	message := NewMsg(nil, nil)
	message.SetRowsCount(0)
	message.RecoverInfo = &recoverable.RecoverInfo{
		Dispatchers: []recoverable.DispatcherEpoch{
			{DispatcherID: dispatcherID, Epoch: 1},
		},
	}

	AttachMessageRecoverInfo([]*Message{message}, events)
	require.Nil(t, message.RecoverInfo)
}
