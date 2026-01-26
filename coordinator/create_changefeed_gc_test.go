package coordinator

import (
	"context"
	goerrors "errors"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	mock_changefeed "github.com/pingcap/ticdc/coordinator/changefeed/mock"
	"github.com/pingcap/ticdc/coordinator/operator"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/txnutil/gc"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

type recordingGCManager struct {
	mu sync.Mutex

	called                 bool
	checkpointTsUpperBound common.Ts
	forceUpdate            bool

	keyspaceCalled bool
	keyspaceID     uint32
	keyspaceName   string

	retErr error
}

var _ gc.Manager = (*recordingGCManager)(nil)

func (m *recordingGCManager) TryUpdateServiceGCSafepoint(
	_ context.Context, checkpointTs common.Ts, forceUpdate bool,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.called = true
	m.checkpointTsUpperBound = checkpointTs
	m.forceUpdate = forceUpdate
	return m.retErr
}

func (m *recordingGCManager) CheckStaleCheckpointTs(
	_ context.Context, _ uint32, _ common.ChangeFeedID, _ common.Ts,
) error {
	return nil
}

func (m *recordingGCManager) TryUpdateKeyspaceGCBarrier(
	_ context.Context, keyspaceID uint32, keyspaceName string, checkpointTs common.Ts, forceUpdate bool,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.keyspaceCalled = true
	m.keyspaceID = keyspaceID
	m.keyspaceName = keyspaceName
	m.checkpointTsUpperBound = checkpointTs
	m.forceUpdate = forceUpdate
	return m.retErr
}

func TestCreateChangefeedForceUpdateGCSafepointBestEffort(t *testing.T) {
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)

	mc := messaging.NewMockMessageCenter()
	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(appcontext.MessageCenter, mc)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	changefeedDB := changefeed.NewChangefeedDB(1)
	controller := &Controller{
		backend:      backend,
		changefeedDB: changefeedDB,
		operatorController: operator.NewOperatorController(
			node.NewInfo("node1", ""),
			changefeedDB,
			backend,
			10,
		),
		initialized: atomic.NewBool(true),
		nodeManager: nodeManager,
	}

	gcManager := &recordingGCManager{retErr: goerrors.New("update gc safepoint failed")}
	co := &coordinator{
		controller: controller,
		gcManager:  gcManager,
		pdClock:    pdutil.NewClock4Test(),
	}

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	info := &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		StartTs:      10,
		State:        config.StateNormal,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "kafka://127.0.0.1:9092",
		KeyspaceID:   1,
	}
	backend.EXPECT().CreateChangefeed(gomock.Any(), gomock.Any()).Return(nil).Times(1)

	require.NoError(t, co.CreateChangefeed(context.Background(), info))
	require.Equal(t, 1, changefeedDB.GetAbsentSize())

	if kerneltype.IsNextGen() {
		require.True(t, gcManager.keyspaceCalled)
		require.Equal(t, info.KeyspaceID, gcManager.keyspaceID)
		require.Equal(t, info.ChangefeedID.Keyspace(), gcManager.keyspaceName)
	} else {
		require.True(t, gcManager.called)
	}
	require.True(t, gcManager.forceUpdate)
	require.Equal(t, common.Ts(info.StartTs-1), gcManager.checkpointTsUpperBound)
}

func TestCreateChangefeedSkipUpdateGCSafepointOnCreateFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)

	mc := messaging.NewMockMessageCenter()
	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(appcontext.MessageCenter, mc)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	changefeedDB := changefeed.NewChangefeedDB(1)
	controller := &Controller{
		backend:      backend,
		changefeedDB: changefeedDB,
		operatorController: operator.NewOperatorController(
			node.NewInfo("node1", ""),
			changefeedDB,
			backend,
			10,
		),
		initialized: atomic.NewBool(true),
		nodeManager: nodeManager,
	}

	gcManager := &recordingGCManager{retErr: goerrors.New("unexpected call")}
	co := &coordinator{
		controller: controller,
		gcManager:  gcManager,
		pdClock:    pdutil.NewClock4Test(),
	}

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	info := &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		StartTs:      10,
		State:        config.StateNormal,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "kafka://127.0.0.1:9092",
		KeyspaceID:   1,
	}
	backend.EXPECT().CreateChangefeed(gomock.Any(), gomock.Any()).Return(goerrors.New("create failed")).Times(1)

	require.Error(t, co.CreateChangefeed(context.Background(), info))
	require.Equal(t, 0, changefeedDB.GetAbsentSize())
	require.False(t, gcManager.called)
	require.False(t, gcManager.keyspaceCalled)
}
