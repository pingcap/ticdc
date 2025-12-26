package maintainer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/pkg/bootstrap"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils/threadpool"
	"github.com/stretchr/testify/require"
)

type recordingMessageCenter struct {
	mu   sync.Mutex
	cmds []*messaging.TargetMessage
}

func (m *recordingMessageCenter) SendEvent(_ *messaging.TargetMessage) error { return nil }
func (m *recordingMessageCenter) SendCommand(cmd *messaging.TargetMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cmds = append(m.cmds, cmd)
	return nil
}
func (m *recordingMessageCenter) IsReadyToSend(_ node.ID) bool { return true }
func (m *recordingMessageCenter) RegisterHandler(_ string, _ messaging.MessageHandler) {
}
func (m *recordingMessageCenter) DeRegisterHandler(_ string) {}
func (m *recordingMessageCenter) OnNodeChanges(_ map[node.ID]*node.Info) {
}
func (m *recordingMessageCenter) Close() {}

func (m *recordingMessageCenter) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cmds = nil
}

func (m *recordingMessageCenter) commands() []*messaging.TargetMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*messaging.TargetMessage, len(m.cmds))
	copy(out, m.cmds)
	return out
}

func TestCalculateNewCheckpointTsGatedByChecksumState(t *testing.T) {
	appcontext.SetService(appcontext.MessageCenter, &recordingMessageCenter{})
	appcontext.SetService(watcher.NodeManagerName, watcher.NewNodeManager(nil, nil))
	appcontext.SetService(appcontext.DefaultPDClock, pdutil.NewClock4Test())

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)
	self := node.NewInfo("127.0.0.1:0", "127.0.0.1:0")
	other := node.NewInfo("127.0.0.1:1", "127.0.0.1:1")

	_, ddlSpan := newDDLSpan(common.DefaultKeyspaceID, cfID, 100, self, common.DefaultMode)
	refresher := replica.NewRegionCountRefresher(cfID, time.Second)
	controller := NewController(
		cfID,
		100,
		threadpool.NewThreadPoolDefault(),
		config.GetDefaultReplicaConfig(),
		ddlSpan,
		nil,
		1,
		time.Second,
		refresher,
		common.KeyspaceMeta{ID: common.DefaultKeyspaceID, Name: cfID.Keyspace()},
		false,
		nil,
		nil,
	)
	controller.barrier = NewBarrier(controller.spanController, controller.operatorController, false, nil, common.DefaultMode)

	m := &Maintainer{
		changefeedID:           cfID,
		selfNode:               self,
		controller:             controller,
		bootstrapper:           bootstrap.NewBootstrapper[heartbeatpb.MaintainerBootstrapResponse](cfID.Name(), func(node.ID) *messaging.TargetMessage { return nil }),
		checkpointTsByCapture:  newWatermarkCaptureMap(),
		checksumStateByCapture: newChecksumStateCaptureMap(),
	}
	m.watermark.Watermark = &heartbeatpb.Watermark{CheckpointTs: 100, ResolvedTs: 100}

	_ = m.bootstrapper.HandleNewNodes([]*node.Info{self, other})

	spanID := common.NewDispatcherID()
	status := &heartbeatpb.TableSpanStatus{
		ID:              spanID.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Working,
		CheckpointTs:    100,
		Mode:            common.DefaultMode,
	}
	rep := replica.NewWorkingSpanReplication(
		cfID,
		spanID,
		1,
		&heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("a"), EndKey: []byte("b"), KeyspaceID: common.DefaultKeyspaceID},
		status,
		other.ID,
		false,
	)
	controller.spanController.AddReplicatingSpan(rep)

	m.checkpointTsByCapture.Set(self.ID, heartbeatpb.Watermark{CheckpointTs: 200, ResolvedTs: 200, Seq: 1})
	m.checkpointTsByCapture.Set(other.ID, heartbeatpb.Watermark{CheckpointTs: 200, ResolvedTs: 200, Seq: 1})
	m.checksumStateByCapture.Set(self.ID, heartbeatpb.ChecksumState_OK)
	m.checksumStateByCapture.Set(other.ID, heartbeatpb.ChecksumState_UNINITIALIZED)

	_, ok := m.calculateNewCheckpointTs()
	require.False(t, ok)
}

func TestAdvanceRedoMetaTsOnceGatedByChecksumState(t *testing.T) {
	mc := &recordingMessageCenter{}
	appcontext.SetService(appcontext.MessageCenter, mc)
	appcontext.SetService(watcher.NodeManagerName, watcher.NewNodeManager(nil, nil))
	appcontext.SetService(appcontext.DefaultPDClock, pdutil.NewClock4Test())

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)
	self := node.NewInfo("127.0.0.1:0", "127.0.0.1:0")
	other := node.NewInfo("127.0.0.1:1", "127.0.0.1:1")

	_, ddlSpan := newDDLSpan(common.DefaultKeyspaceID, cfID, 100, self, common.DefaultMode)
	_, redoDDLSpan := newDDLSpan(common.DefaultKeyspaceID, cfID, 100, self, common.RedoMode)
	refresher := replica.NewRegionCountRefresher(cfID, time.Second)

	cfg := config.GetDefaultReplicaConfig()
	level := string(redo.ConsistentLevelEventual)
	cfg.Consistent.Level = &level
	controller := NewController(
		cfID,
		100,
		threadpool.NewThreadPoolDefault(),
		cfg,
		ddlSpan,
		redoDDLSpan,
		1,
		time.Second,
		refresher,
		common.KeyspaceMeta{ID: common.DefaultKeyspaceID, Name: cfID.Keyspace()},
		true,
		nil,
		nil,
	)
	controller.redoBarrier = NewBarrier(controller.redoSpanController, controller.redoOperatorController, false, nil, common.RedoMode)

	m := &Maintainer{
		changefeedID:               cfID,
		selfNode:                   self,
		controller:                 controller,
		mc:                         mc,
		bootstrapper:               bootstrap.NewBootstrapper[heartbeatpb.MaintainerBootstrapResponse](cfID.Name(), func(node.ID) *messaging.TargetMessage { return nil }),
		redoMetaTs:                 &heartbeatpb.RedoMetaMessage{ChangefeedID: cfID.ToPB(), CheckpointTs: 100, ResolvedTs: 100},
		redoTsByCapture:            newWatermarkCaptureMap(),
		redoChecksumStateByCapture: newChecksumStateCaptureMap(),
	}
	m.watermark.Watermark = &heartbeatpb.Watermark{CheckpointTs: 150, ResolvedTs: 150}
	m.bootstrapped.Store(true)

	_ = m.bootstrapper.HandleNewNodes([]*node.Info{self, other})

	spanID := common.NewDispatcherID()
	status := &heartbeatpb.TableSpanStatus{
		ID:              spanID.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Working,
		CheckpointTs:    100,
		Mode:            common.RedoMode,
	}
	rep := replica.NewWorkingSpanReplication(
		cfID,
		spanID,
		1,
		&heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("a"), EndKey: []byte("b"), KeyspaceID: common.DefaultKeyspaceID},
		status,
		other.ID,
		false,
	)
	controller.redoSpanController.AddReplicatingSpan(rep)

	m.redoTsByCapture.Set(self.ID, heartbeatpb.Watermark{CheckpointTs: 200, ResolvedTs: 200, Seq: 1})
	m.redoTsByCapture.Set(other.ID, heartbeatpb.Watermark{CheckpointTs: 200, ResolvedTs: 200, Seq: 1})

	m.redoChecksumStateByCapture.Set(self.ID, heartbeatpb.ChecksumState_OK)
	m.redoChecksumStateByCapture.Set(other.ID, heartbeatpb.ChecksumState_UNINITIALIZED)

	mc.reset()
	m.advanceRedoMetaTsOnce()

	var hasRedoMeta bool
	for _, msg := range mc.commands() {
		if msg.Topic != messaging.HeartbeatCollectorTopic || len(msg.Message) == 0 {
			continue
		}
		_, ok := msg.Message[0].(*heartbeatpb.RedoMetaMessage)
		if ok {
			hasRedoMeta = true
		}
	}
	require.False(t, hasRedoMeta)
}

func TestDispatcherSetChecksumResendAndAck(t *testing.T) {
	mgr := newCaptureSetChecksumManager(
		common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme),
		1,
		common.DefaultMode,
	)

	capture := node.ID("capture-1")
	id1 := common.NewDispatcherID()
	id2 := common.NewDispatcherID()

	mgr.ApplyDelta(capture, []common.DispatcherID{id1}, nil)
	mgr.ApplyDelta(capture, []common.DispatcherID{id2}, nil)

	msgs := mgr.FlushDirty()
	require.Len(t, msgs, 1)
	update := msgs[0].Message[0].(*heartbeatpb.DispatcherSetChecksumUpdate)
	require.Equal(t, uint64(2), update.Seq)

	mgr.mu.Lock()
	mgr.state.captures[capture].lastSendAt = time.Time{}
	mgr.mu.Unlock()

	msgs = mgr.ResendPending()
	require.Len(t, msgs, 1)
	update = msgs[0].Message[0].(*heartbeatpb.DispatcherSetChecksumUpdate)
	require.Equal(t, uint64(2), update.Seq)

	mgr.HandleAck(capture, &heartbeatpb.DispatcherSetChecksumAck{
		ChangefeedID: mgr.changefeedID.ToPB(),
		Epoch:        1,
		Mode:         common.DefaultMode,
		Seq:          1,
	})
	mgr.mu.Lock()
	mgr.state.captures[capture].lastSendAt = time.Time{}
	mgr.mu.Unlock()
	msgs = mgr.ResendPending()
	require.Len(t, msgs, 1)

	mgr.HandleAck(capture, &heartbeatpb.DispatcherSetChecksumAck{
		ChangefeedID: mgr.changefeedID.ToPB(),
		Epoch:        1,
		Mode:         common.DefaultMode,
		Seq:          2,
	})
	msgs = mgr.ResendPending()
	require.Empty(t, msgs)
}

func TestRecordingMessageCenterImplementsInterface(t *testing.T) {
	var _ messaging.MessageCenter = (*recordingMessageCenter)(nil)
	var _ messaging.MessageHandler = func(context.Context, *messaging.TargetMessage) error { return nil }
}
