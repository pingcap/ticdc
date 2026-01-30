package maintainer

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	maintainerTestutil "github.com/pingcap/ticdc/maintainer/testutil"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/eventservice"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

type noopMessageCenter struct{}

func (noopMessageCenter) SendEvent(*messaging.TargetMessage) error         { return nil }
func (noopMessageCenter) SendCommand(*messaging.TargetMessage) error       { return nil }
func (noopMessageCenter) IsReadyToSend(node.ID) bool                       { return true }
func (noopMessageCenter) RegisterHandler(string, messaging.MessageHandler) {}
func (noopMessageCenter) DeRegisterHandler(string)                         {}
func (noopMessageCenter) OnNodeChanges(map[node.ID]*node.Info)             {}
func (noopMessageCenter) Close()                                           {}

func TestMaintainer_IgnoreNonCascadeRemoveBeforeBootstrap(t *testing.T) {
	m := &Maintainer{
		changefeedID: common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName),
	}

	m.onRemoveMaintainer(false, false)
	require.False(t, m.removing.Load())
	require.False(t, m.cascadeRemoving.Load())
	require.False(t, m.changefeedRemoved.Load())

	require.NotPanics(t, func() {
		// Duplicate remove requests should be safe and still ignored.
		m.onRemoveMaintainer(false, false)
	})
}

func TestMaintainer_CascadeRemoveNotIgnoredBeforeBootstrap(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)

	nodeManager := watcher.NewNodeManager(nil, nil)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}

	m := &Maintainer{
		changefeedID:  cfID,
		nodeManager:   nodeManager,
		closedNodes:   map[node.ID]struct{}{"node1": {}},
		statusChanged: atomic.NewBool(false),
		removed:       atomic.NewBool(false),
	}
	m.scheduleState.Store(int32(heartbeatpb.ComponentState_Working))
	m.watermark.Watermark = &heartbeatpb.Watermark{CheckpointTs: 1, ResolvedTs: 1}

	require.NotPanics(t, func() {
		m.onRemoveMaintainer(true, false)
	})
	require.True(t, m.removing.Load())
	require.True(t, m.cascadeRemoving.Load())
}

func TestMaintainer_IgnoreNonCascadeRemoveDoesNotBlockBootstrap(t *testing.T) {
	mc := noopMessageCenter{}
	appcontext.SetService(appcontext.MessageCenter, messaging.MessageCenter(mc))
	appcontext.SetService(appcontext.RegionCache, maintainerTestutil.NewMockRegionCache())

	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID),
		&heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1", false)

	refresher := replica.NewRegionCountRefresher(cfID, time.Minute)
	controller := NewController(cfID, 1, &mockThreadPool{},
		config.GetDefaultReplicaConfig(), ddlSpan, nil, 1000, 0, refresher, common.DefaultKeyspace, false)

	schemaStore := eventservice.NewMockSchemaStore()
	schemaStore.SetTables([]commonEvent.Table{
		{
			TableID:         1,
			SchemaID:        1,
			SchemaTableName: &commonEvent.SchemaTableName{SchemaName: "test", TableName: "t"},
		},
	})
	appcontext.SetService(appcontext.SchemaStore, schemaStore)

	m := &Maintainer{
		changefeedID: cfID,
		info: &config.ChangeFeedInfo{
			ChangefeedID: cfID,
			SinkURI:      "blackhole://",
			Config:       config.GetDefaultReplicaConfig(),
			KeyspaceID:   common.DefaultKeyspaceID,
		},
		controller:    controller,
		mc:            mc,
		selfNode:      &node.Info{ID: "node1"},
		statusChanged: atomic.NewBool(false),
	}
	m.watermark.Watermark = &heartbeatpb.Watermark{CheckpointTs: 1, ResolvedTs: 1}

	// Simulate receiving a non-cascade remove request before bootstrap finishes.
	m.onRemoveMaintainer(false, false)
	require.False(t, m.removing.Load())

	totalSpan := common.TableIDToComparableSpan(common.DefaultKeyspaceID, 1)
	span := &heartbeatpb.TableSpan{TableID: int64(1), StartKey: totalSpan.StartKey, EndKey: totalSpan.EndKey}
	dispatcherID := common.NewDispatcherID()

	m.onBootstrapResponses(map[node.ID]*heartbeatpb.MaintainerBootstrapResponse{
		"node1": {
			ChangefeedID: cfID.ToPB(),
			Spans: []*heartbeatpb.BootstrapTableSpan{
				{
					ID:              dispatcherID.ToPB(),
					SchemaID:        1,
					Span:            span,
					ComponentStatus: heartbeatpb.ComponentState_Working,
					CheckpointTs:    10,
				},
			},
			CheckpointTs: 10,
		},
	})

	require.True(t, m.initialized.Load())
}
