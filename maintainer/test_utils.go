package maintainer

import (
	"context"

	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/server/watcher"
)

// setNodeManagerAndMessageCenter sets up the node manager and message center for testing
func setNodeManagerAndMessageCenter() *watcher.NodeManager {
	n := node.NewInfo("", "")
	mockPDClock := pdutil.NewClock4Test()
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)
	mc := messaging.NewMessageCenter(context.Background(), n.ID, config.NewDefaultMessageCenterConfig(n.AdvertiseAddr), nil)
	mc.Run(context.Background())
	defer mc.Close()
	appcontext.SetService(appcontext.MessageCenter, mc)

	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)
	return nodeManager
}
