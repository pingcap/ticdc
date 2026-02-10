package scheduler

import (
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/coordinator/nodeliveness"
	"github.com/pingcap/ticdc/coordinator/operator"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"go.uber.org/zap"
)

const drainCheckInterval = 200 * time.Millisecond

// drainScheduler moves maintainers out of nodes in DRAINING state.
type drainScheduler struct {
	id        string
	batchSize int

	operatorController *operator.Controller
	changefeedDB       *changefeed.ChangefeedDB
	nodeManager        *watcher.NodeManager
	livenessView       *nodeliveness.View

	rrIndex int
}

func NewDrainScheduler(
	id string,
	batchSize int,
	oc *operator.Controller,
	changefeedDB *changefeed.ChangefeedDB,
	livenessView *nodeliveness.View,
) *drainScheduler {
	return &drainScheduler{
		id:                 id,
		batchSize:          batchSize,
		operatorController: oc,
		changefeedDB:       changefeedDB,
		nodeManager:        appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName),
		livenessView:       livenessView,
	}
}

func (s *drainScheduler) Name() string {
	return "drain-scheduler"
}

func (s *drainScheduler) Execute() time.Time {
	if s.livenessView == nil {
		return time.Now().Add(drainCheckInterval)
	}

	now := time.Now()
	drainingNodes := s.livenessView.GetNodesByState(nodeliveness.StateDraining, now)
	if len(drainingNodes) == 0 {
		return now.Add(time.Second)
	}

	destNodes := s.livenessView.FilterSchedulableDestNodes(s.nodeManager.GetAliveNodes(), now)
	if len(destNodes) == 0 {
		return now.Add(time.Second)
	}

	nodeTaskSize := s.changefeedDB.GetTaskSizePerNode()

	scheduled := 0
	for i := 0; i < len(drainingNodes) && scheduled < s.batchSize; i++ {
		origin := drainingNodes[(s.rrIndex+i)%len(drainingNodes)]
		changefeeds := s.changefeedDB.GetByNodeID(origin)
		if len(changefeeds) == 0 {
			continue
		}

		for _, cf := range changefeeds {
			if scheduled >= s.batchSize {
				break
			}
			if s.operatorController.HasOperatorByID(cf.ID) {
				continue
			}

			dest := pickLeastLoadedNode(destNodes, nodeTaskSize)
			if dest == "" {
				log.Info("no schedulable destination node for drain",
					zap.Stringer("origin", origin))
				return now.Add(time.Second)
			}

			if !s.operatorController.AddOperator(operator.NewMoveMaintainerOperator(s.changefeedDB, cf, origin, dest)) {
				continue
			}
			nodeTaskSize[dest]++
			scheduled++
		}
	}

	s.rrIndex++
	if len(drainingNodes) > 0 {
		s.rrIndex %= len(drainingNodes)
	}
	return now.Add(drainCheckInterval)
}

func pickLeastLoadedNode(nodes map[node.ID]*node.Info, nodeTaskSize map[node.ID]int) node.ID {
	var (
		minNode node.ID
		minLoad int
	)
	for id := range nodes {
		load, ok := nodeTaskSize[id]
		if !ok {
			load = 0
		}
		if minNode == "" || load < minLoad {
			minNode = id
			minLoad = load
		}
	}
	return minNode
}
