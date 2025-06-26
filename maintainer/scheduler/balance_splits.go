// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package scheduler

import (
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/span"
	"github.com/pingcap/ticdc/maintainer/split"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	pkgReplica "github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/pingcap/ticdc/server/watcher"
	"go.uber.org/zap"
)

// balanceSplitsScheduler is a scheduler that balances the spans of the splitted tables.
type balanceSplitsScheduler struct {
	changefeedID common.ChangeFeedID
	batchSize    int

	operatorController *operator.Controller
	spanController     *span.Controller
	nodeManager        *watcher.NodeManager

	splitter *split.Splitter
}

func NewBalanceSplitsScheduler(
	changefeedID common.ChangeFeedID,
	batchSize int,
	splitter *split.Splitter,
	oc *operator.Controller,
	sc *span.Controller,
) *balanceSplitsScheduler {
	return &balanceSplitsScheduler{
		changefeedID:       changefeedID,
		batchSize:          batchSize,
		splitter:           splitter,
		operatorController: oc,
		spanController:     sc,
		nodeManager:        appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName),
	}
}

func (s *balanceSplitsScheduler) Execute() time.Time {
	// 因为每一步前后都有依赖，所以每个 group 中的操作步骤最多做一种
	// 1. 先检查是否有现有的 spans 需要被拆分（spans 的 region 超过了上限，span 的流量超过了上限），如有则先拆分
	// 2. 检查当前的 spans 是否具备有全部合并回一个 span 的需求。（全部加起来 region 和流量都不超标，并且 lag 低），如有则合并（这里的合并通过先 move，再 merge 的方式）
	// 3. 查看各个节点之间的流量是否均衡，如果存在明显的不均衡，先尝试能不能从最大流量的节点往最小节点的流量迁移 span, 如果不行就拆最大流量节点的 span，然后移过去
	// 4. 查看是否有需要 merge 的 dispatchers，满足在同一个节点相邻，并且 lag 低，并且 合并以后 span 的 region 和 流量都不超标
	// 5. 如果都没有，先计算目前的 dispatchers 个数是否在可接受范围内，如果不在，就查看可以移动什么 dispatchers 来促成 merge 操作。

	// 也就是我需要知道的

	// 理论上 对拆表 span 的平衡，我们就是只处理一个 group 的 spans 都在 replicating 状态(merge/split/move op 一创建就会进入 scheduling 状态)
	for _, group := range s.spanController.GetGroups() {
		// we don't deal with the default group in this scheduler
		if group == pkgReplica.DefaultGroupID {
			continue
		}

		replications := s.spanController.GetReplicatingByGroup(group)
		if len(replications) != s.spanController.GetTaskSizeByGroup(group) {
			log.Debug("here is some spans in the group is not in the replicating state; skip this balance check",
				zap.String("changefeed", s.changefeedID.String()),
				zap.Int64("group", int64(group)),
				zap.Int("replicatingSize", len(replications)),
				zap.Int("taskSize", s.spanController.GetTaskSizeByGroup(group)))
			continue
		}

	}
}
