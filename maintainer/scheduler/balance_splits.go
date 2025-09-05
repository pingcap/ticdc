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
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/span"
	"github.com/pingcap/ticdc/maintainer/split"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/node"
	pkgScheduler "github.com/pingcap/ticdc/pkg/scheduler"
	pkgReplica "github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/pingcap/ticdc/server/watcher"
	"go.uber.org/zap"
)

// balanceSplitsScheduler is a scheduler that balances the spans of split tables.
// It manages the distribution and optimization of table spans across different nodes
// by performing operations such as splitting, merging, and moving spans based on
// traffic load, region count.
type balanceSplitsScheduler struct {
	changefeedID common.ChangeFeedID
	batchSize    int

	operatorController *operator.Controller
	spanController     *span.Controller
	nodeManager        *watcher.NodeManager

	splitter *split.Splitter
	mode     int64
}

func NewBalanceSplitsScheduler(
	changefeedID common.ChangeFeedID,
	batchSize int,
	splitter *split.Splitter,
	oc *operator.Controller,
	sc *span.Controller,
	mode int64,
) *balanceSplitsScheduler {
	return &balanceSplitsScheduler{
		changefeedID:       changefeedID,
		batchSize:          batchSize,
		splitter:           splitter,
		operatorController: oc,
		spanController:     sc,
		nodeManager:        appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName),
		mode:               mode,
	}
}

func (s *balanceSplitsScheduler) Name() string {
	if common.IsRedoMode(s.mode) {
		return pkgScheduler.RedoBalanceSplitScheduler
	}
	return pkgScheduler.BalanceSplitScheduler
}

func (s *balanceSplitsScheduler) Execute() time.Time {
	if s.operatorController.OperatorSize() > 0 || s.spanController.GetAbsentSize() > 0 {
		// not in stable schedule state, skip balance split
		return time.Now().Add(time.Second * 5)
	}
	availableSize := s.batchSize
	// We check the state of each group as following. Since each step has dependencies before and after,
	// at most one operation step can be performed in each group.
	// The main function please refer to check() in split_span_checker.go
	//
	// 1. First check if there are existing spans that need to be split (spans' region count exceeds the limit, span's traffic exceeds the limit)
	// if so, split first
	// 2. Check if the current spans meet the requirement to merge all back into one span
	// (total region count and traffic are within limits, and lag is low), if so, merge (merge is done by first move, then merge)
	// 3. Check if traffic is balanced between nodes, if there is obvious imbalance,
	// first try to migrate spans from the node with maximum traffic to the node with minimum traffic,
	// if not possible, split the span of the maximum traffic node and move it over
	// 4. Check if there are dispatchers that need to be merged, satisfying that they are adjacent in the same node, lag is low,
	// and the merged span's region count and traffic are within limits
	// 5. If none of the above, first calculate whether the current number of dispatchers is within acceptable range,
	// if not, check what dispatchers can be moved to facilitate merge operations.

	// we only process spans of one group that are all in replicating state (merge/split/move operations will enter scheduling state once created)
	for _, group := range s.spanController.GetGroups() {
		// we don't deal with the default group in this scheduler
		if group == pkgReplica.DefaultGroupID {
			continue
		}

		replications := s.spanController.GetReplicatingByGroup(group)
		if len(replications) != s.spanController.GetTaskSizeByGroup(group) {
			log.Info("here is some spans in the group is not in the replicating state; skip this balance check",
				zap.String("changefeed", s.changefeedID.String()),
				zap.Int64("group", int64(group)),
				zap.Int("replicatingSize", len(replications)),
				zap.Int("taskSize", s.spanController.GetTaskSizeByGroup(group)))
			continue
		}

		checkResults := s.spanController.CheckByGroup(group, availableSize)
		for _, checkResult := range checkResults.([]replica.SplitSpanCheckResult) {
			if checkResult.OpType == replica.OpSplit {
				splitSpans := s.splitter.Split(context.Background(), checkResult.SplitSpan.Span, checkResult.SpanNum, checkResult.SpanType)
				if len(splitSpans) > 1 {
					op := operator.NewSplitDispatcherOperator(s.spanController, checkResult.SplitSpan, splitSpans, checkResult.SplitTargetNodes, func(span *replica.SpanReplication, node node.ID) bool {
						return s.operatorController.AddOperator(operator.NewAddDispatcherOperator(s.spanController, span, node))
					})
					ret := s.operatorController.AddOperator(op)
					if ret {
						availableSize--
					}
				}
			} else if checkResult.OpType == replica.OpMove {
				for _, span := range checkResult.MoveSpans {
					op := operator.NewMoveDispatcherOperator(s.spanController, span, span.GetNodeID(), checkResult.TargetNode)
					ret := s.operatorController.AddOperator(op)
					if ret {
						availableSize--
					}
				}
			} else if checkResult.OpType == replica.OpMerge {
				if s.operatorController.AddMergeOperator(checkResult.MergeSpans) != nil {
					availableSize = availableSize - 1 - len(checkResult.MergeSpans)
				}
			}
		}

		if availableSize <= 0 {
			// too many schedule ops, wait for next tick
			return time.Now().Add(5 * time.Second)
		}
	}

	return time.Now().Add(5 * time.Second)
}
