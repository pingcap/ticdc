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
	"math/rand"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/span"
	"github.com/pingcap/ticdc/maintainer/split"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/node"
	pkgScheduler "github.com/pingcap/ticdc/pkg/scheduler"
	pkgReplica "github.com/pingcap/ticdc/pkg/scheduler/replica"
	pkgreplica "github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils"
	"go.uber.org/zap"
)

// balanceScheduler is used to check the balance status of all spans among all nodes
type balanceScheduler struct {
	changefeedID common.ChangeFeedID
	batchSize    int

	operatorController *operator.Controller
	spanController     *span.Controller
	nodeManager        *watcher.NodeManager

	random               *rand.Rand
	lastRebalanceTime    time.Time
	checkBalanceInterval time.Duration
	// forceBalance forces the scheduler to produce schedule tasks regardless of
	// `checkBalanceInterval`.
	// It is set to true when the last time `Schedule` produces some tasks,
	// and it is likely there are more tasks will be produced in the next
	// `Schedule`.
	// It speeds up rebalance.
	forceBalance bool
	maxCheckTime time.Duration
}

func NewBalanceScheduler(
	changefeedID common.ChangeFeedID, batchSize int,
	oc *operator.Controller, sc *span.Controller,
	balanceInterval time.Duration,
) *balanceScheduler {
	return &balanceScheduler{
		changefeedID:         changefeedID,
		batchSize:            batchSize,
		random:               rand.New(rand.NewSource(time.Now().UnixNano())),
		operatorController:   oc,
		spanController:       sc,
		nodeManager:          appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName),
		checkBalanceInterval: balanceInterval,
		lastRebalanceTime:    time.Now(),
		maxCheckTime:         time.Second * 500,
	}
}

func (s *balanceScheduler) Execute() time.Time {
	if !s.forceBalance && time.Since(s.lastRebalanceTime) < s.checkBalanceInterval {
		return s.lastRebalanceTime.Add(s.checkBalanceInterval)
	}
	now := time.Now()

	failpoint.Inject("StopBalanceScheduler", func() {
		failpoint.Return(now.Add(s.checkBalanceInterval))
	})

	// TODO: consider to ignore split tables' dispatcher basic schedule operator to decide whether we can make balance schedule
	if s.operatorController.OperatorSize() > 0 || s.spanController.GetAbsentSize() > 0 {
		// not in stable schedule state, skip balance
		return now.Add(s.checkBalanceInterval)
	}

	// 1. check whether we have spans in defaultGroupID need to be splitted.
	checked, batch, start := 0, s.batchSize, time.Now()
	needBreak := false
	for _, group := range s.spanController.GetGroups() {
		if group == pkgreplica.DefaultGroupID {
			continue
		}
		checkResults := s.spanController.CheckByGroup(group, s.batchSize)
		checked, needBreak = s.doCheck(checkResults, start)
		batch -= checked
		s.lastCheckTime = time.Now()
	}

	// 2. do balance for the spans in defaultGroupID
	nodes := s.nodeManager.GetAliveNodes()
	moved := s.schedulerDefaultGroup(nodes)
	// if moved == 0 {
	// 	// all groups are balanced, safe to do the global balance
	// 	moved = s.schedulerGlobal(nodes)
	// }

	s.forceBalance = moved >= s.batchSize
	s.lastRebalanceTime = time.Now()

	return now.Add(s.checkBalanceInterval)
}

func (s *balanceScheduler) Name() string {
	return "balance-scheduler"
}

func (s *balanceScheduler) schedulerDefaultGroup(nodes map[node.ID]*node.Info) int {
	group := pkgreplica.DefaultGroupID
	// fast path, check the balance status
	moveSize := pkgScheduler.CheckBalanceStatus(s.spanController.GetTaskSizePerNodeByGroup(group), nodes)
	if moveSize <= 0 {
		return 0
	}
	replicas := s.spanController.GetReplicatingByGroup(group)
	return pkgScheduler.Balance(s.batchSize, s.random, nodes, replicas, s.doMove)
}

func (s *balanceScheduler) doCheck(ret pkgReplica.GroupCheckResult, start time.Time) (int, bool) {
	if ret == nil {
		return 0, false
	}
	checkResults := ret.([]replica.CheckResult)

	checkedIndex := 0
	for ; checkedIndex < len(checkResults); checkedIndex++ {
		if time.Since(start) > s.maxCheckTime {
			return checkedIndex, true
		}
		ret := checkResults[checkedIndex]
		totalSpan, valid := s.valid(ret)
		if !valid {
			continue
		}

		switch ret.OpType {
		case replica.OpMerge:
			log.Info("Into OP Merge")
			s.opController.AddMergeSplitOperator(ret.Replications, []*heartbeatpb.TableSpan{totalSpan})
		case replica.OpSplit:
			log.Info("Into OP Split")
			fallthrough
		case replica.OpMergeAndSplit:
			log.Info("Into OP MergeAndSplit")
			// expectedSpanNum := split.NextExpectedSpansNumber(len(ret.Replications))
			spans := s.splitter.SplitSpansByWriteKey(context.Background(), totalSpan, len(s.nodeManager.GetAliveNodes()))
			if len(spans) > 1 {
				log.Info("split span",
					zap.String("changefeed", s.changefeedID.Name()),
					zap.String("span", totalSpan.String()),
					zap.Int("spanSize", len(spans)))
				s.opController.AddMergeSplitOperator(ret.Replications, spans)
			}
		}
	}
	return checkedIndex, false
}

func (s *balanceScheduler) valid(c replica.CheckResult) (*heartbeatpb.TableSpan, bool) {
	if c.OpType == replica.OpSplit && len(c.Replications) != 1 {
		log.Panic("split operation should have only one replication",
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Int64("tableId", c.Replications[0].Span.TableID),
			zap.Stringer("checkResult", c))
	}
	span := common.TableIDToComparableSpan(c.Replications[0].Span.TableID)
	totalSpan := &heartbeatpb.TableSpan{
		TableID:  span.TableID,
		StartKey: span.StartKey,
		EndKey:   span.EndKey,
	}

	if c.OpType == replica.OpMerge || c.OpType == replica.OpMergeAndSplit {
		if len(c.Replications) <= 1 {
			log.Panic("invalid replication size",
				zap.String("changefeed", s.changefeedID.Name()),
				zap.Int64("tableId", c.Replications[0].Span.TableID),
				zap.Stringer("checkResult", c))
		}
		spanMap := utils.NewBtreeMap[*heartbeatpb.TableSpan, *replica.SpanReplication](common.LessTableSpan)
		for _, r := range c.Replications {
			spanMap.ReplaceOrInsert(r.Span, r)
		}
		holes := split.FindHoles(spanMap, totalSpan)
		log.Warn("skip merge operation since there are holes",
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Int64("tableId", c.Replications[0].Span.TableID),
			zap.Int("holes", len(holes)), zap.Stringer("checkResult", c))
		return totalSpan, len(holes) == 0
	}

	if c.OpType == replica.OpMergeAndSplit && len(c.Replications) >= split.DefaultMaxSpanNumber {
		log.Debug("skip split operation since the replication number is too large",
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Int64("tableId", c.Replications[0].Span.TableID), zap.Stringer("checkResult", c))
		return totalSpan, false
	}
	return totalSpan, true
}

/*
// TODO: refactor and simplify the implementation and limit max group size
func (s *balanceScheduler) schedulerGlobal(nodes map[node.ID]*node.Info) int {
	// fast path, check the balance status
	moveSize := pkgScheduler.CheckBalanceStatus(s.spanController.GetTaskSizePerNode(), nodes)
	if moveSize <= 0 {
		// no need to do the balance, skip
		return 0
	}
	groupNodetasks, valid := s.spanController.GetImbalanceGroupNodeTask(nodes)
	if !valid {
		// no need to do the balance, skip
		return 0
	}

	// complexity note: len(nodes) * len(groups)
	totalTasks := 0
	sizePerNode := make(map[node.ID]int, len(nodes))
	for _, nodeTasks := range groupNodetasks {
		for id, task := range nodeTasks {
			if task != nil {
				totalTasks++
				sizePerNode[id]++
			}
		}
	}
	lowerLimitPerNode := int(math.Floor(float64(totalTasks) / float64(len(nodes))))
	limitCnt := 0
	for _, size := range sizePerNode {
		if size == lowerLimitPerNode {
			limitCnt++
		}
	}
	if limitCnt == len(nodes) {
		// all nodes are global balanced
		return 0
	}

	moved := 0
	for _, nodeTasks := range groupNodetasks {
		availableNodes, victims, next := []node.ID{}, []node.ID{}, 0
		for id, task := range nodeTasks {
			if task != nil && sizePerNode[id] > lowerLimitPerNode {
				victims = append(victims, id)
			} else if task == nil && sizePerNode[id] < lowerLimitPerNode {
				availableNodes = append(availableNodes, id)
			}
		}

		for _, new := range availableNodes {
			if next >= len(victims) {
				break
			}
			old := victims[next]
			if s.doMove(nodeTasks[old], new) {
				sizePerNode[old]--
				sizePerNode[new]++
				next++
				moved++
			}
		}
	}
	log.Info("finish global balance", zap.Stringer("changefeed", s.changefeedID), zap.Int("moved", moved))
	return moved
}
*/

func (s *balanceScheduler) doMove(replication *replica.SpanReplication, id node.ID) bool {
	op := operator.NewMoveDispatcherOperator(s.spanController, replication, replication.GetNodeID(), id)
	return s.operatorController.AddOperator(op)
}
