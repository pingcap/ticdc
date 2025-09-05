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
)

// balanceScheduler mainly focus on two types of operations:
// 1. Split operations: splits the one large span for the whole table into smaller ones.
// 2. Move operations: distributes spans across nodes for balance the span count in each node.
type balanceScheduler struct {
	changefeedID common.ChangeFeedID
	batchSize    int

	operatorController *operator.Controller
	spanController     *span.Controller
	nodeManager        *watcher.NodeManager

	splitter *split.Splitter

	random *rand.Rand
	mode   int64
}

func NewBalanceScheduler(
	changefeedID common.ChangeFeedID,
	batchSize int,
	splitter *split.Splitter,
	oc *operator.Controller,
	sc *span.Controller,
	_ time.Duration,
	mode int64,
) *balanceScheduler {
	return &balanceScheduler{
		changefeedID:       changefeedID,
		batchSize:          batchSize,
		random:             rand.New(rand.NewSource(time.Now().UnixNano())),
		operatorController: oc,
		spanController:     sc,
		nodeManager:        appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName),
		splitter:           splitter,
		mode:               mode,
	}
}

func (s *balanceScheduler) Execute() time.Time {
	failpoint.Inject("StopBalanceScheduler", func() {
		failpoint.Return(time.Now().Add(time.Second * 5))
	})

	// TODO: consider to ignore split tables' dispatcher basic schedule operator to decide whether we can make balance schedule
	if s.operatorController.OperatorSize() > 0 || s.spanController.GetAbsentSize() > 0 {
		// not in stable schedule state, skip balance
		return time.Now().Add(time.Second * 5)
	}

	// 1. check whether we have spans in defaultGroupID need to be splitted.
	//    we only consider the not splitted span here.
	checkResults := s.spanController.CheckByGroup(pkgReplica.DefaultGroupID, s.batchSize)
	count := s.doSplit(checkResults)

	// to many split operators, do move operator later
	if count >= s.batchSize {
		return time.Now().Add(time.Second * 5)
	}

	// 2. do balance for the spans in defaultGroupID
	s.schedulerDefaultGroup(s.batchSize - count)

	return time.Now().Add(time.Second * 5)
}

func (s *balanceScheduler) Name() string {
	if common.IsRedoMode(s.mode) {
		return pkgScheduler.RedoBalanceScheduler
	}
	return pkgScheduler.BalanceScheduler
}

func (s *balanceScheduler) schedulerDefaultGroup(maxSize int) int {
	nodes := s.nodeManager.GetAliveNodes()
	group := pkgreplica.DefaultGroupID
	// fast path, check the balance status
	moveSize := pkgScheduler.CheckBalanceStatus(s.spanController.GetTaskSizePerNodeByGroup(group), nodes)
	if moveSize <= 0 {
		return 0
	}
	replicas := s.spanController.GetReplicatingByGroup(group)
	return pkgScheduler.Balance(maxSize, s.random, nodes, replicas, s.doMove)
}

func (s *balanceScheduler) doSplit(results pkgReplica.GroupCheckResult) int {
	if results == nil {
		return 0
	}
	checkResults := results.([]replica.DefaultSpanSplitCheckResult)
	splitCount := 0
	for _, result := range checkResults {
		spansNum := max(result.SpanNum, len(s.nodeManager.GetAliveNodes())*2)
		splitSpans := s.splitter.Split(context.Background(), result.Span.Span, spansNum, result.SpanType)
		if len(splitSpans) > 1 {
			op := operator.NewSplitDispatcherOperator(s.spanController, result.Span, splitSpans, []node.ID{}, nil)
			ret := s.operatorController.AddOperator(op)
			if ret {
				splitCount++
			}
		}
	}

	return splitCount
}

func (s *balanceScheduler) doMove(replication *replica.SpanReplication, id node.ID) bool {
	op := operator.NewMoveDispatcherOperator(s.spanController, replication, replication.GetNodeID(), id)
	return s.operatorController.AddOperator(op)
}
