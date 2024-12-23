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

package maintainer

import (
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/split"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/scheduler"
	pkgReplica "github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/tiflow/pkg/spanz"
	"go.uber.org/zap"
)

func NewScheduleController(changefeedID common.ChangeFeedID,
	batchSize int,
	oc *operator.Controller,
	db *replica.ReplicationDB,
	nodeM *watcher.NodeManager,
	balanceInterval time.Duration,
	splitter *split.Splitter,
) *scheduler.Controller {
	schedulers := map[string]scheduler.Scheduler{
		scheduler.BasicScheduler:   scheduler.NewBasicScheduler(changefeedID.String(), batchSize, oc, db, nodeM, oc.NewAddOperator),
		scheduler.BalanceScheduler: scheduler.NewBalanceScheduler(changefeedID.String(), batchSize, oc, db, nodeM, balanceInterval, oc.NewMoveOperator),
	}
	if splitter != nil {
		schedulers[scheduler.SplitScheduler] = newSplitScheduler(changefeedID, batchSize, splitter, oc, db, nodeM, balanceInterval)
	}
	return scheduler.NewController(schedulers)
}

// splitScheduler is used to check the split status of all spans
type splitScheduler struct {
	changefeedID common.ChangeFeedID

	splitter     *split.Splitter
	opController *operator.Controller
	db           *replica.ReplicationDB
	nodeManager  *watcher.NodeManager

	maxCheckTime  time.Duration
	checkInterval time.Duration
	lastCheckTime time.Time

	batchSize int
}

func newSplitScheduler(
	changefeedID common.ChangeFeedID, batchSize int, splitter *split.Splitter,
	oc *operator.Controller, db *replica.ReplicationDB, nodeManager *watcher.NodeManager,
	checkInterval time.Duration,
) *splitScheduler {
	return &splitScheduler{
		changefeedID:  changefeedID,
		splitter:      splitter,
		opController:  oc,
		db:            db,
		nodeManager:   nodeManager,
		batchSize:     batchSize,
		maxCheckTime:  time.Second * 5,
		checkInterval: checkInterval,
	}
}

func (s *splitScheduler) Execute() time.Time {
	if s.splitter == nil {
		return time.Time{}
	}
	if time.Since(s.lastCheckTime) < s.checkInterval {
		return s.lastCheckTime.Add(s.checkInterval)
	}

	log.Info("check split status", zap.String("changefeed", s.changefeedID.Name()),
		zap.String("hotSpans", s.db.GetCheckerStat()), zap.String("groupDistribution", s.db.GetGroupStat()))

	checked, batch, start := 0, s.batchSize, time.Now()
	needBreak := false
	for _, group := range s.db.GetGroups() {
		if needBreak || batch <= 0 {
			break
		}

		checkResults := s.db.CheckByGroup(group, s.batchSize)
		checked, needBreak = s.doCheck(checkResults, start)
		batch -= checked
		s.lastCheckTime = time.Now()
	}
	return s.lastCheckTime.Add(s.checkInterval)
}

func (s *splitScheduler) Name() string {
	return scheduler.SplitScheduler
}

func (s *splitScheduler) doCheck(ret pkgReplica.GroupCheckResult, start time.Time) (int, bool) {
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
		switch ret.OpType {
		case replica.OpSplit:
			span := ret.Replications[0]
			if s.db.GetTaskByID(span.ID) == nil {
				continue
			}
			spans := s.splitter.SplitSpans(context.Background(), span.Span, len(s.nodeManager.GetAliveNodes()), 0)
			if len(spans) > 1 {
				log.Info("split span",
					zap.String("changefeed", s.changefeedID.Name()),
					zap.String("dispatcher", span.ID.String()),
					zap.Int("span szie", len(spans)))
				// s.opController.AddOperator(operator.NewSplitDispatcherOperator(s.db, span, span.GetNodeID(), spans))
				s.opController.AddMergeSplitOperator(ret.Replications, spans)
			}
		case replica.OpMergeAndSplit:
			// check hole
			span := spanz.TableIDToComparableSpan(ret.Replications[0].Span.TableID)
			totalSpan := &heartbeatpb.TableSpan{
				TableID:  span.TableID,
				StartKey: span.StartKey,
				EndKey:   span.EndKey,
			}
			spans := s.splitter.SplitSpans(context.Background(), totalSpan, len(s.nodeManager.GetAliveNodes()), 0)
			if len(spans) > 1 {
				log.Info("split span",
					zap.String("changefeed", s.changefeedID.Name()),
					zap.String("span", totalSpan.String()),
					zap.Int("span szie", len(spans)))
				s.opController.AddMergeSplitOperator(ret.Replications, spans)
			}
		case replica.OpMerge:
			newSpan := spanz.TableIDToComparableSpan(ret.Replications[0].Span.TableID)
			s.opController.AddMergeSplitOperator(ret.Replications, []*heartbeatpb.TableSpan{
				{
					TableID:  newSpan.TableID,
					StartKey: newSpan.StartKey,
					EndKey:   newSpan.EndKey,
				},
			})
		}
	}
	return checkedIndex, false
}
