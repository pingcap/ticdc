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
	"github.com/pingcap/ticdc/pkg/spanz"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils"
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
		scheduler.BasicScheduler: scheduler.NewBasicScheduler(
			changefeedID.String(),
			batchSize,
			oc,
			db,
			nodeM,
			oc.NewAddOperator,
		),
		scheduler.BalanceScheduler: scheduler.NewBalanceScheduler(
			changefeedID.String(),
			batchSize,
			oc,
			db,
			nodeM,
			balanceInterval,
			oc.NewMoveOperator,
		),
	}
	if splitter != nil {
		schedulers[scheduler.SplitScheduler] = newSplitScheduler(
			changefeedID,
			batchSize,
			splitter,
			oc,
			db,
			nodeM,
			balanceInterval,
		)
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
		maxCheckTime:  time.Second * 500,
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
			spans := s.splitter.SplitSpans(context.Background(), totalSpan, len(s.nodeManager.GetAliveNodes()))
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

func (s *splitScheduler) valid(c replica.CheckResult) (*heartbeatpb.TableSpan, bool) {
	if c.OpType == replica.OpSplit && len(c.Replications) != 1 {
		log.Panic("split operation should have only one replication",
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Int64("tableId", c.Replications[0].Span.TableID),
			zap.Stringer("checkResult", c))
	}
	span := spanz.TableIDToComparableSpan(c.Replications[0].Span.TableID)
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
		spanMap := utils.NewBtreeMap[*heartbeatpb.TableSpan, *replica.SpanReplication](heartbeatpb.LessTableSpan)
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
