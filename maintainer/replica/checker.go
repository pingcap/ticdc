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

package replica

import (
	"fmt"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
	"go.uber.org/zap"
)

type OpType int

const (
	OpSplit         OpType = iota // Split one span to multiple subspans
	OpMerge                       // merge multiple spans to one span
	OpMergeAndSplit               // remove old spans and split to multiple subspans
	OpMove
)

const (
	HotSpanWriteThreshold = 1024 * 1024 // 1MB per second
	HotSpanScoreThreshold = 3           // TODO: bump to 10 befroe release
	DefaultScoreThreshold = 10

	// defaultHardImbalanceThreshold = float64(1.35) // used to trigger the rebalance
	defaultHardImbalanceThreshold = float64(5) // used to trigger the rebalance
	clearTimeout                  = 300        // seconds
)

var MinSpanNumberCoefficient = 0

type CheckResult struct {
	OpType       OpType
	Replications []*SpanReplication
}

func (c CheckResult) String() string {
	opStr := ""
	switch c.OpType {
	case OpSplit:
		opStr = "split"
	case OpMerge:
		opStr = "merge"
	case OpMergeAndSplit:
		opStr = "merge and split"
	default:
		panic("unknown op type")
	}
	return fmt.Sprintf("OpType: %s, ReplicationSize: %d", opStr, len(c.Replications))
}

func GetNewGroupChecker(
	cfID common.ChangeFeedID, schedulerCfg *config.ChangefeedSchedulerConfig,
) func(replica.GroupID) replica.GroupChecker[common.DispatcherID, *SpanReplication] {
	if schedulerCfg == nil || !schedulerCfg.EnableTableAcrossNodes {
		return replica.NewEmptyChecker[common.DispatcherID, *SpanReplication]
	}
	return func(groupID replica.GroupID) replica.GroupChecker[common.DispatcherID, *SpanReplication] {
		groupType := replica.GetGroupType(groupID)
		switch groupType {
		case replica.GroupDefault:
			return NewDefaultSpanSplitChecker(cfID, schedulerCfg)
		case replica.GroupTable:
			return NewSplitSpanChecker(cfID, groupID, schedulerCfg)
		}
		log.Panic("unknown group type", zap.String("changefeed", cfID.Name()), zap.Int8("groupType", int8(groupType)))
		return nil
	}
}
