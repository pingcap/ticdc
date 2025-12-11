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
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

type OpType int

const (
	OpSplit OpType = iota // Split one span to multiple subspans
	OpMerge               // merge multiple spans to one span
	OpMove                // move one span to another node
)

func GetNewGroupChecker(
	cfID common.ChangeFeedID, schedulerCfg *config.ChangefeedSchedulerConfig,
) func(replica.GroupID) replica.GroupChecker[common.DispatcherID, *SpanReplication] {
	if schedulerCfg == nil || !util.GetOrZero(schedulerCfg.EnableTableAcrossNodes) {
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
