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
	"time"

	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/scheduler"
	"github.com/pingcap/ticdc/maintainer/span"
	"github.com/pingcap/ticdc/maintainer/split"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/node"
	pkgscheduler "github.com/pingcap/ticdc/pkg/scheduler"
)

func NewScheduleController(changefeedID common.ChangeFeedID,
	batchSize int,
	oc, redoOC *operator.Controller,
	spanController, redoSpanController *span.Controller,
	balanceInterval time.Duration,
	splitter *split.Splitter,
	schedulerCfg *config.ChangefeedSchedulerConfig,
	drainTargetGetter func() (node.ID, uint64),
	selfNodeGetter func() node.ID,
) *pkgscheduler.Controller {
	schedulers := map[string]pkgscheduler.Scheduler{
		pkgscheduler.BasicScheduler: scheduler.NewBasicScheduler(
			changefeedID,
			batchSize,
			oc,
			spanController,
			schedulerCfg,
			common.DefaultMode,
			drainTargetGetter,
		),
		pkgscheduler.DrainScheduler: scheduler.NewDrainScheduler(
			changefeedID,
			batchSize,
			oc,
			spanController,
			common.DefaultMode,
			drainTargetGetter,
			selfNodeGetter,
		),
		pkgscheduler.BalanceScheduler: scheduler.NewBalanceScheduler(
			changefeedID,
			batchSize,
			splitter,
			oc,
			spanController,
			balanceInterval,
			common.DefaultMode,
			drainTargetGetter,
		),
	}
	if splitter != nil {
		schedulers[pkgscheduler.BalanceSplitScheduler] = scheduler.NewBalanceSplitsScheduler(
			changefeedID,
			batchSize,
			splitter,
			oc,
			spanController,
			common.DefaultMode,
			drainTargetGetter,
		)
	}
	if redoOC != nil {
		schedulers[pkgscheduler.RedoBasicScheduler] = scheduler.NewBasicScheduler(
			changefeedID,
			batchSize,
			redoOC,
			redoSpanController,
			schedulerCfg,
			common.RedoMode,
			drainTargetGetter,
		)
		schedulers[pkgscheduler.RedoDrainScheduler] = scheduler.NewDrainScheduler(
			changefeedID,
			batchSize,
			redoOC,
			redoSpanController,
			common.RedoMode,
			drainTargetGetter,
			selfNodeGetter,
		)
		schedulers[pkgscheduler.RedoBalanceScheduler] = scheduler.NewBalanceScheduler(
			changefeedID,
			batchSize,
			splitter,
			redoOC,
			redoSpanController,
			balanceInterval,
			common.RedoMode,
			drainTargetGetter,
		)
		if splitter != nil {
			schedulers[pkgscheduler.RedoBalanceSplitScheduler] = scheduler.NewBalanceSplitsScheduler(
				changefeedID,
				batchSize,
				splitter,
				redoOC,
				redoSpanController,
				common.RedoMode,
				drainTargetGetter,
			)
		}
	}
	return pkgscheduler.NewController(schedulers)
}
