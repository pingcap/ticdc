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
	pkgscheduler "github.com/pingcap/ticdc/pkg/scheduler"
)

func NewScheduleController(changefeedID common.ChangeFeedID,
	batchSize int,
	oc *operator.Controller,
	spanController *span.Controller,
	balanceInterval time.Duration,
	splitter *split.Splitter,
	schedulerCfg *config.ChangefeedSchedulerConfig,
) *pkgscheduler.Controller {
	schedulers := map[string]pkgscheduler.Scheduler{
		pkgscheduler.BasicScheduler: scheduler.NewBasicScheduler(
			changefeedID.String(),
			batchSize,
			oc,
			spanController,
			schedulerCfg,
		),
		pkgscheduler.BalanceScheduler: scheduler.NewBalanceScheduler(
			changefeedID,
			batchSize,
			oc,
			spanController,
			balanceInterval,
		),
	}
	if splitter != nil {
		schedulers[pkgscheduler.SplitScheduler] = scheduler.NewSplitScheduler(
			changefeedID,
			batchSize,
			splitter,
			oc,
			spanController,
			balanceInterval,
		)
	}
	return pkgscheduler.NewController(schedulers)
}
