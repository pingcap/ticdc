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
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/scheduler"
	"github.com/pingcap/ticdc/maintainer/split"
	"github.com/pingcap/ticdc/pkg/common"
	pkgscheduler "github.com/pingcap/ticdc/pkg/scheduler"
	"github.com/pingcap/ticdc/server/watcher"
)

func NewScheduleController(changefeedID common.ChangeFeedID,
	batchSize int,
	oc *operator.Controller,
	db *replica.ReplicationDB,
	nodeM *watcher.NodeManager,
	balanceInterval time.Duration,
	splitter *split.Splitter,
) *pkgscheduler.Controller {
	schedulers := map[string]pkgscheduler.Scheduler{
		pkgscheduler.BasicScheduler: scheduler.NewBasicScheduler(
			changefeedID.String(),
			batchSize,
			oc,
			db,
			nodeM,
		),
		pkgscheduler.BalanceScheduler: scheduler.NewBalanceScheduler(
			changefeedID,
			batchSize,
			oc,
			db,
			nodeM,
			balanceInterval,
		),
	}
	if splitter != nil {
		schedulers[pkgscheduler.SplitScheduler] = scheduler.NewSplitScheduler(
			changefeedID,
			batchSize,
			splitter,
			oc,
			db,
			nodeM,
			balanceInterval,
		)
	}
	return pkgscheduler.NewController(schedulers)
}
