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
	"math/rand"
	"time"

	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/coordinator/operator"
	"github.com/pingcap/ticdc/pkg/node"
	pkgScheduler "github.com/pingcap/ticdc/pkg/scheduler"
)

// balanceScheduler is used to check the balance status of all spans among all nodes
type balanceScheduler struct {
	id        string
	batchSize int

	operatorController  *operator.Controller
	changefeedDB        *changefeed.ChangefeedDB
	getSchedulableNodes func() map[node.ID]*node.Info

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
}

func NewBalanceScheduler(
	id string, batchSize int,
	oc *operator.Controller,
	changefeedDB *changefeed.ChangefeedDB,
	balanceInterval time.Duration,
	getSchedulableNodes func() map[node.ID]*node.Info,
) *balanceScheduler {
	return &balanceScheduler{
		id:                   id,
		batchSize:            batchSize,
		random:               rand.New(rand.NewSource(time.Now().UnixNano())),
		operatorController:   oc,
		changefeedDB:         changefeedDB,
		getSchedulableNodes:  getSchedulableNodes,
		checkBalanceInterval: balanceInterval,
		lastRebalanceTime:    time.Now(),
	}
}

func (s *balanceScheduler) Execute() time.Time {
	if !s.forceBalance && time.Since(s.lastRebalanceTime) < s.checkBalanceInterval {
		return s.lastRebalanceTime.Add(s.checkBalanceInterval)
	}
	now := time.Now()

	if s.operatorController.OperatorSize() > 0 || s.changefeedDB.GetAbsentSize() > 0 {
		// not in stable schedule state, skip balance
		return now.Add(s.checkBalanceInterval)
	}
	schedulableNodes := s.getSchedulableNodes()
	if len(schedulableNodes) <= 1 {
		return now.Add(s.checkBalanceInterval)
	}

	// check the balance status
	moveSize := pkgScheduler.CheckBalanceStatus(s.changefeedDB.GetTaskSizePerNode(), schedulableNodes)
	if moveSize <= 0 {
		// fast check the balance status, no need to do the balance,skip
		return now.Add(s.checkBalanceInterval)
	}
	// balance changefeeds among the active nodes
	movedSize := pkgScheduler.Balance(s.batchSize, s.random, schedulableNodes, s.changefeedDB.GetReplicating(),
		func(cf *changefeed.Changefeed, nodeID node.ID) bool {
			return s.operatorController.AddOperator(operator.NewMoveMaintainerOperator(s.changefeedDB, cf, cf.GetNodeID(), nodeID))
		})
	s.forceBalance = movedSize >= s.batchSize
	s.lastRebalanceTime = time.Now()

	return now.Add(s.checkBalanceInterval)
}

func (s *balanceScheduler) Name() string {
	return "balance-scheduler"
}
