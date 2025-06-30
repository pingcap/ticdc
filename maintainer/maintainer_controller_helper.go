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
	"bytes"
	"context"
	"math/rand"
	"sort"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/pkg/apperror"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/node"
	pkgoperator "github.com/pingcap/ticdc/pkg/scheduler/operator"
	"go.uber.org/zap"
)

// functions in this file is about the methods for HTTP API, all these is just for test

// only for test
// moveTable is used for inner api(which just for make test cases convience) to force move a table to a target node.
// moveTable only works for the complete table, not for the table splited.
func (c *Controller) moveTable(tableId int64, targetNode node.ID) error {
	if err := c.checkParams(tableId, targetNode); err != nil {
		return err
	}

	replications := c.spanController.GetTasksByTableID(tableId)
	if len(replications) != 1 {
		return apperror.ErrTableIsNotFounded.GenWithStackByArgs("unexpected number of replications found for table in this node; tableID is %s, replication count is %s", tableId, len(replications))
	}

	replication := replications[0]

	op := c.operatorController.NewMoveOperator(replication, replication.GetNodeID(), targetNode)
	ret := c.operatorController.AddOperator(op)
	if !ret {
		return apperror.ErrOperatorIsNil.GenWithStackByArgs("unexpected error in create move operator")
	}

	// check the op is finished or not
	count := 0
	maxTry := 30
	for !op.IsFinished() && count < maxTry {
		time.Sleep(1 * time.Second)
		count += 1
		log.Info("wait for move table operator finished", zap.Int("count", count))
	}

	if !op.IsFinished() {
		return apperror.ErrTimeout.GenWithStackByArgs("move table operator is timeout")
	}

	return nil
}

// only for test
// moveSplitTable is used for inner api(which just for make test cases convience) to force move the dispatchers in a split table to a target node.
func (c *Controller) moveSplitTable(tableId int64, targetNode node.ID) error {
	if err := c.checkParams(tableId, targetNode); err != nil {
		return err
	}

	replications := c.spanController.GetTasksByTableID(tableId)
	opList := make([]pkgoperator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus], 0, len(replications))
	finishList := make([]bool, len(replications))
	for _, replication := range replications {
		if replication.GetNodeID() == targetNode {
			continue
		}
		op := c.operatorController.NewMoveOperator(replication, replication.GetNodeID(), targetNode)
		ret := c.operatorController.AddOperator(op)
		if !ret {
			for _, op := range opList {
				op.OnTaskRemoved()
			}
			return apperror.ErrOperatorIsNil.GenWithStackByArgs("unexpected error in create move operator")
		}
		opList = append(opList, op)
	}

	// check the op is finished or not
	count := 0
	maxTry := 30
	for count < maxTry {
		finish := true
		for idx, op := range opList {
			if finishList[idx] {
				continue
			}
			if op.IsFinished() {
				finishList[idx] = true
				continue
			} else {
				finish = false
			}
		}

		if finish {
			return nil
		}

		time.Sleep(1 * time.Second)
		count += 1
		log.Info("wait for move split table operator finished", zap.Int("count", count))
	}

	return apperror.ErrTimeout.GenWithStackByArgs("move split table operator is timeout")
}

// only for test
// splitTableByRegionCount split table based on region count
// it can split the table whether the table have one dispatcher or multiple dispatchers
func (c *Controller) splitTableByRegionCount(tableID int64) error {
	if !c.spanController.IsTableExists(tableID) {
		// the table is not exist in this node
		return apperror.ErrTableIsNotFounded.GenWithStackByArgs("tableID", tableID)
	}

	if tableID == 0 {
		return apperror.ErrTableNotSupportMove.GenWithStackByArgs("tableID", tableID)
	}

	replications := c.spanController.GetTasksByTableID(tableID)

	span := common.TableIDToComparableSpan(tableID)
	wholeSpan := &heartbeatpb.TableSpan{
		TableID:  span.TableID,
		StartKey: span.StartKey,
		EndKey:   span.EndKey,
	}
	splitTableSpans := c.spanController.GetSplitter().SplitSpansByRegion(context.Background(), wholeSpan)

	if len(splitTableSpans) == len(replications) {
		log.Info("Split Table is finished; There is no need to do split", zap.Any("tableID", tableID))
		return nil
	}

	randomIdx := rand.Intn(len(replications))
	primaryID := replications[randomIdx].ID
	primaryOp := operator.NewMergeSplitDispatcherOperator(c.spanController, primaryID, replications[randomIdx], replications, splitTableSpans, nil)
	operators := make([]*operator.MergeSplitDispatcherOperator, 0, len(replications))
	for _, replicaSet := range replications {
		var op *operator.MergeSplitDispatcherOperator
		if replicaSet.ID == primaryID {
			op = primaryOp
		} else {
			op = operator.NewMergeSplitDispatcherOperator(c.spanController, primaryID, replicaSet, nil, nil, primaryOp.GetOnFinished())
		}
		ret := c.operatorController.AddOperator(op)
		if !ret {
			// this op is created failed, so we need to remove the previous operators. Otherwise, the previous operators will never finish.
			for _, op := range operators {
				op.OnTaskRemoved()
			}
			return apperror.ErrOperatorIsNil.GenWithStackByArgs("unexpected error in create merge split dispatcher operator")
		}
		operators = append(operators, op)
	}

	count := 0
	maxTry := 30
	for count < maxTry {
		if primaryOp.IsFinished() {
			return nil
		}

		time.Sleep(1 * time.Second)
		count += 1
		log.Info("wait for split table operator finished", zap.Int("count", count))
	}

	return apperror.ErrTimeout.GenWithStackByArgs("split table operator is timeout")
}

// only for test
// mergeTable merge two nearby dispatchers in this table into one dispatcher,
// so after merge table, the table may also have multiple dispatchers
func (c *Controller) mergeTable(tableID int64) error {
	if !c.spanController.IsTableExists(tableID) {
		// the table is not exist in this node
		return apperror.ErrTableIsNotFounded.GenWithStackByArgs("tableID", tableID)
	}

	if tableID == 0 {
		return apperror.ErrTableNotSupportMove.GenWithStackByArgs("tableID", tableID)
	}

	replications := c.spanController.GetTasksByTableID(tableID)

	if len(replications) == 1 {
		log.Info("Merge Table is finished; There is only one replication for this table, so no need to do merge", zap.Any("tableID", tableID))
		return nil
	}

	// sort by startKey
	sort.Slice(replications, func(i, j int) bool {
		return bytes.Compare(replications[i].Span.StartKey, replications[j].Span.StartKey) < 0
	})

	log.Debug("sorted replications in mergeTable", zap.Any("replications", replications))

	// try to select two consecutive spans in the same node to merge
	// if we can't find, we just move one span to make it satisfied.
	idx := 0
	mergeSpanFound := false
	for idx+1 < len(replications) {
		if replications[idx].GetNodeID() == replications[idx+1].GetNodeID() && common.IsTableSpanConsecutive(replications[idx].Span, replications[idx+1].Span) {
			mergeSpanFound = true
			break
		} else {
			idx++
		}
	}

	if !mergeSpanFound {
		idx = 0
		// try to move the second span to the first span's node
		moveOp := c.operatorController.NewMoveOperator(replications[1], replications[1].GetNodeID(), replications[0].GetNodeID())
		ret := c.operatorController.AddOperator(moveOp)
		if !ret {
			return apperror.ErrOperatorIsNil.GenWithStackByArgs("unexpected error in create move operator")
		}

		count := 0
		maxTry := 30
		flag := false
		for count < maxTry {
			if moveOp.IsFinished() {
				flag = true
				break
			}
			time.Sleep(1 * time.Second)
			count += 1
			log.Info("wait for move table table operator finished", zap.Int("count", count))
		}

		if !flag {
			return apperror.ErrTimeout.GenWithStackByArgs("move table operator before merge table is timeout")
		}
	}

	operator := c.operatorController.AddMergeOperator(replications[idx : idx+2])
	if operator == nil {
		return apperror.ErrOperatorIsNil.GenWithStackByArgs("unexpected error in create merge operator")
	}

	count := 0
	maxTry := 30
	for count < maxTry {
		if operator.IsFinished() {
			return nil
		}

		time.Sleep(1 * time.Second)
		count += 1
		log.Info("wait for merge table table operator finished", zap.Int("count", count), zap.Any("operator", operator.String()))
	}

	return apperror.ErrTimeout.GenWithStackByArgs("merge table operator is timeout")
}

func (c *Controller) checkParams(tableId int64, targetNode node.ID) error {
	if !c.spanController.IsTableExists(tableId) {
		// the table is not exist in this node
		return apperror.ErrTableIsNotFounded.GenWithStackByArgs("tableID", tableId)
	}

	if tableId == 0 {
		return apperror.ErrTableNotSupportMove.GenWithStackByArgs("tableID", tableId)
	}

	nodes := c.nodeManager.GetAliveNodes()
	hasNode := false
	for _, node := range nodes {
		if node.ID == targetNode {
			hasNode = true
			break
		}
	}
	if !hasNode {
		return apperror.ErrNodeIsNotFound.GenWithStackByArgs("targetNode", targetNode)
	}

	return nil
}
