// Copyright 2025 PingCAP, Inc.
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

package dispatchermanager

import (
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"go.uber.org/zap"
)

func (e *DispatcherManager) TrackMergeOperator(req *heartbeatpb.MergeDispatcherRequest) {
	if req == nil || req.MergedDispatcherID == nil {
		return
	}
	mergedID := common.NewDispatcherIDFromPB(req.MergedDispatcherID)
	e.mergeOperatorMap.Store(mergedID.String(), cloneMergeDispatcherRequest(req))
}

func (e *DispatcherManager) RemoveMergeOperator(mergedDispatcherID common.DispatcherID) {
	e.mergeOperatorMap.Delete(mergedDispatcherID.String())
}

func (e *DispatcherManager) MaybeCleanupMergeOperator(req *heartbeatpb.MergeDispatcherRequest) {
	if req == nil || req.MergedDispatcherID == nil {
		return
	}
	mergedID := common.NewDispatcherIDFromPB(req.MergedDispatcherID)
	if common.IsRedoMode(req.Mode) {
		if dispatcherItem, ok := e.redoDispatcherMap.Get(mergedID); ok {
			if dispatcherItem.GetComponentStatus() == heartbeatpb.ComponentState_Working {
				e.RemoveMergeOperator(mergedID)
			}
			return
		}
	} else {
		if dispatcherItem, ok := e.dispatcherMap.Get(mergedID); ok {
			if dispatcherItem.GetComponentStatus() == heartbeatpb.ComponentState_Working {
				e.RemoveMergeOperator(mergedID)
			}
			return
		}
	}
	log.Info("cleanup merge operator because merged dispatcher not found",
		zap.Stringer("changefeedID", e.changefeedID),
		zap.String("dispatcherID", mergedID.String()),
		zap.Int64("mode", req.Mode))
	e.RemoveMergeOperator(mergedID)
}

func (e *DispatcherManager) GetMergeOperators() []*heartbeatpb.MergeDispatcherRequest {
	operators := make([]*heartbeatpb.MergeDispatcherRequest, 0)
	e.mergeOperatorMap.Range(func(_, value any) bool {
		req, ok := value.(*heartbeatpb.MergeDispatcherRequest)
		if !ok || req == nil {
			return true
		}
		operators = append(operators, cloneMergeDispatcherRequest(req))
		return true
	})
	return operators
}

func cloneMergeDispatcherRequest(req *heartbeatpb.MergeDispatcherRequest) *heartbeatpb.MergeDispatcherRequest {
	if req == nil {
		return nil
	}
	clone := &heartbeatpb.MergeDispatcherRequest{
		Mode: req.Mode,
	}
	if req.ChangefeedID != nil {
		id := *req.ChangefeedID
		clone.ChangefeedID = &id
	}
	if req.MergedDispatcherID != nil {
		mergedID := *req.MergedDispatcherID
		clone.MergedDispatcherID = &mergedID
	}
	if len(req.DispatcherIDs) > 0 {
		clone.DispatcherIDs = make([]*heartbeatpb.DispatcherID, 0, len(req.DispatcherIDs))
		for _, dispatcherID := range req.DispatcherIDs {
			if dispatcherID == nil {
				continue
			}
			id := *dispatcherID
			clone.DispatcherIDs = append(clone.DispatcherIDs, &id)
		}
	}
	return clone
}
