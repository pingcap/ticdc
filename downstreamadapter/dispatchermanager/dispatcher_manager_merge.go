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
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"go.uber.org/zap"
)

// TrackMergeOperator records an in-flight merge request so bootstrap can restore it after maintainer failover.
func (e *DispatcherManager) TrackMergeOperator(req *heartbeatpb.MergeDispatcherRequest) {
	if req == nil || req.MergedDispatcherID == nil {
		return
	}
	mergedID := common.NewDispatcherIDFromPB(req.MergedDispatcherID)
	if mergedID.IsZero() {
		log.Warn("ignore merge operator with invalid merged dispatcher ID",
			zap.Stringer("changefeedID", e.changefeedID),
			zap.Int64("mode", req.Mode))
		return
	}
	e.mergeOperatorMap.Store(mergedID.String(), cloneMergeDispatcherRequest(req))
}

// RemoveMergeOperator drops a persisted merge request that no longer needs bootstrap recovery.
func (e *DispatcherManager) RemoveMergeOperator(mergedDispatcherID common.DispatcherID) {
	e.mergeOperatorMap.Delete(mergedDispatcherID.String())
}

// MaybeCleanupMergeOperator removes a persisted merge request when the merged dispatcher is already complete or gone.
func (e *DispatcherManager) MaybeCleanupMergeOperator(req *heartbeatpb.MergeDispatcherRequest) {
	if req == nil || req.MergedDispatcherID == nil {
		return
	}
	mergedID := common.NewDispatcherIDFromPB(req.MergedDispatcherID)
	if e.isMergeOperatorFinished(req) {
		e.RemoveMergeOperator(mergedID)
		return
	}
	if e.hasMergedDispatcher(req) {
		return
	}
	log.Info("cleanup merge operator because merged dispatcher not found",
		zap.Stringer("changefeedID", e.changefeedID),
		zap.String("dispatcherID", mergedID.String()),
		zap.Int64("mode", req.Mode))
	e.RemoveMergeOperator(mergedID)
}

func (e *DispatcherManager) cleanupFinishedMergeOperators() {
	e.mergeOperatorMap.Range(func(_, value any) bool {
		req, ok := value.(*heartbeatpb.MergeDispatcherRequest)
		if !ok || req == nil || req.MergedDispatcherID == nil {
			return true
		}
		if !e.isMergeOperatorFinished(req) {
			return true
		}
		mergedID := common.NewDispatcherIDFromPB(req.MergedDispatcherID)
		log.Info("cleanup finished merge operator",
			zap.Stringer("changefeedID", e.changefeedID),
			zap.String("dispatcherID", mergedID.String()),
			zap.Int64("mode", req.Mode))
		e.RemoveMergeOperator(mergedID)
		return true
	})
}

func (e *DispatcherManager) isMergeOperatorFinished(req *heartbeatpb.MergeDispatcherRequest) bool {
	if req == nil || req.MergedDispatcherID == nil || len(req.DispatcherIDs) < 2 {
		return true
	}
	if common.IsRedoMode(req.Mode) {
		if !e.IsRedoReady() {
			return false
		}
		return isMergeOperatorFinished(req, e.redoDispatcherMap)
	}
	return isMergeOperatorFinished(req, e.dispatcherMap)
}

func isMergeOperatorFinished[T dispatcher.Dispatcher](
	req *heartbeatpb.MergeDispatcherRequest,
	dispatcherMap *DispatcherMap[T],
) bool {
	for _, idPB := range req.DispatcherIDs {
		if idPB == nil {
			continue
		}
		dispatcherID := common.NewDispatcherIDFromPB(idPB)
		if _, ok := dispatcherMap.Get(dispatcherID); ok {
			// Keep the merge journal while any source dispatcher still exists. A maintainer failover
			// in this window must restore the merge intent so late source terminal statuses do not
			// recreate spans that are already covered by the merged dispatcher.
			return false
		}
	}

	mergedID := common.NewDispatcherIDFromPB(req.MergedDispatcherID)
	mergedDispatcher, ok := dispatcherMap.Get(mergedID)
	if !ok {
		return true
	}
	return mergedDispatcher.GetComponentStatus() == heartbeatpb.ComponentState_Working
}

func (e *DispatcherManager) hasMergedDispatcher(req *heartbeatpb.MergeDispatcherRequest) bool {
	if req == nil || req.MergedDispatcherID == nil {
		return false
	}
	mergedID := common.NewDispatcherIDFromPB(req.MergedDispatcherID)
	if common.IsRedoMode(req.Mode) {
		if !e.IsRedoReady() {
			return false
		}
		_, ok := e.redoDispatcherMap.Get(mergedID)
		return ok
	}
	_, ok := e.dispatcherMap.Get(mergedID)
	return ok
}

// GetMergeOperators returns cloned in-flight merge requests for maintainer bootstrap.
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
