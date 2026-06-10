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

package dispatchermanager

import (
	"sync"

	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/node"
)

// dispatcher_manager_info.go stores the basic info and functions of the dispatcher manager.

type dispatcherCreateInfo struct {
	ID        common.DispatcherID
	TableSpan *heartbeatpb.TableSpan
	StartTs   uint64
	SchemaID  int64
	// SkipDMLAsStartTs indicates whether to skip DML events at (StartTs+1).
	// It is used when a dispatcher is recreated during an in-flight DDL barrier:
	// we need to replay the DDL by starting from (blockTs-1), while avoiding
	// potential duplicate DML writes at blockTs.
	SkipDMLAsStartTs bool
}

type cleanMap struct {
	id       common.DispatcherID
	schemaID int64
	mode     int64
}

func (e *DispatcherManager) GetDispatcherMap() *DispatcherMap[*dispatcher.EventDispatcher] {
	return e.dispatcherMap
}

func (e *DispatcherManager) GetMaintainerID() node.ID {
	e.meta.Lock()
	defer e.meta.Unlock()
	return e.meta.maintainerID
}

// TryUpdateMaintainer records the active maintainer owner and epoch.
// Maintainer epoch 0 is accepted only while the manager is still in compatibility
// mode. Once a non-zero epoch is known, epoch 0 must never downgrade the receiver
// back to compatibility mode.
func (e *DispatcherManager) TryUpdateMaintainer(from node.ID, maintainerEpoch uint64) bool {
	e.meta.Lock()
	defer e.meta.Unlock()
	if maintainerEpoch == 0 {
		if e.meta.maintainerEpoch != 0 {
			return false
		}
		e.meta.maintainerID = from
		return true
	}
	if e.meta.maintainerEpoch > maintainerEpoch {
		return false
	}
	if e.meta.maintainerEpoch == maintainerEpoch && e.meta.maintainerID != "" && e.meta.maintainerID != from {
		return false
	}
	e.meta.maintainerEpoch = maintainerEpoch
	e.meta.maintainerID = from
	return true
}

// IsMaintainerRequestAllowed reports whether a request belongs to the current
// maintainer owner/epoch view known by this dispatcher manager.
func (e *DispatcherManager) IsMaintainerRequestAllowed(from node.ID, maintainerEpoch uint64) bool {
	e.meta.Lock()
	defer e.meta.Unlock()
	if maintainerEpoch == 0 {
		return e.meta.maintainerEpoch == 0 && (e.meta.maintainerID == "" || e.meta.maintainerID == from)
	}
	return e.meta.maintainerEpoch == maintainerEpoch && e.meta.maintainerID == from
}

func (e *DispatcherManager) GetMaintainerEpoch() uint64 {
	e.meta.Lock()
	defer e.meta.Unlock()
	return e.meta.maintainerEpoch
}

func (e *DispatcherManager) GetTableTriggerEventDispatcher() *dispatcher.EventDispatcher {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.tableTriggerEventDispatcher
}

func (e *DispatcherManager) SetTableTriggerEventDispatcher(d *dispatcher.EventDispatcher) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.tableTriggerEventDispatcher = d
}

func (e *DispatcherManager) SetHeartbeatRequestQueue(heartbeatRequestQueue *HeartbeatRequestQueue) {
	e.heartbeatRequestQueue = heartbeatRequestQueue
}

func (e *DispatcherManager) SetBlockStatusRequestQueue(blockStatusRequestQueue *BlockStatusRequestQueue) {
	e.blockStatusRequestQueue = blockStatusRequestQueue
}

// Get all dispatchers id of the specified schemaID. Including the tableTriggerEventDispatcherID if exists.
func (e *DispatcherManager) GetAllDispatchers(schemaID int64) []common.DispatcherID {
	dispatcherIDs := e.schemaIDToDispatchers.GetDispatcherIDs(schemaID)
	if e.GetTableTriggerEventDispatcher() != nil {
		dispatcherIDs = append(dispatcherIDs, e.GetTableTriggerEventDispatcher().GetId())
	}
	return dispatcherIDs
}

func (e *DispatcherManager) GetCurrentOperatorMap() *sync.Map {
	return &e.currentOperatorMap
}

// IsRedoEnabled reports whether redo is configured for the changefeed.
func (e *DispatcherManager) IsRedoEnabled() bool {
	return e.redoEnabled
}

// IsRedoReady reports whether redo is configured and its runtime components are fully initialized.
func (e *DispatcherManager) IsRedoReady() bool {
	return e.IsRedoEnabled() &&
		e.redoReady.Load() &&
		e.redoSink != nil &&
		e.redoDispatcherMap != nil &&
		e.redoSchemaIDToDispatchers != nil
}
