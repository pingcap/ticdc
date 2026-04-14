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
	"fmt"
	"sync"

	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/node"
)

// event_dispatcher_mananger_info.go is used to store the basic info and function of the event dispatcher manager

type dispatcherCreateInfo struct {
	Id        common.DispatcherID
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

func (e *DispatcherManager) SetMaintainerID(maintainerID node.ID) {
	e.meta.Lock()
	defer e.meta.Unlock()
	e.meta.maintainerID = maintainerID
}

func (e *DispatcherManager) GetMaintainerSessionEpoch() uint64 {
	e.meta.Lock()
	defer e.meta.Unlock()
	return e.meta.maintainerSessionEpoch
}

func (e *DispatcherManager) SetMaintainerSession(maintainerID node.ID, sessionEpoch uint64) {
	e.meta.Lock()
	defer e.meta.Unlock()
	e.meta.maintainerID = maintainerID
	e.meta.maintainerSessionEpoch = sessionEpoch
}

func (e *DispatcherManager) GetMaintainerEpoch() uint64 {
	e.meta.Lock()
	defer e.meta.Unlock()
	return e.meta.maintainerEpoch
}

// AcceptBootstrapSession is the only path that may install or advance the
// runtime maintainer session on dispatcher manager.
func (e *DispatcherManager) AcceptBootstrapSession(maintainerID node.ID, sessionEpoch uint64) (bool, string) {
	if sessionEpoch == 0 {
		// Zero means the peer is still running the legacy protocol. We must keep
		// accepting that path during rolling upgrade, otherwise a new dispatcher
		// manager can no longer interoperate with an old maintainer.
		return true, "legacy"
	}

	e.meta.Lock()
	defer e.meta.Unlock()

	switch {
	case e.meta.maintainerSessionEpoch == 0:
		e.meta.maintainerID = maintainerID
		e.meta.maintainerSessionEpoch = sessionEpoch
		return true, "install"
	case sessionEpoch < e.meta.maintainerSessionEpoch:
		return false, fmt.Sprintf("stale:%d<%d", sessionEpoch, e.meta.maintainerSessionEpoch)
	case sessionEpoch == e.meta.maintainerSessionEpoch:
		if e.meta.maintainerID != maintainerID {
			e.meta.maintainerID = maintainerID
		}
		return true, "current"
	default:
		e.meta.maintainerID = maintainerID
		e.meta.maintainerSessionEpoch = sessionEpoch
		return true, "advance"
	}
}

func (e *DispatcherManager) AcceptMaintainerSession(sessionEpoch uint64) (bool, string) {
	current := e.GetMaintainerSessionEpoch()
	if sessionEpoch == 0 {
		// Zero-session control messages are only allowed before the dispatcher
		// manager enters the session-aware path. Once a non-zero owner session is
		// installed, later legacy control messages can only come from stale owners.
		if current == 0 {
			return true, "legacy"
		}
		return false, fmt.Sprintf("stale legacy:0<%d", current)
	}

	switch {
	case sessionEpoch < current:
		return false, fmt.Sprintf("stale:%d<%d", sessionEpoch, current)
	case sessionEpoch == current:
		return true, "current"
	default:
		return false, fmt.Sprintf("future:%d>%d", sessionEpoch, current)
	}
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
