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
}

type cleanMap struct {
	id       common.DispatcherID
	schemaID int64
	redo     bool
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

func (e *DispatcherManager) GetMaintainerEpoch() uint64 {
	e.meta.Lock()
	defer e.meta.Unlock()
	return e.meta.maintainerEpoch
}

func (e *DispatcherManager) GetTableTriggerEventDispatcher() *dispatcher.EventDispatcher {
	return e.tableTriggerEventDispatcher
}

func (e *DispatcherManager) SetHeartbeatRequestQueue(heartbeatRequestQueue *HeartbeatRequestQueue) {
	e.heartbeatRequestQueue = heartbeatRequestQueue
}

func (e *DispatcherManager) SetBlockStatusRequestQueue(blockStatusRequestQueue *BlockStatusRequestQueue) {
	e.blockStatusRequestQueue = blockStatusRequestQueue
}

// Get all dispatchers id of the specified schemaID. Including the tableTriggerEventDispatcherID if exists.
func (e *DispatcherManager) GetAllDispatchers(schemaID int64) []common.DispatcherID {
	dispatcherIDs := e.sharedInfo.GetSchemaIDToDispatchers().GetDispatcherIDs(schemaID)
	if e.tableTriggerEventDispatcher != nil {
		dispatcherIDs = append(dispatcherIDs, e.tableTriggerEventDispatcher.GetId())
	}
	return dispatcherIDs
}
