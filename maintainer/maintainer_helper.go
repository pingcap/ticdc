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
	"sync"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/node"
)

type WatermarkCaptureMap struct {
	mu sync.RWMutex
	m  map[node.ID]heartbeatpb.Watermark
}

func newWatermarkCaptureMap() *WatermarkCaptureMap {
	return &WatermarkCaptureMap{
		m: make(map[node.ID]heartbeatpb.Watermark),
	}
}

func (c *WatermarkCaptureMap) Get(nodeID node.ID) (heartbeatpb.Watermark, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	watermark, ok := c.m[nodeID]
	return watermark, ok
}

func (c *WatermarkCaptureMap) Set(nodeID node.ID, watermark heartbeatpb.Watermark) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[nodeID] = watermark
}

func (c *WatermarkCaptureMap) Delete(nodeID node.ID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, nodeID)
}

// ========================== Exported methods for HTTP API ==========================

// GetDispatcherCount returns the number of dispatchers.
func (m *Maintainer) GetDispatcherCount(mode int64) int {
	if common.IsRedoMode(mode) {
		return len(m.controller.redoSpanController.GetAllTasks())
	}
	return len(m.controller.spanController.GetAllTasks())
}

// MoveTable moves a table to a specific node.
func (m *Maintainer) MoveTable(tableId int64, targetNode node.ID, mode int64) error {
	return m.controller.moveTable(tableId, targetNode, mode)
}

// MoveSplitTable moves all the dispatchers in a split table to a specific node.
func (m *Maintainer) MoveSplitTable(tableId int64, targetNode node.ID, mode int64) error {
	return m.controller.moveSplitTable(tableId, targetNode, mode)
}

// GetTables returns all tables.
func (m *Maintainer) GetTables(mode int64) []*replica.SpanReplication {
	if common.IsRedoMode(mode) {
		return m.controller.redoSpanController.GetAllTasks()
	}
	return m.controller.spanController.GetAllTasks()
}

// SplitTableByRegionCount split table based on region count
// it can split the table whether the table have one dispatcher or multiple dispatchers
func (m *Maintainer) SplitTableByRegionCount(tableId int64, mode int64) error {
	return m.controller.splitTableByRegionCount(tableId, mode)
}

// MergeTable merge two dispatchers in this table into one dispatcher,
// so after merge table, the table may also have multiple dispatchers
func (m *Maintainer) MergeTable(tableId int64, mode int64) error {
	return m.controller.mergeTable(tableId, mode)
}
