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

package maintainer

import (
	"bytes"
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/span"
	"github.com/pingcap/ticdc/maintainer/split"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/utils"
	"go.uber.org/zap"
)

// FinishBootstrap finalizes the Controller initialization process using bootstrap responses from all nodes.
// This method is the main entry point for Controller initialization and performs several critical steps:
//
//  1. Determines the actual startTs by getting the checkpointTs from all node responses
//     and updates the DDL dispatcher status accordingly.
//     Only when the downstream is mysql-class, startTs != startCheckpointTs.
//     In this case, startTs will be the real startTs of the table trigger event dispatcher.
//
// 2. Loads the table schemas from the schema store using the determined start timestamp
//
// 3. Processes existing table assignments:
//
//   - Creates a mapping of currently running table spans across nodes
//
//   - For each table in the schema store:
//
//   - If not currently running: creates new table assignments
//
//   - If already running: maintains existing assignments and handles any gaps
//     in table coverage when table-across-nodes is enabled
//
//     4. Handles edge cases such as orphaned table assignments that may occur during
//     DDL operations (e.g., DROP TABLE) concurrent with node restarts
//
// 5. Initializes and starts core components:
//   - Rebuilds barrier status for consistency tracking
//   - Starts the scheduler controller for table distribution
//   - Starts the operator controller for managing table operations
//
// Parameters:
//   - allNodesResp: Bootstrap responses from all nodes containing their current state
//   - isMysqlCompatible: Flag indicating if using MySQL-compatible backend
//
// Returns:
//   - *MaintainerPostBootstrapRequest: Configuration for post-bootstrap setup
//   - error: Any error encountered during the bootstrap process
func (c *Controller) FinishBootstrap(
	allNodesResp map[node.ID]*heartbeatpb.MaintainerBootstrapResponse,
	isMysqlCompatibleBackend bool,
) (*heartbeatpb.MaintainerPostBootstrapRequest, error) {
	if c.bootstrapped {
		log.Panic("already bootstrapped",
			zap.Stringer("changefeed", c.changefeedID),
			zap.Any("allNodesResp", allNodesResp))
	}

	log.Info("all nodes have sent bootstrap response, start to handle them",
		zap.Stringer("changefeed", c.changefeedID),
		zap.Int("nodeCount", len(allNodesResp)))

	// Step 1: Determine start timestamp and update DDL dispatcher
	startTs := c.determineStartTs(allNodesResp)

	// Step 2: Load tables from schema store
	tables, err := c.loadTables(startTs)
	if err != nil {
		log.Error("load table from scheme store failed",
			zap.String("changefeed", c.changefeedID.Name()),
			zap.Error(err))
		return nil, errors.Trace(err)
	}

	// Step 3: Build working task map from bootstrap responses
	workingTaskMap, redoWorkingTaskMap := c.buildWorkingTaskMap(allNodesResp)

	// Step 4: Process tables and build schema info
	schemaInfos := c.processTablesAndBuildSchemaInfo(tables, workingTaskMap, redoWorkingTaskMap, isMysqlCompatibleBackend)

	// Step 5: Handle any remaining working tasks (likely dropped tables)
	c.handleRemainingWorkingTasks(workingTaskMap, redoWorkingTaskMap)

	// Step 6: Initialize and start sub components
	c.initializeComponents(allNodesResp)

	// Step 7: Prepare response
	initSchemaInfos := c.prepareSchemaInfoResponse(schemaInfos)

	// Step 8: Mark the controller as bootstrapped
	c.bootstrapped = true

	return &heartbeatpb.MaintainerPostBootstrapRequest{
		ChangefeedID:                  c.changefeedID.ToPB(),
		TableTriggerEventDispatcherId: c.spanController.GetDDLDispatcherID().ToPB(),
		Schemas:                       initSchemaInfos,
	}, nil
}

func (c *Controller) determineStartTs(allNodesResp map[node.ID]*heartbeatpb.MaintainerBootstrapResponse) uint64 {
	startTs := uint64(0)
	for node, resp := range allNodesResp {
		log.Info("handle bootstrap response",
			zap.Stringer("changefeed", c.changefeedID),
			zap.Stringer("nodeID", node),
			zap.Uint64("checkpointTs", resp.CheckpointTs),
			zap.Int("spanCount", len(resp.Spans)))
		if resp.CheckpointTs > startTs {
			startTs = resp.CheckpointTs
			status := c.spanController.GetDDLDispatcher().GetStatus()
			status.CheckpointTs = startTs
			c.spanController.UpdateStatus(c.spanController.GetDDLDispatcher(), status)
		}
	}
	if startTs == 0 {
		log.Panic("cant not found the startTs from the bootstrap response",
			zap.String("changefeed", c.changefeedID.Name()))
	}
	return startTs
}

func (c *Controller) buildWorkingTaskMap(
	allNodesResp map[node.ID]*heartbeatpb.MaintainerBootstrapResponse,
) (
	map[int64]utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication],
	map[int64]utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication],
) {
	workingTaskMap := make(map[int64]utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication])
	redoWorkingTaskMap := make(map[int64]utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication])
	for node, resp := range allNodesResp {
		for _, spanInfo := range resp.Spans {
			dispatcherID := common.NewDispatcherIDFromPB(spanInfo.ID)
			spanController := c.getSpanController(spanInfo.Mode)
			if spanController.IsDDLDispatcher(dispatcherID) {
				continue
			}
			spanReplication := c.createSpanReplication(spanInfo, node)
			if common.IsRedoMode(spanInfo.Mode) {
				addToWorkingTaskMap(redoWorkingTaskMap, spanInfo.Span, spanReplication)
			} else {
				addToWorkingTaskMap(workingTaskMap, spanInfo.Span, spanReplication)
			}
		}
	}
	return workingTaskMap, redoWorkingTaskMap
}

func (c *Controller) processTablesAndBuildSchemaInfo(
	tables []commonEvent.Table,
	workingTaskMap, redoWorkingTaskMap map[int64]utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication],
	isMysqlCompatibleBackend bool,
) map[int64]*heartbeatpb.SchemaInfo {
	schemaInfos := make(map[int64]*heartbeatpb.SchemaInfo)

	for _, table := range tables {
		schemaID := table.SchemaID

		// Add schema info if not exists
		if _, ok := schemaInfos[schemaID]; !ok {
			schemaInfos[schemaID] = getSchemaInfo(table, isMysqlCompatibleBackend)
		}

		// Add table info to schema
		tableInfo := getTableInfo(table, isMysqlCompatibleBackend)
		schemaInfos[schemaID].Tables = append(schemaInfos[schemaID].Tables, tableInfo)

		// Process table spans
		if c.enableRedo {
			c.processTableSpans(table, redoWorkingTaskMap, common.RedoMode)
		}
		c.processTableSpans(table, workingTaskMap, common.DefaultMode)
	}

	return schemaInfos
}

func (c *Controller) processTableSpans(
	table commonEvent.Table,
	workingTaskMap map[int64]utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication],
	mode int64,
) {
	tableSpans, isTableWorking := workingTaskMap[table.TableID]
	spanController := c.getSpanController(mode)

	// Add new table if not working
	if isTableWorking {
		// Handle existing table spans
		keyspaceID := c.GetKeyspaceID()
		span := common.TableIDToComparableSpan(keyspaceID, table.TableID)
		tableSpan := &heartbeatpb.TableSpan{
			TableID:    table.TableID,
			StartKey:   span.StartKey,
			EndKey:     span.EndKey,
			KeyspaceID: keyspaceID,
		}
		log.Info("table already working in other node",
			zap.Stringer("changefeed", c.changefeedID),
			zap.Int64("tableID", table.TableID))

		spanController.AddWorkingSpans(tableSpans)

		if c.enableTableAcrossNodes {
			c.handleTableHoles(spanController, table, tableSpans, tableSpan)
		}
		// Remove processed table from working task map
		delete(workingTaskMap, table.TableID)
	} else {
		spanController.AddNewTable(table, c.startCheckpointTs)
	}
}

func (c *Controller) handleTableHoles(
	spanController *span.Controller,
	table commonEvent.Table,
	tableSpans utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication],
	tableSpan *heartbeatpb.TableSpan,
) {
	holes := findHoles(tableSpans, tableSpan)
	if c.splitter != nil {
		for _, hole := range holes {
			spans := c.splitter.Split(context.Background(), hole, 0, split.SplitTypeRegionCount)
			spanController.AddNewSpans(table.SchemaID, spans, c.startCheckpointTs)
		}
	} else {
		spanController.AddNewSpans(table.SchemaID, holes, c.startCheckpointTs)
	}
}

func (c *Controller) handleRemainingWorkingTasks(
	workingTaskMap, redoWorkingTaskMap map[int64]utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication],
) {
	for tableID := range redoWorkingTaskMap {
		log.Warn("found a redo working table that is not in initial table map, just ignore it",
			zap.Stringer("changefeed", c.changefeedID),
			zap.Int64("tableID", tableID))
	}
	for tableID := range workingTaskMap {
		log.Warn("found a working table that is not in initial table map, just ignore it",
			zap.Stringer("changefeed", c.changefeedID),
			zap.Int64("tableID", tableID))
	}
}

func (c *Controller) initializeComponents(
	allNodesResp map[node.ID]*heartbeatpb.MaintainerBootstrapResponse,
) {
	// Initialize barrier
	if c.enableRedo {
		c.redoBarrier = NewBarrier(c.redoSpanController, c.redoOperatorController, c.cfConfig.Scheduler.EnableTableAcrossNodes, allNodesResp, common.RedoMode)
	}
	c.barrier = NewBarrier(c.spanController, c.operatorController, c.cfConfig.Scheduler.EnableTableAcrossNodes, allNodesResp, common.DefaultMode)

	// Start scheduler
	c.taskHandlesMu.Lock()
	c.taskHandles = append(c.taskHandles, c.schedulerController.Start(c.taskPool)...)

	if c.enableRedo {
		c.taskHandles = append(c.taskHandles, c.taskPool.Submit(c.redoOperatorController, time.Now()))
	}
	// Start operator controller
	c.taskHandles = append(c.taskHandles, c.taskPool.Submit(c.operatorController, time.Now()))
	c.taskHandlesMu.Unlock()
}

func (c *Controller) prepareSchemaInfoResponse(
	schemaInfos map[int64]*heartbeatpb.SchemaInfo,
) []*heartbeatpb.SchemaInfo {
	initSchemaInfos := make([]*heartbeatpb.SchemaInfo, 0, len(schemaInfos))
	for _, info := range schemaInfos {
		initSchemaInfos = append(initSchemaInfos, info)
	}
	return initSchemaInfos
}

func (c *Controller) createSpanReplication(spanInfo *heartbeatpb.BootstrapTableSpan, node node.ID) *replica.SpanReplication {
	status := &heartbeatpb.TableSpanStatus{
		ComponentStatus: spanInfo.ComponentStatus,
		ID:              spanInfo.ID,
		CheckpointTs:    spanInfo.CheckpointTs,
		Mode:            spanInfo.Mode,
	}

	return replica.NewWorkingSpanReplication(
		c.changefeedID,
		common.NewDispatcherIDFromPB(spanInfo.ID),
		spanInfo.SchemaID,
		spanInfo.Span,
		status,
		node,
	)
}

func (c *Controller) loadTables(startTs uint64) ([]commonEvent.Table, error) {
	// Use a empty timezone because table filter does not need it.
	f, err := filter.NewFilter(c.cfConfig.Filter, "", c.cfConfig.CaseSensitive, c.cfConfig.ForceReplicate)
	if err != nil {
		return nil, errors.Cause(err)
	}

	schemaStore := appcontext.GetService[schemastore.SchemaStore](appcontext.SchemaStore)
	tables, err := schemaStore.GetAllPhysicalTables(c.keyspaceMeta, startTs, f)
	return tables, err
}

func getSchemaInfo(table commonEvent.Table, isMysqlCompatibleBackend bool) *heartbeatpb.SchemaInfo {
	schemaInfo := &heartbeatpb.SchemaInfo{}
	schemaInfo.SchemaID = table.SchemaID
	if !isMysqlCompatibleBackend {
		schemaInfo.SchemaName = table.SchemaName
	}
	return schemaInfo
}

func getTableInfo(table commonEvent.Table, isMysqlCompatibleBackend bool) *heartbeatpb.TableInfo {
	tableInfo := &heartbeatpb.TableInfo{}
	tableInfo.TableID = table.TableID
	if !isMysqlCompatibleBackend {
		tableInfo.TableName = table.TableName
	}
	return tableInfo
}

func addToWorkingTaskMap(
	workingTaskMap map[int64]utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication],
	span *heartbeatpb.TableSpan,
	spanReplication *replica.SpanReplication,
) {
	tableSpans, ok := workingTaskMap[span.TableID]
	if !ok {
		tableSpans = utils.NewBtreeMap[*heartbeatpb.TableSpan, *replica.SpanReplication](common.LessTableSpan)
		workingTaskMap[span.TableID] = tableSpans
	}
	tableSpans.ReplaceOrInsert(span, spanReplication)
}

// findHoles returns an array of Span that are not covered in the range
func findHoles(currentSpan utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication], totalSpan *heartbeatpb.TableSpan) []*heartbeatpb.TableSpan {
	lastSpan := &heartbeatpb.TableSpan{
		TableID:    totalSpan.TableID,
		StartKey:   totalSpan.StartKey,
		EndKey:     totalSpan.StartKey,
		KeyspaceID: totalSpan.KeyspaceID,
	}
	var holes []*heartbeatpb.TableSpan
	// table span is sorted
	currentSpan.Ascend(func(current *heartbeatpb.TableSpan, _ *replica.SpanReplication) bool {
		ord := bytes.Compare(lastSpan.EndKey, current.StartKey)
		if ord < 0 {
			// Find a hole.
			holes = append(holes, &heartbeatpb.TableSpan{
				TableID:    totalSpan.TableID,
				StartKey:   lastSpan.EndKey,
				EndKey:     current.StartKey,
				KeyspaceID: totalSpan.KeyspaceID,
			})
		} else if ord > 0 {
			log.Panic("map is out of order",
				zap.String("lastSpan", lastSpan.String()),
				zap.String("current", current.String()))
		}
		lastSpan = current
		return true
	})
	// Check if there is a hole in the end.
	// the lastSpan not reach the totalSpan end
	if !bytes.Equal(lastSpan.EndKey, totalSpan.EndKey) {
		holes = append(holes, &heartbeatpb.TableSpan{
			TableID:    totalSpan.TableID,
			StartKey:   lastSpan.EndKey,
			EndKey:     totalSpan.EndKey,
			KeyspaceID: totalSpan.KeyspaceID,
		})
	}
	return holes
}
