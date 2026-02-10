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
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/span"
	"github.com/pingcap/ticdc/maintainer/split"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/util"
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
		log.Info("maintainer already bootstrapped, may a new node join the cluster",
			zap.Stringer("changefeed", c.changefeedID),
			zap.Any("allNodesResp", allNodesResp))
		return nil, nil
	}

	log.Info("all nodes have sent bootstrap response, start to handle them",
		zap.Stringer("changefeed", c.changefeedID),
		zap.Int("nodeCount", len(allNodesResp)))

	// Step 1: Determine start timestamp and update DDL dispatcher
	startTs, redoStartTs := c.determineStartTs(allNodesResp)

	// Step 2: Load tables from schema store
	tables, err := c.loadTables(startTs)
	if err != nil {
		log.Error("load table from scheme store failed",
			zap.String("changefeed", c.changefeedID.Name()),
			zap.Error(err))
		return nil, errors.Trace(err)
	}

	var (
		redoTables         []commonEvent.Table
		redoWorkingTaskMap map[int64]utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication]
		redoSchemaInfos    map[int64]*heartbeatpb.SchemaInfo
	)
	if c.enableRedo {
		redoTables, err = c.loadTables(redoStartTs)
		if err != nil {
			log.Error("load table from scheme store failed",
				zap.String("changefeed", c.changefeedID.Name()),
				zap.Error(err))
			return nil, errors.Trace(err)
		}
	}

	// Step 3: Build working task map from bootstrap responses and Process tables and build schema info
	workingTaskMap, schemaInfos, err := c.buildTaskInfo(allNodesResp, tables, isMysqlCompatibleBackend, common.DefaultMode)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if c.enableRedo {
		redoWorkingTaskMap, redoSchemaInfos, err = c.buildTaskInfo(allNodesResp, redoTables, isMysqlCompatibleBackend, common.RedoMode)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// Step 4: Handle any remaining working tasks (likely dropped tables)
	c.handleRemainingWorkingTasks(workingTaskMap, redoWorkingTaskMap)

	// Step 5: Initialize and start sub components
	c.initializeComponents(allNodesResp)

	// Step 6: Mark the controller as bootstrapped
	c.bootstrapped = true

	return &heartbeatpb.MaintainerPostBootstrapRequest{
		ChangefeedID:                  c.changefeedID.ToPB(),
		TableTriggerEventDispatcherId: c.spanController.GetDDLDispatcherID().ToPB(),
		Schemas:                       c.prepareSchemaInfoResponse(schemaInfos),
		RedoSchemas:                   c.prepareSchemaInfoResponse(redoSchemaInfos),
	}, nil
}

func (c *Controller) determineStartTs(allNodesResp map[node.ID]*heartbeatpb.MaintainerBootstrapResponse) (uint64, uint64) {
	var (
		startTs     uint64
		redoStartTs uint64
	)
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
		if c.enableRedo && resp.RedoCheckpointTs > redoStartTs {
			redoStartTs = resp.RedoCheckpointTs
			redoStatus := c.redoSpanController.GetDDLDispatcher().GetStatus()
			redoStatus.CheckpointTs = redoStartTs
			c.redoSpanController.UpdateStatus(c.redoSpanController.GetDDLDispatcher(), redoStatus)
		}
	}
	if startTs == 0 {
		log.Panic("cant not found the startTs from the bootstrap response",
			zap.String("changefeed", c.changefeedID.Name()))
	}
	if c.enableRedo && redoStartTs == 0 {
		log.Panic("cant not found the redoStartTs from the bootstrap response",
			zap.String("changefeed", c.changefeedID.Name()))
	}
	return startTs, redoStartTs
}

func (c *Controller) buildWorkingTaskMap(
	allNodesResp map[node.ID]*heartbeatpb.MaintainerBootstrapResponse,
	tableSplitMap map[int64]bool,
	mode int64,
) map[int64]utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication] {
	workingTaskMap := make(map[int64]utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication])
	spanController := c.getSpanController(mode)
	for node, resp := range allNodesResp {
		for _, spanInfo := range resp.Spans {
			if spanInfo.Mode != mode {
				continue
			}
			dispatcherID := common.NewDispatcherIDFromPB(spanInfo.ID)
			if spanController.IsDDLDispatcher(dispatcherID) {
				continue
			}
			splitEnabled := spanController.ShouldEnableSplit(tableSplitMap[spanInfo.Span.TableID])
			spanReplication := c.createSpanReplication(spanInfo, node, splitEnabled)
			addToWorkingTaskMap(workingTaskMap, spanInfo.Span, spanReplication)
		}
	}
	return workingTaskMap
}

func (c *Controller) processTablesAndBuildSchemaInfo(
	tables []commonEvent.Table,
	workingTaskMap map[int64]utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication],
	isMysqlCompatibleBackend bool,
	mode int64,
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

		c.processTableSpans(table, workingTaskMap, mode)
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
	replicaSets := spanController.GetTasksByTableID(table.TableID)
	isTableSpanExists := replicaSets != nil && len(replicaSets) > 0
	splitEnabled := spanController.ShouldEnableSplit(table.Splitable)
	// During bootstrap we have two sources of "table already exists" information:
	//   - workingTaskMap: dispatchers reported by dispatcher managers (bootstrap snapshots resp.Spans).
	//   - spanController tasks: replica sets created locally by the maintainer bootstrap (for example by restoring
	//     in-flight create/remove operators), which may not be visible in workingTaskMap yet.
	//
	// Therefore it is possible that isTableWorking==false but isTableSpanExists==true: the table exists in
	// maintainer state, but the dispatcher is not observed as Working on any node snapshot (e.g. a Create operator
	// is still in-flight). In this case we must not create another replica set for the same table.
	if isTableWorking || isTableSpanExists {
		// Handle existing table spans
		keyspaceID := c.GetKeyspaceID()
		span := common.TableIDToComparableSpan(keyspaceID, table.TableID)
		tableSpan := &heartbeatpb.TableSpan{
			TableID:    table.TableID,
			StartKey:   span.StartKey,
			EndKey:     span.EndKey,
			KeyspaceID: keyspaceID,
		}
		if isTableWorking {
			log.Info("table already reported by bootstrap snapshots",
				zap.Stringer("changefeed", c.changefeedID),
				zap.Int64("tableID", table.TableID))
		} else {
			log.Info("table spans already exist in span controller during bootstrap",
				zap.Stringer("changefeed", c.changefeedID),
				zap.Int64("tableID", table.TableID))
		}

		if isTableWorking {
			spanController.AddWorkingSpans(tableSpans)
		}

		if c.enableTableAcrossNodes {
			if !isTableWorking && tableSpans == nil {
				tableSpans = utils.NewBtreeMap[*heartbeatpb.TableSpan, *replica.SpanReplication](common.LessTableSpan)
			}
			if isTableSpanExists {
				for _, replicaSet := range replicaSets {
					tableSpans.ReplaceOrInsert(replicaSet.Span, replicaSet)
				}
			}
			c.handleTableHoles(spanController, table, tableSpans, tableSpan, splitEnabled)
		}
		// Remove processed table from working task map
		if isTableWorking {
			delete(workingTaskMap, table.TableID)
		}
	} else {
		spanController.AddNewTable(table, c.startTs)
	}
}

func (c *Controller) handleTableHoles(
	spanController *span.Controller,
	table commonEvent.Table,
	tableSpans utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication],
	tableSpan *heartbeatpb.TableSpan,
	splitEnabled bool,
) {
	holes := findHoles(tableSpans, tableSpan)
	if c.splitter != nil {
		for _, hole := range holes {
			spans := c.splitter.Split(context.Background(), hole, 0, split.SplitTypeRegionCount)
			spanController.AddNewSpans(table.SchemaID, spans, c.startTs, splitEnabled)
		}
	} else {
		spanController.AddNewSpans(table.SchemaID, holes, c.startTs, splitEnabled)
	}
}

func (c *Controller) buildTaskInfo(
	allNodesResp map[node.ID]*heartbeatpb.MaintainerBootstrapResponse,
	tables []commonEvent.Table,
	isMysqlCompatibleBackend bool,
	mode int64) (
	map[int64]utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication],
	map[int64]*heartbeatpb.SchemaInfo,
	error,
) {
	// Build table splitability map for later use
	tableSplitMap := make(map[int64]bool, len(tables))
	for _, tbl := range tables {
		tableSplitMap[tbl.TableID] = tbl.Splitable
	}
	workingTaskMap := c.buildWorkingTaskMap(allNodesResp, tableSplitMap, mode)
	// Restore current working operators first so spanController reflects "in-flight scheduling intent"
	// captured by dispatcher managers. This avoids bootstrap creating duplicate tasks for a dispatcherID
	// that is already being created/removed by a previous maintainer instance.
	if err := c.restoreCurrentWorkingOperators(allNodesResp, tableSplitMap, mode); err != nil {
		return nil, nil, err
	}
	schemaInfos := c.processTablesAndBuildSchemaInfo(tables, workingTaskMap, isMysqlCompatibleBackend, mode)
	return workingTaskMap, schemaInfos, nil
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
		c.redoBarrier = NewBarrier(c.redoSpanController, c.redoOperatorController, util.GetOrZero(c.replicaConfig.Scheduler.EnableTableAcrossNodes), allNodesResp, common.RedoMode)
	}
	c.barrier = NewBarrier(c.spanController, c.operatorController, util.GetOrZero(c.replicaConfig.Scheduler.EnableTableAcrossNodes), allNodesResp, common.DefaultMode)

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

func (c *Controller) createSpanReplication(spanInfo *heartbeatpb.BootstrapTableSpan, node node.ID, splitEnabled bool) *replica.SpanReplication {
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
		splitEnabled,
	)
}

func (c *Controller) loadTables(startTs uint64) ([]commonEvent.Table, error) {
	// Use a empty timezone because table filter does not need it.
	f, err := filter.NewFilter(c.replicaConfig.Filter, "", util.GetOrZero(c.replicaConfig.CaseSensitive), util.GetOrZero(c.replicaConfig.ForceReplicate))
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

// indexBootstrapSpans builds a lookup table from bootstrap span snapshots (resp.Spans) for a given mode.
// This helps operator restoration fill in missing Span/SchemaID in ScheduleDispatcherRequest configs.
func indexBootstrapSpans(
	spans []*heartbeatpb.BootstrapTableSpan,
	mode int64,
) map[common.DispatcherID]*heartbeatpb.BootstrapTableSpan {
	spanInfoByID := make(map[common.DispatcherID]*heartbeatpb.BootstrapTableSpan, len(spans))
	for _, spanInfo := range spans {
		if spanInfo == nil || spanInfo.ID == nil {
			continue
		}
		if spanInfo.Mode != mode {
			continue
		}
		id := common.NewDispatcherIDFromPB(spanInfo.ID)
		if id.IsZero() {
			continue
		}
		spanInfoByID[id] = spanInfo
	}
	return spanInfoByID
}

// restoreCurrentWorkingOperators rebuilds maintainer-side operators from dispatcher managers' bootstrap responses.
//
// Why:
//   - The dispatcher manager may still be executing scheduling requests (create/remove dispatcher) issued by a
//     previous maintainer instance.
//   - After a maintainer failover/restart, operatorController state is lost. If we ignore these in-flight requests,
//     the new maintainer can observe confusing combinations (e.g. orphan dispatchers, spans stuck in replicating
//     even though runtime already removed them, or duplicate scheduling decisions).
//
// Inputs and cases:
//   - resp.Spans: snapshot of existing dispatchers on the node.
//   - resp.Operators: snapshot of "current working" ScheduleDispatcherRequest(s) on the dispatcher manager.
//     Each request contains:
//   - ScheduleAction: Create/Remove (what dispatcher manager is doing).
//   - OperatorType: Add/Remove/Move/Split (which high-level maintainer operator this request belongs to).
//   - Config: may omit Span/SchemaID in some corner cases, so we best-effort fill them from resp.Spans.
//
// Strategy:
//   - For Create: ensure a replica set exists (create an absent one if needed) and enqueue an add operator, unless the
//     dispatcher already exists or is already scheduled.
//   - For Remove: ensure a replica set exists and is bound to the reporting node, then enqueue a remove operator.
//     For Move/Split, we only restore the "remove" phase and mark the span absent after removal, letting the basic
//     scheduler trigger follow-up add(s) with up-to-date placement decisions.
func (c *Controller) restoreCurrentWorkingOperators(
	allNodesResp map[node.ID]*heartbeatpb.MaintainerBootstrapResponse,
	tableSplitMap map[int64]bool,
	mode int64,
) error {
	spanController := c.getSpanController(mode)
	for nodeID, resp := range allNodesResp {
		if err := c.restoreCurrentWorkingOperatorsForNode(nodeID, resp, spanController, tableSplitMap, mode); err != nil {
			return err
		}
	}
	return nil
}

// validateBootstrapOperatorRequest extracts and validates the dispatcherID in a bootstrap operator request.
//
// A bootstrap snapshot can contain stale or partial requests. We validate early so we don't create
// maintainer-side tasks/operators from invalid data, and we filter by mode to avoid mixing normal/redo
// scheduling states.
func validateBootstrapOperatorRequest(
	nodeID node.ID,
	resp *heartbeatpb.MaintainerBootstrapResponse,
	req *heartbeatpb.ScheduleDispatcherRequest,
	mode int64,
) (common.DispatcherID, bool) {
	if req == nil || req.Config == nil || req.Config.DispatcherID == nil {
		log.Warn("bootstrap operator config is nil, skip restoring it",
			zap.String("nodeID", nodeID.String()),
			zap.String("changefeed", resp.ChangefeedID.String()))
		return common.DispatcherID{}, false
	}
	dispatcherID := common.NewDispatcherIDFromPB(req.Config.DispatcherID)
	if dispatcherID.IsZero() {
		log.Warn("bootstrap operator has invalid dispatcher id, skip restoring it",
			zap.String("nodeID", nodeID.String()),
			zap.String("changefeed", resp.ChangefeedID.String()))
		return common.DispatcherID{}, false
	}
	if req.Config.Mode != mode {
		return common.DispatcherID{}, false
	}
	return dispatcherID, true
}

// normalizeBootstrapOperatorSpanAndSchemaID fills missing Span/SchemaID fields using the node's span snapshot.
//
// Why:
//   - For Create, the dispatcher manager can persist the request before the dispatcher appears in resp.Spans.
//   - For Remove, maintainer may remove an orphan dispatcher without knowing its exact span/schemaID, so the
//     request can omit them (protobuf default zero values).
//
// Having a concrete span is required to rebuild SpanReplication and to drive correct operator restoration.
func normalizeBootstrapOperatorSpanAndSchemaID(
	nodeID node.ID,
	resp *heartbeatpb.MaintainerBootstrapResponse,
	req *heartbeatpb.ScheduleDispatcherRequest,
	dispatcherID common.DispatcherID,
	spanInfo *heartbeatpb.BootstrapTableSpan,
) (*heartbeatpb.TableSpan, int64, bool) {
	span := req.Config.Span
	schemaID := req.Config.SchemaID
	if spanInfo != nil {
		if span == nil {
			span = spanInfo.Span
		}
		if schemaID == 0 {
			schemaID = spanInfo.SchemaID
		}
	}
	if span == nil {
		log.Warn("bootstrap operator missing span, skip restoring it",
			zap.String("nodeID", nodeID.String()),
			zap.String("changefeed", resp.ChangefeedID.String()),
			zap.String("dispatcherID", dispatcherID.String()),
			zap.String("operatorType", req.OperatorType.String()),
			zap.String("scheduleAction", req.ScheduleAction.String()))
		return nil, 0, false
	}
	return span, schemaID, true
}

func (c *Controller) restoreCurrentWorkingOperatorsForNode(
	nodeID node.ID,
	resp *heartbeatpb.MaintainerBootstrapResponse,
	spanController *span.Controller,
	tableSplitMap map[int64]bool,
	mode int64,
) error {
	spanInfoByID := indexBootstrapSpans(resp.Spans, mode)
	for _, req := range resp.Operators {
		if err := c.restoreCurrentWorkingOperatorForRequest(
			nodeID,
			resp,
			req,
			spanController,
			spanInfoByID,
			tableSplitMap,
			mode,
		); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) restoreCurrentWorkingOperatorForRequest(
	nodeID node.ID,
	resp *heartbeatpb.MaintainerBootstrapResponse,
	req *heartbeatpb.ScheduleDispatcherRequest,
	spanController *span.Controller,
	spanInfoByID map[common.DispatcherID]*heartbeatpb.BootstrapTableSpan,
	tableSplitMap map[int64]bool,
	mode int64,
) error {
	dispatcherID, ok := validateBootstrapOperatorRequest(nodeID, resp, req, mode)
	if !ok {
		return nil
	}

	spanInfo := spanInfoByID[dispatcherID]
	span, schemaID, ok := normalizeBootstrapOperatorSpanAndSchemaID(nodeID, resp, req, dispatcherID, spanInfo)
	if !ok {
		return nil
	}

	// At this point:
	// - span/schemaID are best-effort normalized (using resp.Spans when possible).
	// - replicaSet reflects maintainer's current in-memory task state (may be nil before tasks are rebuilt).
	// - spanInfo reflects runtime snapshot on this node (nil when the dispatcher does not exist on the node).
	replicaSet := spanController.GetTaskByID(dispatcherID)

	switch req.ScheduleAction {
	case heartbeatpb.ScheduleAction_Create:
		return c.restoreCurrentWorkingCreateAction(
			nodeID,
			resp,
			req,
			dispatcherID,
			span,
			schemaID,
			spanInfo,
			spanController,
			replicaSet,
			tableSplitMap,
		)
	case heartbeatpb.ScheduleAction_Remove:
		return c.restoreCurrentWorkingRemoveAction(
			nodeID,
			resp,
			req,
			dispatcherID,
			span,
			spanInfo,
			spanController,
			replicaSet,
			tableSplitMap,
		)
	}
	return nil
}

func (c *Controller) restoreCurrentWorkingCreateAction(
	nodeID node.ID,
	resp *heartbeatpb.MaintainerBootstrapResponse,
	req *heartbeatpb.ScheduleDispatcherRequest,
	dispatcherID common.DispatcherID,
	span *heartbeatpb.TableSpan,
	schemaID int64,
	spanInfo *heartbeatpb.BootstrapTableSpan,
	spanController *span.Controller,
	replicaSet *replica.SpanReplication,
	tableSplitMap map[int64]bool,
) error {
	// Create is only meaningful if the dispatcher does not already exist. If the node snapshot already contains
	// the dispatcher (or maintainer already bound the span), treat it as done.
	if spanInfo != nil {
		log.Debug("dispatcher already exists, skip restoring add operator",
			zap.String("nodeID", nodeID.String()),
			zap.String("changefeed", resp.ChangefeedID.String()),
			zap.String("dispatcherID", dispatcherID.String()))
		return nil
	}
	if replicaSet != nil && replicaSet.IsScheduled() {
		log.Debug("dispatcher already scheduled, skip restoring add operator",
			zap.String("nodeID", nodeID.String()),
			zap.String("changefeed", resp.ChangefeedID.String()),
			zap.String("dispatcherID", dispatcherID.String()))
		return nil
	}
	if replicaSet == nil {
		// Create a new absent replica set for the add operator.
		replicaSet = replica.NewSpanReplication(
			c.changefeedID,
			dispatcherID,
			schemaID,
			span,
			req.Config.StartTs,
			req.Config.Mode,
			spanController.ShouldEnableSplit(tableSplitMap[span.TableID]),
		)
		spanController.AddAbsentReplicaSet(replicaSet)
	}
	return c.handleCurrentWorkingAdd(req, spanController, replicaSet, nodeID, resp)
}

func (c *Controller) restoreCurrentWorkingRemoveAction(
	nodeID node.ID,
	resp *heartbeatpb.MaintainerBootstrapResponse,
	req *heartbeatpb.ScheduleDispatcherRequest,
	dispatcherID common.DispatcherID,
	span *heartbeatpb.TableSpan,
	spanInfo *heartbeatpb.BootstrapTableSpan,
	spanController *span.Controller,
	replicaSet *replica.SpanReplication,
	tableSplitMap map[int64]bool,
) error {
	// For Remove we may not have a replica set if the table was dropped concurrently or if maintainer hasn't
	// rebuilt its table map yet. When possible, reconstruct it from the node snapshot so the remove operator can
	// be completed and cleaned up.
	if replicaSet == nil {
		if spanInfo == nil {
			log.Warn("bootstrap remove operator missing span info, skip restoring it",
				zap.String("nodeID", nodeID.String()),
				zap.String("changefeed", resp.ChangefeedID.String()),
				zap.String("dispatcherID", dispatcherID.String()),
				zap.String("operatorType", req.OperatorType.String()))
			return nil
		}
		replicaSet = c.createSpanReplication(
			spanInfo,
			nodeID,
			spanController.ShouldEnableSplit(tableSplitMap[spanInfo.Span.TableID]),
		)
		spanController.AddReplicatingSpan(replicaSet)
	} else if replicaSet.GetNodeID() == "" {
		// A replica set reconstructed from schema store (during maintainer restart) may not be bound to a node yet,
		// because we haven't processed bootstrap snapshots to learn the actual runtime placement. Bind it so the
		// remove operator sends the request to the correct dispatcher manager.
		replicaSet.SetNodeID(nodeID)
	}

	if err := c.handleCurrentWorkingRemove(req, spanController, replicaSet, nodeID, resp); err != nil {
		return err
	}

	// If the table is already dropped (not present in schema store at startTs), keep the remove operator so the
	// runtime dispatcher can be cleaned up, but remove the task to avoid rescheduling a table that no longer exists.
	if req.OperatorType == heartbeatpb.OperatorType_O_Remove {
		if _, ok := tableSplitMap[span.TableID]; !ok {
			spanController.RemoveReplicatingSpan(replicaSet)
		}
	}
	return nil
}

func (c *Controller) handleCurrentWorkingAdd(
	req *heartbeatpb.ScheduleDispatcherRequest,
	spanController *span.Controller,
	replicaSet *replica.SpanReplication,
	node node.ID,
	resp *heartbeatpb.MaintainerBootstrapResponse,
) error {
	// handleCurrentWorkingAdd translates a bootstrap Create request back into a maintainer-side operator.
	// Dispatcher managers only understand create/remove, while maintainer operators are higher-level:
	// - Add: create dispatcher.
	// - Move/Split: remove + add(+ add...) across nodes/spans.
	// During failover we might only see the current "create" sub-step. Enqueuing an add operator is enough
	// to converge, because its PostFinish/Check logic is idempotent and guarded by span existence checks.
	// Check the original operator of this add operator
	switch req.OperatorType {
	// 1. If the original operator is add, just finish it directly by adding a new add operator.
	// 2. If the original operator is move, which is a remove + add,
	// just finish the add part so that the move operator is done.
	// 3. If the original operator is split, which is a remove + add + add...,
	// same as move, just finish the add part.
	case heartbeatpb.OperatorType_O_Add, heartbeatpb.OperatorType_O_Move, heartbeatpb.OperatorType_O_Split:
		op := operator.NewAddDispatcherOperator(spanController, replicaSet, node, heartbeatpb.OperatorType_O_Add)
		operatorController := c.getOperatorController(req.Config.Mode)
		if ok := operatorController.AddOperator(op); !ok {
			log.Error("add operator failed when dealing current working operators in bootstrap, should not happen",
				zap.String("nodeID", node.String()),
				zap.String("changefeed", resp.ChangefeedID.String()),
				zap.String("dispatcher", op.ID().String()),
				zap.String("originOperatorType", req.OperatorType.String()),
				zap.Any("operator", op),
				zap.Any("replicaSet", replicaSet),
			)
			return errors.ErrOperatorIsNil.GenWithStack("add operator failed when bootstrap")
		}
	}
	return nil
}

func (c *Controller) handleCurrentWorkingRemove(
	req *heartbeatpb.ScheduleDispatcherRequest,
	spanController *span.Controller,
	replicaSet *replica.SpanReplication,
	node node.ID,
	resp *heartbeatpb.MaintainerBootstrapResponse,
) error {
	operatorController := c.getOperatorController(req.Config.Mode)
	// handleCurrentWorkingRemove translates a bootstrap Remove request back into a maintainer-side operator.
	//
	// A Remove request can be:
	// - A standalone remove operator (drop table / drop dispatcher).
	// - The first phase of a Move/Split operator.
	// For Move/Split, restoring only the remove phase is enough: once the old dispatcher is confirmed stopped,
	// the basic scheduler will decide the follow-up add(s) using up-to-date topology and load information.
	// Check the original operator of this remove operator
	switch req.OperatorType {
	// 1. If the original operator is remove, just finish it directly by adding a new remove operator.
	case heartbeatpb.OperatorType_O_Remove:
		op := operator.NewRemoveDispatcherOperator(
			spanController,
			replicaSet,
			heartbeatpb.OperatorType_O_Remove,
			nil,
		)
		if ok := operatorController.AddOperator(op); !ok {
			log.Error("add operator failed when dealing current working operators in bootstrap, should not happen",
				zap.String("nodeID", node.String()),
				zap.String("changefeed", resp.ChangefeedID.String()),
				zap.String("dispatcher", op.ID().String()),
				zap.String("originOperatorType", req.OperatorType.String()),
				zap.Any("operator", op),
				zap.Any("replicaSet", replicaSet),
			)
			return errors.ErrOperatorIsNil.GenWithStack("add operator failed when bootstrap")
		}
	// 2. If the original operator is move or split, which contains a remove part,
	// we just need to finish the first remove part, this is enough to keep the operator consistent.
	// The following add part will be triggered by our basic scheduling logic.
	case heartbeatpb.OperatorType_O_Move, heartbeatpb.OperatorType_O_Split:
		op := operator.NewRemoveDispatcherOperator(
			spanController,
			replicaSet,
			req.OperatorType,
			func() { // post finish
				// Mark the span absent only if it still exists. A concurrent DDL may have already removed it,
				// and we must not reintroduce a ghost entry into spanController.
				if spanController.GetTaskByID(replicaSet.ID) != nil {
					spanController.MarkSpanAbsent(replicaSet)
				}
			},
		)
		if ok := operatorController.AddOperator(op); !ok {
			log.Error("add operator failed when dealing current working operators in bootstrap, should not happen",
				zap.String("nodeID", node.String()),
				zap.String("changefeed", resp.ChangefeedID.String()),
				zap.String("dispatcher", op.ID().String()),
				zap.String("originOperatorType", req.OperatorType.String()),
				zap.Any("operator", op),
				zap.Any("replicaSet", replicaSet),
			)
			return errors.ErrOperatorIsNil.GenWithStack("add operator failed when bootstrap")
		}
	}
	return nil
}
