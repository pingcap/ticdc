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
	"sort"
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
	"github.com/pingcap/ticdc/pkg/routing"
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
	startTs, redoStartTs, err := c.determineStartTs(allNodesResp)
	if err != nil {
		log.Error("can not determine the startTs from the bootstrap response",
			zap.String("changefeed", c.changefeedID.Name()), zap.Error(err))
		return nil, errors.Trace(err)
	}

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

	// Restore merge operators after task state is rebuilt from bootstrap spans/operators.
	// Merge restoration needs the per-dispatcher task map from buildTaskInfo, but must run
	// before we discard any leftover working tasks as dropped-table artifacts.
	mergeTableSplitMaps := map[int64]map[int64]bool{
		common.DefaultMode: buildTableSplitMap(tables),
	}
	if c.enableRedo {
		mergeTableSplitMaps[common.RedoMode] = buildTableSplitMap(redoTables)
	}
	if err := c.restoreCurrentMergeOperators(allNodesResp, mergeTableSplitMaps); err != nil {
		return nil, errors.Trace(err)
	}

	// Step 4: Handle any remaining working tasks (likely dropped tables)
	c.handleRemainingWorkingTasks(workingTaskMap, redoWorkingTaskMap)

	// Step 5: Initialize route admission before barrier starts handling bootstrap
	// block states. The barrier captures the route admin pointer at construction time.
	admin, err := routing.NewAdmin(c.changefeedID, c.replicaConfig, c.reportError, tables)
	if err != nil {
		return nil, err
	}
	c.routeAdmin = admin

	c.initializeComponents(allNodesResp)

	c.bootstrapped = true

	return &heartbeatpb.MaintainerPostBootstrapRequest{
		ChangefeedID:                  c.changefeedID.ToPB(),
		TableTriggerEventDispatcherId: c.spanController.GetDDLDispatcherID().ToPB(),
		Schemas:                       c.prepareSchemaInfoResponse(schemaInfos),
		RedoSchemas:                   c.prepareSchemaInfoResponse(redoSchemaInfos),
	}, nil
}

func (c *Controller) determineStartTs(allNodesResp map[node.ID]*heartbeatpb.MaintainerBootstrapResponse) (uint64, uint64, error) {
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
		return 0, 0, errors.WrapError(
			errors.ErrChangefeedInitTableTriggerDispatcherFailed,
			errors.New("all bootstrap responses reported empty checkpointTs"),
		)
	}
	if c.enableRedo && redoStartTs == 0 {
		return 0, 0, errors.WrapError(
			errors.ErrChangefeedInitTableTriggerDispatcherFailed,
			errors.New("all bootstrap responses reported empty redoCheckpointTs"),
		)
	}
	return startTs, redoStartTs, nil
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
			if spanInfo == nil || spanInfo.ID == nil || spanInfo.Span == nil || spanInfo.Mode != mode {
				continue
			}
			dispatcherID := common.NewDispatcherIDFromPB(spanInfo.ID)
			if dispatcherID.IsZero() || spanController.IsDDLDispatcher(dispatcherID) {
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
			schemaInfos[schemaID] = getSchemaInfo(table, isMysqlCompatibleBackend, util.GetOrZero(c.replicaConfig.EnableActiveActive))
		}

		// Add table info to schema
		tableInfo := getTableInfo(table, isMysqlCompatibleBackend, util.GetOrZero(c.replicaConfig.EnableActiveActive))
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
	isTableSpanExists := len(replicaSets) > 0
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
				tableSpans = utils.NewBtreeMap[*heartbeatpb.TableSpan, *replica.SpanReplication](lessBootstrapTableSpan)
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
	tableSplitMap := buildTableSplitMap(tables)
	workingTaskMap := c.buildWorkingTaskMap(allNodesResp, tableSplitMap, mode)
	// Restore current working operators first so spanController reflects "in-flight scheduling intent"
	// captured by dispatcher managers. This avoids bootstrap creating duplicate tasks for a dispatcherID
	// that is already being created/removed by a previous maintainer instance.
	if err := c.restoreCurrentWorkingOperators(allNodesResp, tableSplitMap, mode); err != nil {
		return nil, nil, err
	}
	c.removeUnexpectedBootstrapOverlaps(workingTaskMap, allNodesResp, mode)
	schemaInfos := c.processTablesAndBuildSchemaInfo(tables, workingTaskMap, isMysqlCompatibleBackend, mode)
	return workingTaskMap, schemaInfos, nil
}

func buildTableSplitMap(tables []commonEvent.Table) map[int64]bool {
	tableSplitMap := make(map[int64]bool, len(tables))
	for _, tbl := range tables {
		tableSplitMap[tbl.TableID] = tbl.Splitable
	}
	return tableSplitMap
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
		c.redoBarrier = NewBarrier(c.redoSpanController, c.redoOperatorController, util.GetOrZero(c.replicaConfig.Scheduler.EnableTableAcrossNodes), allNodesResp, common.RedoMode, nil)
	}
	c.barrier = NewBarrier(c.spanController, c.operatorController, util.GetOrZero(c.replicaConfig.Scheduler.EnableTableAcrossNodes), allNodesResp, common.DefaultMode, c.routeAdmin)

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

func getSchemaInfo(table commonEvent.Table, isMysqlCompatibleBackend bool, enableActiveActive bool) *heartbeatpb.SchemaInfo {
	schemaInfo := &heartbeatpb.SchemaInfo{}
	schemaInfo.SchemaID = table.SchemaID
	if commonEvent.NeedTableNameStoreAndCheckpointTs(isMysqlCompatibleBackend, enableActiveActive) {
		schemaInfo.SchemaName = table.SchemaName
	}
	return schemaInfo
}

func getTableInfo(table commonEvent.Table, isMysqlCompatibleBackend bool, enableActiveActive bool) *heartbeatpb.TableInfo {
	tableInfo := &heartbeatpb.TableInfo{}
	tableInfo.TableID = table.TableID
	if commonEvent.NeedTableNameStoreAndCheckpointTs(isMysqlCompatibleBackend, enableActiveActive) {
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
		tableSpans = utils.NewBtreeMap[*heartbeatpb.TableSpan, *replica.SpanReplication](lessBootstrapTableSpan)
		workingTaskMap[span.TableID] = tableSpans
	}
	tableSpans.ReplaceOrInsert(span, spanReplication)
}

// lessBootstrapTableSpan keeps spans with the same start key but different end keys distinct.
// Merge bootstrap snapshots can contain a source span and its covering merged span with the same
// start key, so the normal start-key-only comparator would silently replace one of them.
func lessBootstrapTableSpan(left, right *heartbeatpb.TableSpan) bool {
	if left.TableID != right.TableID {
		return left.TableID < right.TableID
	}
	if cmp := bytes.Compare(left.StartKey, right.StartKey); cmp != 0 {
		return cmp < 0
	}
	if cmp := bytes.Compare(left.EndKey, right.EndKey); cmp != 0 {
		// Put the narrower span first. If a merge target is not backed by valid merge evidence,
		// preserving the source topology is safer than accepting an abortable covering target.
		return cmp < 0
	}
	return left.KeyspaceID < right.KeyspaceID
}

type bootstrapMergeEvidence struct {
	sourceToTargets map[common.DispatcherID]map[common.DispatcherID]struct{}
}

func buildBootstrapMergeEvidence(
	allNodesResp map[node.ID]*heartbeatpb.MaintainerBootstrapResponse,
	mode int64,
) bootstrapMergeEvidence {
	evidence := bootstrapMergeEvidence{
		sourceToTargets: make(map[common.DispatcherID]map[common.DispatcherID]struct{}),
	}
	for _, resp := range allNodesResp {
		if resp == nil {
			continue
		}
		spanInfoByID := indexBootstrapSpans(resp.Spans, mode)
		for _, mergeReq := range resp.MergeOperators {
			if mergeReq == nil || mergeReq.MergedDispatcherID == nil || mergeReq.Mode != mode {
				continue
			}
			mergedID := common.NewDispatcherIDFromPB(mergeReq.MergedDispatcherID)
			if mergedID.IsZero() {
				continue
			}
			mergedSpanInfo := spanInfoByID[mergedID]
			if mergedSpanInfo == nil || mergedSpanInfo.Span == nil ||
				!isMergeTargetState(mergedSpanInfo.ComponentStatus) {
				continue
			}

			sourceIDs := make(map[common.DispatcherID]struct{}, len(mergeReq.DispatcherIDs))
			sourceSpans := make([]*heartbeatpb.TableSpan, 0, len(mergeReq.DispatcherIDs))
			valid := true
			for _, idPB := range mergeReq.DispatcherIDs {
				if idPB == nil {
					valid = false
					break
				}
				dispatcherID := common.NewDispatcherIDFromPB(idPB)
				if dispatcherID.IsZero() || dispatcherID == mergedID {
					valid = false
					break
				}
				if _, ok := sourceIDs[dispatcherID]; ok {
					continue
				}
				spanInfo := spanInfoByID[dispatcherID]
				if spanInfo == nil || spanInfo.Span == nil ||
					spanInfo.ComponentStatus != heartbeatpb.ComponentState_WaitingMerge ||
					spanInfo.Span.TableID != mergedSpanInfo.Span.TableID ||
					spanInfo.Span.KeyspaceID != mergedSpanInfo.Span.KeyspaceID {
					valid = false
					break
				}
				sourceIDs[dispatcherID] = struct{}{}
				sourceSpans = append(sourceSpans, spanInfo.Span)
			}
			if !valid || len(sourceIDs) < 2 {
				continue
			}
			sort.Slice(sourceSpans, func(i, j int) bool {
				return bytes.Compare(sourceSpans[i].StartKey, sourceSpans[j].StartKey) < 0
			})
			if !bytes.Equal(sourceSpans[0].StartKey, mergedSpanInfo.Span.StartKey) ||
				!bytes.Equal(sourceSpans[len(sourceSpans)-1].EndKey, mergedSpanInfo.Span.EndKey) {
				continue
			}
			for i := 1; i < len(sourceSpans); i++ {
				if !common.IsTableSpanConsecutive(sourceSpans[i-1], sourceSpans[i]) {
					valid = false
					break
				}
			}
			if !valid {
				continue
			}

			for dispatcherID := range sourceIDs {
				addBootstrapMergeEvidence(evidence, dispatcherID, mergedID)
			}
		}
	}
	return evidence
}

func addBootstrapMergeEvidence(
	evidence bootstrapMergeEvidence,
	dispatcherID common.DispatcherID,
	mergeID common.DispatcherID,
) {
	targets, ok := evidence.sourceToTargets[dispatcherID]
	if !ok {
		targets = make(map[common.DispatcherID]struct{})
		evidence.sourceToTargets[dispatcherID] = targets
	}
	targets[mergeID] = struct{}{}
}

func (e bootstrapMergeEvidence) relates(left, right common.DispatcherID) bool {
	if _, ok := e.sourceToTargets[left][right]; ok {
		return true
	}
	_, ok := e.sourceToTargets[right][left]
	return ok
}

func spansOverlap(left, right *heartbeatpb.TableSpan) bool {
	if left == nil || right == nil ||
		left.TableID != right.TableID ||
		left.KeyspaceID != right.KeyspaceID {
		return false
	}
	return bytes.Compare(left.StartKey, right.EndKey) < 0 &&
		bytes.Compare(right.StartKey, left.EndKey) < 0
}

func isMergeTargetState(status heartbeatpb.ComponentState) bool {
	switch status {
	case heartbeatpb.ComponentState_Preparing,
		heartbeatpb.ComponentState_MergeReady,
		heartbeatpb.ComponentState_Initializing,
		heartbeatpb.ComponentState_Working:
		return true
	default:
		return false
	}
}

func isCommittedMergeTargetState(status heartbeatpb.ComponentState) bool {
	return status == heartbeatpb.ComponentState_Initializing ||
		status == heartbeatpb.ComponentState_Working
}

func isWaitingMergeSpanCoveredByTarget(
	source, target *replica.SpanReplication,
) bool {
	if source == nil || target == nil || source.Span == nil || target.Span == nil {
		return false
	}
	sourceStatus := source.GetStatus()
	targetStatus := target.GetStatus()
	if sourceStatus == nil || targetStatus == nil ||
		sourceStatus.ComponentStatus != heartbeatpb.ComponentState_WaitingMerge ||
		!isCommittedMergeTargetState(targetStatus.ComponentStatus) {
		return false
	}
	return source.GetMode() == target.GetMode() &&
		source.Span.TableID == target.Span.TableID &&
		source.Span.KeyspaceID == target.Span.KeyspaceID &&
		bytes.Compare(target.Span.StartKey, source.Span.StartKey) <= 0 &&
		bytes.Compare(target.Span.EndKey, source.Span.EndKey) >= 0
}

func isExpectedBootstrapOverlap(
	left, right *replica.SpanReplication,
	evidence bootstrapMergeEvidence,
) bool {
	if evidence.relates(left.ID, right.ID) {
		return true
	}
	return isWaitingMergeSpanCoveredByTarget(left, right) ||
		isWaitingMergeSpanCoveredByTarget(right, left)
}

// removeUnexpectedBootstrapOverlaps gives every unproven overlap a cleanup owner. The rejected
// runtime span remains temporary coverage until removal is confirmed, so bootstrap neither trusts
// it as permanent desired state nor schedules a replacement that can race with it.
func (c *Controller) removeUnexpectedBootstrapOverlaps(
	workingTaskMap map[int64]utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication],
	allNodesResp map[node.ID]*heartbeatpb.MaintainerBootstrapResponse,
	mode int64,
) {
	evidence := buildBootstrapMergeEvidence(allNodesResp, mode)
	operatorController := c.getOperatorController(mode)
	for tableID, tableSpans := range workingTaskMap {
		candidates := make([]*replica.SpanReplication, 0, tableSpans.Len())
		tableSpans.Ascend(func(_ *heartbeatpb.TableSpan, candidate *replica.SpanReplication) bool {
			candidates = append(candidates, candidate)
			return true
		})
		sort.SliceStable(candidates, func(i, j int) bool {
			leftStatus := candidates[i].GetStatus()
			rightStatus := candidates[j].GetStatus()
			leftAbortableTarget := leftStatus != nil &&
				(leftStatus.ComponentStatus == heartbeatpb.ComponentState_Preparing ||
					leftStatus.ComponentStatus == heartbeatpb.ComponentState_MergeReady)
			rightAbortableTarget := rightStatus != nil &&
				(rightStatus.ComponentStatus == heartbeatpb.ComponentState_Preparing ||
					rightStatus.ComponentStatus == heartbeatpb.ComponentState_MergeReady)
			if leftAbortableTarget != rightAbortableTarget {
				// Sources survive when an unvalidated merge is still abortable.
				return !leftAbortableTarget
			}
			return lessBootstrapTableSpan(candidates[i].Span, candidates[j].Span)
		})

		accepted := make([]*replica.SpanReplication, 0, tableSpans.Len())
		for _, candidate := range candidates {
			rejected := false
			for _, existing := range accepted {
				if !spansOverlap(existing.Span, candidate.Span) {
					continue
				}
				// Ordinary bootstrap operators also prove that the overlap is already converging.
				if operatorController.GetOperator(existing.ID) != nil ||
					operatorController.GetOperator(candidate.ID) != nil ||
					isExpectedBootstrapOverlap(existing, candidate, evidence) {
					continue
				}

				log.Warn("remove unexpected overlapping dispatcher reported during bootstrap",
					zap.Stringer("changefeed", c.changefeedID),
					zap.Int64("tableID", tableID),
					zap.String("keptDispatcherID", existing.ID.String()),
					zap.String("removedDispatcherID", candidate.ID.String()),
					zap.String("keptSpan", common.FormatTableSpan(existing.Span)),
					zap.String("removedSpan", common.FormatTableSpan(candidate.Span)),
					zap.Int64("mode", mode))
				c.addBootstrapCleanupOperator(candidate)
				rejected = true
				break
			}
			if !rejected {
				accepted = append(accepted, candidate)
			}
		}
	}
}

// addBootstrapCleanupOperator tracks a runtime span until its remove reaches a terminal state.
func (c *Controller) addBootstrapCleanupOperator(replicaSet *replica.SpanReplication) bool {
	if replicaSet == nil || replicaSet.ID.IsZero() || replicaSet.GetNodeID() == "" {
		return false
	}
	spanController := c.getSpanController(replicaSet.GetMode())
	operatorController := c.getOperatorController(replicaSet.GetMode())
	if spanController == nil || operatorController == nil {
		return false
	}
	if spanController.GetTaskByID(replicaSet.ID) == nil {
		// Keep the runtime dispatcher as temporary coverage while its remove operator is active.
		// This prevents the scheduler from creating a replacement before removal is confirmed.
		spanController.AddReplicatingSpan(replicaSet)
	}
	op := operator.NewRemoveDispatcherOperator(
		spanController,
		replicaSet,
		heartbeatpb.OperatorType_O_Remove,
		func() {
			spanController.RemoveReplicatingSpan(replicaSet)
			c.repairBootstrapTableCoverage(replicaSet)
		},
	)
	if !operatorController.AddOperator(op) {
		log.Warn("failed to add cleanup operator for unexpected bootstrap dispatcher",
			zap.Stringer("changefeed", c.changefeedID),
			zap.String("dispatcherID", replicaSet.ID.String()),
			zap.String("nodeID", replicaSet.GetNodeID().String()),
			zap.Int64("mode", replicaSet.GetMode()))
		return false
	}
	return true
}

// repairBootstrapTableCoverage creates absent spans only after temporary runtime coverage is gone.
func (c *Controller) repairBootstrapTableCoverage(removedReplicaSet *replica.SpanReplication) {
	if removedReplicaSet == nil || removedReplicaSet.Span == nil {
		return
	}
	spanController := c.getSpanController(removedReplicaSet.GetMode())
	if spanController == nil {
		return
	}
	currentSpans := utils.NewBtreeMap[*heartbeatpb.TableSpan, *replica.SpanReplication](lessBootstrapTableSpan)
	for _, replicaSet := range spanController.GetTasksByTableID(removedReplicaSet.Span.TableID) {
		if replicaSet == nil || replicaSet.Span == nil {
			continue
		}
		currentSpans.ReplaceOrInsert(replicaSet.Span, replicaSet)
	}
	totalSpan := common.TableIDToComparableSpan(
		removedReplicaSet.Span.KeyspaceID,
		removedReplicaSet.Span.TableID,
	)
	holes := findHoles(currentSpans, &heartbeatpb.TableSpan{
		TableID:    removedReplicaSet.Span.TableID,
		StartKey:   totalSpan.StartKey,
		EndKey:     totalSpan.EndKey,
		KeyspaceID: removedReplicaSet.Span.KeyspaceID,
	})
	spanController.AddNewSpans(
		removedReplicaSet.GetSchemaID(),
		holes,
		c.startTs,
		removedReplicaSet.IsSplitEnabled(),
	)
}

// findHoles returns the uncovered sub-spans in totalSpan.
//
// Bootstrap snapshots can temporarily contain overlapping spans during in-flight merge recovery:
// for example, source dispatchers in WaitingMerge can coexist with a merged dispatcher in
// Preparing/Initializing. We therefore compute holes from the union of reported coverage instead
// of assuming the input spans are strictly non-overlapping.
func findHoles(currentSpan utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication], totalSpan *heartbeatpb.TableSpan) []*heartbeatpb.TableSpan {
	coveredEnd := totalSpan.StartKey
	var holes []*heartbeatpb.TableSpan
	// table span is sorted
	currentSpan.Ascend(func(current *heartbeatpb.TableSpan, _ *replica.SpanReplication) bool {
		// Skip spans that are fully covered by earlier overlaps. This preserves complete table
		// coverage without manufacturing holes for legitimate bootstrap overlap.
		if bytes.Compare(current.EndKey, coveredEnd) <= 0 {
			return true
		}
		if bytes.Compare(coveredEnd, current.StartKey) < 0 {
			// Find a hole.
			holes = append(holes, &heartbeatpb.TableSpan{
				TableID:    totalSpan.TableID,
				StartKey:   coveredEnd,
				EndKey:     current.StartKey,
				KeyspaceID: totalSpan.KeyspaceID,
			})
		}
		coveredEnd = current.EndKey
		return true
	})
	// Check if there is a hole in the end.
	// coveredEnd not reach the totalSpan end
	if !bytes.Equal(coveredEnd, totalSpan.EndKey) {
		holes = append(holes, &heartbeatpb.TableSpan{
			TableID:    totalSpan.TableID,
			StartKey:   coveredEnd,
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
	splitable, tableExists := tableSplitMap[span.TableID]
	if !tableExists {
		// The bootstrap schema-store snapshot is already taken at startTs. If the table is absent there,
		// this create request is stale (for example, add/move/split was in-flight before a DROP TABLE) and
		// restoring it would recreate a ghost task/operator for a table that should stay removed.
		log.Warn("bootstrap create operator references table missing from schema snapshot, skip restoring it",
			zap.String("nodeID", nodeID.String()),
			zap.String("changefeed", resp.ChangefeedID.String()),
			zap.String("dispatcherID", dispatcherID.String()),
			zap.String("operatorType", req.OperatorType.String()),
			zap.Int64("tableID", span.TableID))
		return nil
	}

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
			spanController.ShouldEnableSplit(splitable),
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

// restoreCurrentMergeOperators rebuilds maintainer-side merge operators from bootstrap snapshots.
//
// Dispatcher managers persist in-flight merge requests independently from create/remove scheduling requests.
// After a maintainer failover, we restore those merge requests so source spans keep converging instead of
// remaining stuck in scheduling state or leaking an incomplete merged dispatcher.
func (c *Controller) restoreCurrentMergeOperators(
	allNodesResp map[node.ID]*heartbeatpb.MaintainerBootstrapResponse,
	tableSplitMaps map[int64]map[int64]bool,
) error {
	for nodeID, resp := range allNodesResp {
		if len(resp.MergeOperators) == 0 {
			continue
		}

		for _, mergeReq := range resp.MergeOperators {
			if mergeReq == nil || mergeReq.MergedDispatcherID == nil {
				continue
			}
			if len(mergeReq.DispatcherIDs) < 2 {
				log.Warn("merge operator has insufficient dispatcher IDs",
					zap.String("nodeID", nodeID.String()),
					zap.String("changefeed", resp.ChangefeedID.String()))
				continue
			}

			tableSplitMap, ok := tableSplitMapForMode(mergeReq.Mode, tableSplitMaps)
			if !ok {
				log.Warn("skip restoring merge operator due to unavailable mode",
					zap.String("nodeID", nodeID.String()),
					zap.String("changefeed", resp.ChangefeedID.String()),
					zap.Int64("mode", mergeReq.Mode))
				continue
			}
			spanController := c.getSpanController(mergeReq.Mode)
			operatorController := c.getOperatorController(mergeReq.Mode)
			if spanController == nil || operatorController == nil {
				log.Warn("skip restoring merge operator due to uninitialized controller",
					zap.String("nodeID", nodeID.String()),
					zap.String("changefeed", resp.ChangefeedID.String()),
					zap.Int64("mode", mergeReq.Mode))
				continue
			}
			spanInfoByID := indexBootstrapSpans(resp.Spans, mergeReq.Mode)
			mergedDispatcherID := common.NewDispatcherIDFromPB(mergeReq.MergedDispatcherID)
			if mergedDispatcherID.IsZero() {
				log.Warn("skip restoring merge operator due to invalid merged dispatcher ID",
					zap.String("nodeID", nodeID.String()),
					zap.String("changefeed", resp.ChangefeedID.String()),
					zap.Int64("mode", mergeReq.Mode))
				continue
			}

			sourceReplicaSets := make([]*replica.SpanReplication, 0, len(mergeReq.DispatcherIDs))
			createdSourceReplicaSets := make([]*replica.SpanReplication, 0, len(mergeReq.DispatcherIDs))
			cleanupCreatedSourceReplicaSets := func() {
				for _, replicaSet := range createdSourceReplicaSets {
					spanController.RemoveReplicatingSpan(replicaSet)
				}
			}
			sourceComplete := true
			skipMerge := false
			seenSources := make(map[common.DispatcherID]struct{}, len(mergeReq.DispatcherIDs))
			for _, idPB := range mergeReq.DispatcherIDs {
				if idPB == nil {
					sourceComplete = false
					break
				}
				dispatcherID := common.NewDispatcherIDFromPB(idPB)
				if dispatcherID.IsZero() {
					sourceComplete = false
					break
				}
				if _, ok := seenSources[dispatcherID]; ok {
					continue
				}
				seenSources[dispatcherID] = struct{}{}

				replicaSet := spanController.GetTaskByID(dispatcherID)
				if replicaSet == nil {
					spanInfo := spanInfoByID[dispatcherID]
					if spanInfo == nil || spanInfo.Span == nil {
						sourceComplete = false
						break
					}
					splitable, tableExists := tableSplitMap[spanInfo.Span.TableID]
					if !tableExists {
						log.Warn("skip restoring merge operator because source table is missing from schema snapshot",
							zap.String("nodeID", nodeID.String()),
							zap.String("changefeed", resp.ChangefeedID.String()),
							zap.String("dispatcher", dispatcherID.String()),
							zap.Int64("tableID", spanInfo.Span.TableID),
							zap.Int64("mode", mergeReq.Mode))
						skipMerge = true
						break
					}
					splitEnabled := spanController.ShouldEnableSplit(splitable)
					replicaSet = c.createSpanReplication(spanInfo, nodeID, splitEnabled)
					spanController.AddReplicatingSpan(replicaSet)
					createdSourceReplicaSets = append(createdSourceReplicaSets, replicaSet)
				} else if replicaSet.Span == nil {
					sourceComplete = false
					break
				} else if _, tableExists := tableSplitMap[replicaSet.Span.TableID]; !tableExists {
					log.Warn("skip restoring merge operator because source table is missing from schema snapshot",
						zap.String("nodeID", nodeID.String()),
						zap.String("changefeed", resp.ChangefeedID.String()),
						zap.String("dispatcher", dispatcherID.String()),
						zap.Int64("tableID", replicaSet.Span.TableID),
						zap.Int64("mode", mergeReq.Mode))
					skipMerge = true
					break
				}
				sourceReplicaSets = append(sourceReplicaSets, replicaSet)
			}
			if skipMerge {
				cleanupCreatedSourceReplicaSets()
				continue
			}

			mergedSpanInfo := spanInfoByID[mergedDispatcherID]
			mergedReplicaSet := spanController.GetTaskByID(mergedDispatcherID)
			if mergedSpanInfo != nil {
				if mergedSpanInfo.Span == nil {
					log.Warn("skip restoring merge operator because merged span is missing",
						zap.String("nodeID", nodeID.String()),
						zap.String("changefeed", resp.ChangefeedID.String()),
						zap.String("dispatcher", mergedDispatcherID.String()),
						zap.Int64("mode", mergeReq.Mode))
					cleanupCreatedSourceReplicaSets()
					continue
				}
				if _, tableExists := tableSplitMap[mergedSpanInfo.Span.TableID]; !tableExists {
					log.Warn("skip restoring merge operator because merged table is missing from schema snapshot",
						zap.String("nodeID", nodeID.String()),
						zap.String("changefeed", resp.ChangefeedID.String()),
						zap.String("dispatcher", mergedDispatcherID.String()),
						zap.Int64("tableID", mergedSpanInfo.Span.TableID),
						zap.Int64("mode", mergeReq.Mode))
					cleanupCreatedSourceReplicaSets()
					continue
				}
			}
			if mergedReplicaSet != nil && mergedReplicaSet.Span != nil {
				if _, tableExists := tableSplitMap[mergedReplicaSet.Span.TableID]; !tableExists {
					log.Warn("skip restoring merge operator because merged table is missing from schema snapshot",
						zap.String("nodeID", nodeID.String()),
						zap.String("changefeed", resp.ChangefeedID.String()),
						zap.String("dispatcher", mergedDispatcherID.String()),
						zap.Int64("tableID", mergedReplicaSet.Span.TableID),
						zap.Int64("mode", mergeReq.Mode))
					cleanupCreatedSourceReplicaSets()
					continue
				}
			}

			conflictingDispatcherID, conflictingOperatorType, hasConflict :=
				findBootstrapMergeOperatorConflict(operatorController, sourceReplicaSets, mergedDispatcherID)
			if hasConflict {
				// Regular create/remove recovery has higher priority than a merge journal because it
				// represents a later concrete action for the same dispatcher. Treat the merge record
				// as stale instead of failing the entire bootstrap.
				log.Warn("skip restoring stale merge operator due to conflicting bootstrap operator",
					zap.String("nodeID", nodeID.String()),
					zap.String("changefeed", resp.ChangefeedID.String()),
					zap.String("dispatcher", conflictingDispatcherID.String()),
					zap.String("operatorType", conflictingOperatorType),
					zap.String("mergedDispatcher", mergedDispatcherID.String()),
					zap.Int64("mode", mergeReq.Mode))
				c.reconcileUnrestorableBootstrapMerge(
					mergedReplicaSet,
					mergedSpanInfo,
					sourceReplicaSets,
				)
				continue
			}

			if mergedSpanInfo != nil && mergedSpanInfo.ComponentStatus == heartbeatpb.ComponentState_Working {
				if mergedReplicaSet == nil {
					splitEnabled := spanController.ShouldEnableSplit(tableSplitMap[mergedSpanInfo.Span.TableID])
					mergedReplicaSet = c.createSpanReplication(mergedSpanInfo, nodeID, splitEnabled)
					spanController.AddReplicatingSpan(mergedReplicaSet)
				}
				for _, replicaSet := range sourceReplicaSets {
					if mergedReplicaSet != nil && replicaSet.ID == mergedReplicaSet.ID {
						continue
					}
					spanController.RemoveReplicatingSpan(replicaSet)
				}
				if mergedReplicaSet != nil {
					spanController.MarkSpanReplicating(mergedReplicaSet)
				}
				log.Info("merge already finished during bootstrap",
					zap.String("nodeID", nodeID.String()),
					zap.String("changefeed", resp.ChangefeedID.String()),
					zap.String("dispatcher", mergedDispatcherID.String()))
				continue
			}

			if !sourceComplete || len(sourceReplicaSets) < 2 {
				log.Warn("skip restoring merge operator due to missing source dispatchers",
					zap.String("nodeID", nodeID.String()),
					zap.String("changefeed", resp.ChangefeedID.String()),
					zap.String("dispatcher", mergedDispatcherID.String()))
				if mergedSpanInfo != nil &&
					isCommittedMergeTargetState(mergedSpanInfo.ComponentStatus) {
					if mergedReplicaSet == nil {
						splitEnabled := spanController.ShouldEnableSplit(tableSplitMap[mergedSpanInfo.Span.TableID])
						mergedReplicaSet = c.createSpanReplication(mergedSpanInfo, nodeID, splitEnabled)
						spanController.AddReplicatingSpan(mergedReplicaSet)
					} else {
						spanController.MarkSpanReplicating(mergedReplicaSet)
					}
					// Initializing is the merge commit point: the target has a real startTs and sources
					// are being removed. Keep the committed target and ignore late source terminals.
					for _, replicaSet := range sourceReplicaSets {
						if replicaSet.ID == mergedReplicaSet.ID {
							continue
						}
						spanController.RemoveReplicatingSpan(replicaSet)
					}
					log.Info("continue merge from existing merged dispatcher after source loss",
						zap.String("nodeID", nodeID.String()),
						zap.String("changefeed", resp.ChangefeedID.String()),
						zap.String("dispatcher", mergedDispatcherID.String()),
						zap.Any("status", mergedSpanInfo.ComponentStatus))
					continue
				}
				if mergedReplicaSet != nil {
					// Preparing and MergeReady are still abortable. Dispatcher manager restores surviving
					// sources and removes the target when any source is missing, so maintainer must not
					// promote the target into permanent desired state. Keep it only as temporary coverage
					// until the cleanup operator observes a terminal status.
					c.addBootstrapCleanupOperator(mergedReplicaSet)
					targetStatus, _ := bootstrapMergeTargetStatus(mergedSpanInfo, mergedReplicaSet)
					log.Info("abort incomplete merge from uncommitted merged dispatcher",
						zap.String("nodeID", nodeID.String()),
						zap.String("changefeed", resp.ChangefeedID.String()),
						zap.String("dispatcher", mergedDispatcherID.String()),
						zap.Any("status", targetStatus))
					continue
				}
				// Without a merged dispatcher snapshot, a merge journal with missing sources is stale
				// and cannot be driven by any restored operator. Leave the surviving source spans in
				// their bootstrap state so normal scheduling can repair any uncovered table ranges.
				cleanupCreatedSourceReplicaSets()
				continue
			}

			sort.Slice(sourceReplicaSets, func(i, j int) bool {
				return bytes.Compare(sourceReplicaSets[i].Span.StartKey, sourceReplicaSets[j].Span.StartKey) < 0
			})

			if mergedReplicaSet == nil {
				if mergedSpanInfo != nil {
					splitEnabled := spanController.ShouldEnableSplit(tableSplitMap[mergedSpanInfo.Span.TableID])
					mergedReplicaSet = c.createSpanReplication(mergedSpanInfo, nodeID, splitEnabled)
					spanController.AddSchedulingReplicaSet(mergedReplicaSet, nodeID)
				} else {
					mergedSpan, schemaID, checkpointTs, ok := buildMergedSpanFromReplicas(sourceReplicaSets)
					if !ok {
						log.Warn("skip restoring merge operator due to invalid merge spans",
							zap.String("nodeID", nodeID.String()),
							zap.String("changefeed", resp.ChangefeedID.String()),
							zap.String("dispatcher", mergedDispatcherID.String()))
						cleanupCreatedSourceReplicaSets()
						continue
					}
					splitEnabled := spanController.ShouldEnableSplit(tableSplitMap[mergedSpan.TableID])
					status := &heartbeatpb.TableSpanStatus{
						ID:              mergedDispatcherID.ToPB(),
						CheckpointTs:    checkpointTs,
						Mode:            mergeReq.Mode,
						ComponentStatus: heartbeatpb.ComponentState_Preparing,
					}
					mergedReplicaSet = replica.NewWorkingSpanReplication(
						c.changefeedID,
						mergedDispatcherID,
						schemaID,
						mergedSpan,
						status,
						nodeID,
						splitEnabled,
					)
					spanController.AddSchedulingReplicaSet(mergedReplicaSet, nodeID)
				}
			}

			if mergedReplicaSet == nil {
				log.Warn("merge replica set not found when restoring merge",
					zap.String("nodeID", nodeID.String()),
					zap.String("changefeed", resp.ChangefeedID.String()),
					zap.String("dispatcher", mergedDispatcherID.String()))
				continue
			}

			mergeOp := operatorController.AddRestoredMergeOperator(sourceReplicaSets, mergedReplicaSet)
			if mergeOp == nil {
				log.Warn("skip stale merge operator that cannot be restored",
					zap.String("nodeID", nodeID.String()),
					zap.String("changefeed", resp.ChangefeedID.String()),
					zap.String("dispatcher", mergedDispatcherID.String()),
					zap.Any("mergedReplicaSet", mergedReplicaSet),
					zap.Any("toMergedReplicaSets", sourceReplicaSets))
				c.reconcileUnrestorableBootstrapMerge(
					mergedReplicaSet,
					mergedSpanInfo,
					sourceReplicaSets,
				)
				continue
			}
			spanController.MarkSpanScheduling(mergedReplicaSet)
			mergeOp.Start()
		}
	}
	return nil
}

// bootstrapMergeTargetStatus reads the most concrete target state available during restoration.
func bootstrapMergeTargetStatus(
	mergedSpanInfo *heartbeatpb.BootstrapTableSpan,
	mergedReplicaSet *replica.SpanReplication,
) (heartbeatpb.ComponentState, bool) {
	if mergedSpanInfo != nil {
		return mergedSpanInfo.ComponentStatus, true
	}
	if mergedReplicaSet != nil && mergedReplicaSet.GetStatus() != nil {
		return mergedReplicaSet.GetStatus().ComponentStatus, true
	}
	return heartbeatpb.ComponentState_Working, false
}

// reconcileUnrestorableBootstrapMerge selects the survivor according to the merge commit point.
func (c *Controller) reconcileUnrestorableBootstrapMerge(
	mergedReplicaSet *replica.SpanReplication,
	mergedSpanInfo *heartbeatpb.BootstrapTableSpan,
	sourceReplicaSets []*replica.SpanReplication,
) {
	if mergedReplicaSet == nil {
		return
	}
	targetStatus, hasStatus := bootstrapMergeTargetStatus(mergedSpanInfo, mergedReplicaSet)
	spanController := c.getSpanController(mergedReplicaSet.GetMode())
	if spanController == nil {
		return
	}
	if hasStatus && isCommittedMergeTargetState(targetStatus) {
		if spanController.GetTaskByID(mergedReplicaSet.ID) == nil {
			spanController.AddReplicatingSpan(mergedReplicaSet)
		} else {
			spanController.MarkSpanReplicating(mergedReplicaSet)
		}
		for _, replicaSet := range sourceReplicaSets {
			if replicaSet == nil || replicaSet.ID == mergedReplicaSet.ID {
				continue
			}
			spanController.RemoveReplicatingSpan(replicaSet)
		}
		return
	}
	c.addBootstrapCleanupOperator(mergedReplicaSet)
}

func findBootstrapMergeOperatorConflict(
	operatorController *operator.Controller,
	sourceReplicaSets []*replica.SpanReplication,
	mergedDispatcherID common.DispatcherID,
) (common.DispatcherID, string, bool) {
	if op := operatorController.GetOperator(mergedDispatcherID); op != nil {
		return mergedDispatcherID, op.Type(), true
	}
	for _, replicaSet := range sourceReplicaSets {
		if replicaSet == nil {
			continue
		}
		if op := operatorController.GetOperator(replicaSet.ID); op != nil {
			return replicaSet.ID, op.Type(), true
		}
	}
	return common.DispatcherID{}, "", false
}

func tableSplitMapForMode(
	mode int64,
	tableSplitMaps map[int64]map[int64]bool,
) (map[int64]bool, bool) {
	if common.IsRedoMode(mode) {
		tableSplitMap, ok := tableSplitMaps[common.RedoMode]
		return tableSplitMap, ok
	}
	tableSplitMap, ok := tableSplitMaps[common.DefaultMode]
	return tableSplitMap, ok
}

// buildMergedSpanFromReplicas synthesizes the merged span shape from consecutive source replica sets.
func buildMergedSpanFromReplicas(
	replicaSets []*replica.SpanReplication,
) (*heartbeatpb.TableSpan, int64, uint64, bool) {
	if len(replicaSets) < 2 {
		return nil, 0, 0, false
	}
	first := replicaSets[0]
	if first == nil || first.Span == nil {
		return nil, 0, 0, false
	}

	tableID := first.Span.TableID
	keyspaceID := first.Span.KeyspaceID
	schemaID := first.GetSchemaID()
	nodeID := first.GetNodeID()
	startKey := first.Span.StartKey
	endKey := first.Span.EndKey
	firstStatus := first.GetStatus()
	if firstStatus == nil {
		return nil, 0, 0, false
	}
	minCheckpoint := firstStatus.CheckpointTs
	prevSpan := first.Span
	for idx := 1; idx < len(replicaSets); idx++ {
		replicaSet := replicaSets[idx]
		if replicaSet == nil || replicaSet.Span == nil {
			return nil, 0, 0, false
		}
		if replicaSet.Span.TableID != tableID ||
			replicaSet.Span.KeyspaceID != keyspaceID ||
			replicaSet.GetSchemaID() != schemaID ||
			replicaSet.GetNodeID() != nodeID {
			return nil, 0, 0, false
		}
		if !common.IsTableSpanConsecutive(prevSpan, replicaSet.Span) {
			return nil, 0, 0, false
		}
		status := replicaSet.GetStatus()
		if status == nil {
			return nil, 0, 0, false
		}
		if checkpoint := status.CheckpointTs; checkpoint < minCheckpoint {
			minCheckpoint = checkpoint
		}
		prevSpan = replicaSet.Span
		endKey = replicaSet.Span.EndKey
	}

	return &heartbeatpb.TableSpan{
		TableID:    tableID,
		StartKey:   startKey,
		EndKey:     endKey,
		KeyspaceID: keyspaceID,
	}, schemaID, minCheckpoint, true
}
