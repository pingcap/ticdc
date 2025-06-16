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
	"math/rand"
	"sort"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/split"
	"github.com/pingcap/ticdc/pkg/apperror"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	pkgscheduler "github.com/pingcap/ticdc/pkg/scheduler"
	pkgoperator "github.com/pingcap/ticdc/pkg/scheduler/operator"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils"
	"github.com/pingcap/ticdc/utils/threadpool"
	"go.uber.org/zap"
)

// ControllerManager schedules and balance tables
// there are 3 main components in the controllerManager, scheduler, ReplicationDB and operator controllerManager
type ControllerManager struct {
	bootstrapped bool

	schedulerController    *pkgscheduler.Controller
	operatorController     *operator.Controller
	redoOperatorController *operator.Controller
	controller             *Controller
	redoController         *Controller
	barrier                *Barrier
	redoBarrier            *Barrier

	messageCenter messaging.MessageCenter

	splitter               *split.Splitter
	enableTableAcrossNodes bool
	startCheckpointTs      uint64

	cfConfig     *config.ReplicaConfig
	changefeedID common.ChangeFeedID

	taskPool threadpool.ThreadPool

	// Store the task handles, it's used to stop the task handlers when the controllerManager is stopped.
	taskHandles []*threadpool.TaskHandle
}

func NewControllerManager(changefeedID common.ChangeFeedID,
	checkpointTs uint64,
	pdAPIClient pdutil.PDAPIClient,
	regionCache split.RegionCache,
	taskPool threadpool.ThreadPool,
	cfConfig *config.ReplicaConfig,
	ddlSpan, redoDDLSpan *replica.SpanReplication,
	batchSize int, balanceInterval time.Duration,
) *ControllerManager {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)

	enableTableAcrossNodes := false
	var splitter *split.Splitter
	if cfConfig != nil && cfConfig.Scheduler.EnableTableAcrossNodes {
		enableTableAcrossNodes = true
		splitter = split.NewSplitter(changefeedID, pdAPIClient, regionCache, cfConfig.Scheduler)
	}

	controller := NewController(changefeedID, ddlSpan, splitter, enableTableAcrossNodes)
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)

	var (
		redoController *Controller
		redoDB         *replica.ReplicationDB
		redoOC         *operator.Controller
	)
	if redoDDLSpan != nil {
		redoController = NewController(changefeedID, redoDDLSpan, splitter, enableTableAcrossNodes)
		redoDB = redoController.replicationDB
		redoOC = operator.NewOperatorController(changefeedID, mc, redoDB, nodeManager, batchSize, true)
	}
	oc := operator.NewOperatorController(changefeedID, mc, controller.replicationDB, nodeManager, batchSize, false)

	var schedulerCfg *config.ChangefeedSchedulerConfig
	if cfConfig != nil {
		schedulerCfg = cfConfig.Scheduler
	}
	sc := NewScheduleController(
		changefeedID, batchSize, oc, redoOC, controller.replicationDB, redoDB, nodeManager, balanceInterval, splitter, schedulerCfg,
	)

	return &ControllerManager{
		startCheckpointTs:      checkpointTs,
		changefeedID:           changefeedID,
		bootstrapped:           false,
		schedulerController:    sc,
		operatorController:     oc,
		redoOperatorController: redoOC,
		controller:             controller,
		redoController:         redoController,
		messageCenter:          mc,
		taskPool:               taskPool,
		cfConfig:               cfConfig,
		splitter:               splitter,
		enableTableAcrossNodes: enableTableAcrossNodes,
	}
}

// HandleStatus handle the status report from the node
func (cm *ControllerManager) HandleStatus(from node.ID, statusList []*heartbeatpb.TableSpanStatus) {
	var stm *replica.SpanReplication
	for _, status := range statusList {
		dispatcherID := common.NewDispatcherIDFromPB(status.ID)
		operatorController := cm.getOC(status.Redo)
		controller := cm.getController(status.Redo)
		operatorController.UpdateOperatorStatus(dispatcherID, from, status)
		stm = controller.GetTask(dispatcherID)
		if stm == nil {
			if status.ComponentStatus != heartbeatpb.ComponentState_Working {
				continue
			}
			if op := operatorController.GetOperator(dispatcherID); op == nil {
				// it's normal case when the span is not found in replication db
				// the span is removed from replication db first, so here we only check if the span status is working or not
				log.Warn("no span found, remove it",
					zap.String("changefeed", cm.changefeedID.Name()),
					zap.String("from", from.String()),
					zap.Any("status", status),
					zap.String("span", dispatcherID.String()))
				// if the span is not found, and the status is working, we need to remove it from dispatcher
				_ = cm.messageCenter.SendCommand(replica.NewRemoveDispatcherMessage(from, cm.changefeedID, status.ID, status.Redo))
			}
			continue
		}
		nodeID := stm.GetNodeID()
		if nodeID != from {
			// todo: handle the case that the node id is mismatch
			log.Warn("node id not match",
				zap.String("changefeed", cm.changefeedID.Name()),
				zap.Any("from", from),
				zap.Stringer("node", nodeID))
			continue
		}
		if status.Redo {
			cm.redoController.replicationDB.UpdateStatus(stm, status)
		} else {
			cm.controller.replicationDB.UpdateStatus(stm, status)
		}
	}
}

// FinishBootstrap finalizes the ControllerManager initialization process using bootstrap responses from all nodes.
// This method is the main entry point for ControllerManager initialization and performs several critical steps:
//
//  1. Determines the actual startTs by finding the maximum checkpoint timestamp
//     across all node responses and updates the DDL dispatcher status accordingly
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
//   - Starts the scheduler controllerManager for table distribution
//   - Starts the operator controllerManager for managing table operations
//
// Parameters:
//   - allNodesResp: Bootstrap responses from all nodes containing their current state
//   - isMysqlCompatible: Flag indicating if using MySQL-compatible backend
//
// Returns:
//   - *MaintainerPostBootstrapRequest: Configuration for post-bootstrap setup
//   - error: Any error encountered during the bootstrap process
func (cm *ControllerManager) FinishBootstrap(
	allNodesResp map[node.ID]*heartbeatpb.MaintainerBootstrapResponse,
	isMysqlCompatibleBackend bool,
) (*heartbeatpb.MaintainerPostBootstrapRequest, error) {
	if cm.bootstrapped {
		log.Panic("already bootstrapped",
			zap.Stringer("changefeed", cm.changefeedID),
			zap.Any("allNodesResp", allNodesResp))
	}

	log.Info("all nodes have sent bootstrap response, start to handle them",
		zap.Stringer("changefeed", cm.changefeedID),
		zap.Int("nodeCount", len(allNodesResp)))

	// Step 1: Determine start timestamp and update DDL dispatcher
	startTs := cm.determineStartTs(allNodesResp)

	// Step 2: Load tables from schema store
	tables, err := cm.loadTables(startTs)
	if err != nil {
		log.Error("load table from scheme store failed",
			zap.String("changefeed", cm.changefeedID.Name()),
			zap.Error(err))
		return nil, errors.Trace(err)
	}

	// Step 3: Build working task map from bootstrap responses
	workingTaskMap, redoWorkingTaskMap := cm.buildWorkingTaskMap(allNodesResp)

	// Step 4: Process tables and build schema info
	schemaInfos := cm.processTablesAndBuildSchemaInfo(tables, workingTaskMap, redoWorkingTaskMap, isMysqlCompatibleBackend)

	// Step 5: Handle any remaining working tasks (likely dropped tables)
	cm.handleRemainingWorkingTasks(workingTaskMap, redoWorkingTaskMap)

	// Step 6: Initialize and start sub components
	cm.initializeComponents()
	cm.newBarrier(allNodesResp)

	// Step 7: Prepare response
	initSchemaInfos := cm.prepareSchemaInfoResponse(schemaInfos)

	// Step 8: Mark the controllerManager as bootstrapped
	cm.bootstrapped = true

	return &heartbeatpb.MaintainerPostBootstrapRequest{
		ChangefeedID:                  cm.changefeedID.ToPB(),
		TableTriggerEventDispatcherId: cm.controller.ddlDispatcherID.ToPB(),
		Schemas:                       initSchemaInfos,
	}, nil
}

func (cm *ControllerManager) newBarrier(allNodesResp map[node.ID]*heartbeatpb.MaintainerBootstrapResponse) {
	// Initialize barrier
	if cm.redoController != nil {
		cm.redoBarrier = NewBarrier(cm.redoOperatorController, cm.redoController, cm.cfConfig.Scheduler.EnableTableAcrossNodes)
		cm.redoBarrier.HandleBootstrapResponse(allNodesResp, true)
	}
	cm.barrier = NewBarrier(cm.operatorController, cm.controller, cm.cfConfig.Scheduler.EnableTableAcrossNodes)
	cm.barrier.HandleBootstrapResponse(allNodesResp, false)
}

func (cm *ControllerManager) determineStartTs(allNodesResp map[node.ID]*heartbeatpb.MaintainerBootstrapResponse) uint64 {
	startTs := uint64(0)
	for node, resp := range allNodesResp {
		log.Info("handle bootstrap response",
			zap.Stringer("changefeed", cm.changefeedID),
			zap.Stringer("nodeID", node),
			zap.Uint64("checkpointTs", resp.CheckpointTs),
			zap.Int("spanCount", len(resp.Spans)))
		if resp.CheckpointTs > startTs {
			startTs = resp.CheckpointTs
			if cm.redoController != nil {
				status := cm.redoController.replicationDB.GetDDLDispatcher().GetStatus()
				status.CheckpointTs = startTs
				cm.redoController.replicationDB.UpdateStatus(cm.redoController.replicationDB.GetDDLDispatcher(), status)
			}
			status := cm.controller.replicationDB.GetDDLDispatcher().GetStatus()
			status.CheckpointTs = startTs
			cm.controller.replicationDB.UpdateStatus(cm.controller.replicationDB.GetDDLDispatcher(), status)
		}
	}
	if startTs == 0 {
		log.Panic("cant not found the startTs from the bootstrap response",
			zap.String("changefeed", cm.changefeedID.Name()))
	}
	return startTs
}

func (cm *ControllerManager) buildWorkingTaskMap(
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
			controller := cm.getController(spanInfo.Redo)
			if controller.isDDLDispatcher(dispatcherID) {
				continue
			}
			spanReplication := cm.createSpanReplication(spanInfo, node)
			if spanInfo.Redo {
				addToWorkingTaskMap(redoWorkingTaskMap, spanInfo.Span, spanReplication)
			} else {
				addToWorkingTaskMap(workingTaskMap, spanInfo.Span, spanReplication)
			}
		}
	}
	return workingTaskMap, redoWorkingTaskMap
}

func (cm *ControllerManager) createSpanReplication(spanInfo *heartbeatpb.BootstrapTableSpan, node node.ID) *replica.SpanReplication {
	status := &heartbeatpb.TableSpanStatus{
		ComponentStatus: spanInfo.ComponentStatus,
		ID:              spanInfo.ID,
		CheckpointTs:    spanInfo.CheckpointTs,
		Redo:            spanInfo.Redo,
	}

	return replica.NewWorkingSpanReplication(
		cm.changefeedID,
		common.NewDispatcherIDFromPB(spanInfo.ID),
		spanInfo.SchemaID,
		spanInfo.Span,
		status,
		node,
	)
}

func (cm *ControllerManager) processTablesAndBuildSchemaInfo(
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
		cm.processTableSpans(table, workingTaskMap, redoWorkingTaskMap)
	}

	return schemaInfos
}

func (cm *ControllerManager) processTableSpans(
	table commonEvent.Table,
	workingTaskMap, redoWorkingTaskMap map[int64]utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication],
) {
	tableSpans, isTableWorking := workingTaskMap[table.TableID]
	redoTableSpans, reodIsTableWorking := redoWorkingTaskMap[table.TableID]
	if cm.redoController != nil && isTableWorking != reodIsTableWorking {
		log.Error("found different table status when processing table spans",
			zap.Any("isTableWorking", isTableWorking), zap.Any("reodIsTableWorking", reodIsTableWorking))
	}

	// Add new table if not working
	if isTableWorking {
		// Handle existing table spans
		span := common.TableIDToComparableSpan(table.TableID)
		tableSpan := &heartbeatpb.TableSpan{
			TableID:  table.TableID,
			StartKey: span.StartKey,
			EndKey:   span.EndKey,
		}
		log.Info("table already working in other node",
			zap.Stringer("changefeed", cm.changefeedID),
			zap.Int64("tableID", table.TableID))

		if cm.redoController != nil {
			cm.redoController.addWorkingSpans(redoTableSpans)
		}
		cm.controller.addWorkingSpans(tableSpans)

		if cm.enableTableAcrossNodes {
			cm.handleTableHoles(table, tableSpans, redoTableSpans, tableSpan)
		}
		// Remove processed table from working task map
		delete(workingTaskMap, table.TableID)
		if reodIsTableWorking {
			delete(redoWorkingTaskMap, table.TableID)
		}
	} else {
		if cm.redoController != nil {
			cm.redoController.AddNewTable(table, cm.startCheckpointTs)
		}
		cm.controller.AddNewTable(table, cm.startCheckpointTs)
	}
}

func (cm *ControllerManager) handleTableHoles(
	table commonEvent.Table,
	tableSpans, redoTableSpans utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication],
	tableSpan *heartbeatpb.TableSpan,
) {
	// redo
	if cm.redoController != nil {
		holes := split.FindHoles(redoTableSpans, tableSpan)
		cm.redoController.addNewSpans(table.SchemaID, holes, cm.startCheckpointTs)
	}
	holes := split.FindHoles(tableSpans, tableSpan)
	// Todo: split the hole
	// Add holes to the replicationDB
	cm.controller.addNewSpans(table.SchemaID, holes, cm.startCheckpointTs)
}

func (cm *ControllerManager) handleRemainingWorkingTasks(
	workingTaskMap, redoWorkingTaskMap map[int64]utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication],
) {
	for tableID := range redoWorkingTaskMap {
		log.Warn("found a redo working table that is not in initial table map, just ignore it",
			zap.Stringer("changefeed", cm.changefeedID),
			zap.Int64("id", tableID))
	}
	for tableID := range workingTaskMap {
		log.Warn("found a working table that is not in initial table map, just ignore it",
			zap.Stringer("changefeed", cm.changefeedID),
			zap.Int64("id", tableID))
	}
}

func (cm *ControllerManager) initializeComponents() {
	// Start scheduler
	cm.taskHandles = append(cm.taskHandles, cm.schedulerController.Start(cm.taskPool)...)
	// redo
	if cm.redoOperatorController != nil {
		cm.taskHandles = append(cm.taskHandles, cm.taskPool.Submit(cm.redoOperatorController, time.Now()))
	}
	// Start operator controllerManager
	cm.taskHandles = append(cm.taskHandles, cm.taskPool.Submit(cm.operatorController, time.Now()))
}

func (cm *ControllerManager) prepareSchemaInfoResponse(
	schemaInfos map[int64]*heartbeatpb.SchemaInfo,
) []*heartbeatpb.SchemaInfo {
	initSchemaInfos := make([]*heartbeatpb.SchemaInfo, 0, len(schemaInfos))
	for _, info := range schemaInfos {
		initSchemaInfos = append(initSchemaInfos, info)
	}
	return initSchemaInfos
}

// RemoveTasksByTableIDs remove all tasks by table id
func (cm *ControllerManager) RemoveTasksByTableIDs(redo bool, tables ...int64) {
	operatorController := cm.getOC(redo)
	operatorController.RemoveTasksByTableIDs(tables...)
}

// RemoveNode is called when a node is removed
func (cm *ControllerManager) RemoveNode(id node.ID) {
	if cm.redoOperatorController != nil {
		cm.redoOperatorController.OnNodeRemoved(id)
	}
	cm.operatorController.OnNodeRemoved(id)
}

// ScheduleFinished return false if not all task are running in working state
func (cm *ControllerManager) ScheduleFinished() bool {
	return cm.operatorController.OperatorSizeWithLock() == 0 && cm.controller.replicationDB.GetAbsentSize() == 0
}

func (cm *ControllerManager) loadTables(startTs uint64) ([]commonEvent.Table, error) {
	// Use a empty timezone because table filter does not need it.
	f, err := filter.NewFilter(cm.cfConfig.Filter, "", cm.cfConfig.CaseSensitive, cm.cfConfig.ForceReplicate)
	if err != nil {
		return nil, errors.Cause(err)
	}

	schemaStore := appcontext.GetService[schemastore.SchemaStore](appcontext.SchemaStore)
	tables, err := schemaStore.GetAllPhysicalTables(startTs, f)
	log.Info("get table ids", zap.Int("count", len(tables)), zap.String("changefeed", cm.changefeedID.Name()))
	return tables, err
}

func (cm *ControllerManager) Stop() {
	for _, handler := range cm.taskHandles {
		handler.Cancel()
	}
}

// only for test
// moveTable is used for inner api(which just for make test cases convience) to force move a table to a target node.
// moveTable only works for the complete table, not for the table splited.
func (cm *ControllerManager) moveTable(tableId int64, targetNode node.ID) error {
	if err := cm.controller.checkParams(tableId, targetNode); err != nil {
		return err
	}

	replications := cm.controller.replicationDB.GetTasksByTableID(tableId)
	if len(replications) != 1 {
		return apperror.ErrTableIsNotFounded.GenWithStackByArgs("unexpected number of replications found for table in this node; tableID is %s, replication count is %s", tableId, len(replications))
	}

	replication := replications[0]

	op := cm.operatorController.NewMoveOperator(replication, replication.GetNodeID(), targetNode)
	cm.operatorController.AddOperator(op)

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
func (cm *ControllerManager) moveSplitTable(tableId int64, targetNode node.ID) error {
	if err := cm.controller.checkParams(tableId, targetNode); err != nil {
		return err
	}

	replications := cm.controller.replicationDB.GetTasksByTableID(tableId)
	opList := make([]pkgoperator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus], 0, len(replications))
	finishList := make([]bool, len(replications))
	for _, replication := range replications {
		if replication.GetNodeID() == targetNode {
			continue
		}
		op := cm.operatorController.NewMoveOperator(replication, replication.GetNodeID(), targetNode)
		cm.operatorController.AddOperator(op)
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
func (cm *ControllerManager) splitTableByRegionCount(tableID int64) error {
	if !cm.controller.replicationDB.IsTableExists(tableID) {
		// the table is not exist in this node
		return apperror.ErrTableIsNotFounded.GenWithStackByArgs("tableID", tableID)
	}

	if tableID == 0 {
		return apperror.ErrTableNotSupportMove.GenWithStackByArgs("tableID", tableID)
	}

	replications := cm.controller.replicationDB.GetTasksByTableID(tableID)

	span := common.TableIDToComparableSpan(tableID)
	wholeSpan := &heartbeatpb.TableSpan{
		TableID:  span.TableID,
		StartKey: span.StartKey,
		EndKey:   span.EndKey,
	}
	splitTableSpans := cm.splitter.SplitSpansByRegion(context.Background(), wholeSpan)

	if len(splitTableSpans) == len(replications) {
		log.Info("Split Table is finished; There is no need to do split", zap.Any("tableID", tableID))
		return nil
	}

	randomIdx := rand.Intn(len(replications))
	primaryID := replications[randomIdx].ID
	primaryOp := operator.NewMergeSplitDispatcherOperator(cm.controller.replicationDB, primaryID, replications[randomIdx], replications, splitTableSpans, nil, false)
	for _, replicaSet := range replications {
		var op *operator.MergeSplitDispatcherOperator
		if replicaSet.ID == primaryID {
			op = primaryOp
		} else {
			op = operator.NewMergeSplitDispatcherOperator(cm.controller.replicationDB, primaryID, replicaSet, nil, nil, primaryOp.GetOnFinished(), false)
		}
		cm.operatorController.AddOperator(op)
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
func (cm *ControllerManager) mergeTable(tableID int64) error {
	if !cm.controller.replicationDB.IsTableExists(tableID) {
		// the table is not exist in this node
		return apperror.ErrTableIsNotFounded.GenWithStackByArgs("tableID", tableID)
	}

	if tableID == 0 {
		return apperror.ErrTableNotSupportMove.GenWithStackByArgs("tableID", tableID)
	}

	replications := cm.controller.replicationDB.GetTasksByTableID(tableID)

	if len(replications) == 1 {
		log.Info("Merge Table is finished; There is only one replication for this table, so no need to do merge", zap.Any("tableID", tableID))
		return nil
	}

	// sort by startKey
	sort.Slice(replications, func(i, j int) bool {
		return bytes.Compare(replications[i].Span.StartKey, replications[j].Span.StartKey) < 0
	})

	// choose the first two replication to merge a new replication
	newSpan := &heartbeatpb.TableSpan{
		TableID:  replications[0].Span.TableID,
		StartKey: replications[0].Span.StartKey,
		EndKey:   replications[1].Span.EndKey,
	}

	mergeReplication := replications[:2]

	primaryID := replications[0].ID
	primaryOp := operator.NewMergeSplitDispatcherOperator(cm.controller.replicationDB, primaryID, replications[0], mergeReplication, []*heartbeatpb.TableSpan{newSpan}, nil, false)
	secondaryOp := operator.NewMergeSplitDispatcherOperator(cm.controller.replicationDB, primaryID, replications[1], nil, nil, primaryOp.GetOnFinished(), false)
	cm.operatorController.AddOperator(primaryOp)
	cm.operatorController.AddOperator(secondaryOp)

	count := 0
	maxTry := 30
	for count < maxTry {
		if primaryOp.IsFinished() {
			return nil
		}

		time.Sleep(1 * time.Second)
		count += 1
		log.Info("wait for merge table table operator finished", zap.Int("count", count))
	}

	return apperror.ErrTimeout.GenWithStackByArgs("merge table operator is timeout")
}

func (cm *ControllerManager) getOC(redo bool) *operator.Controller {
	if redo {
		return cm.redoOperatorController
	}
	return cm.operatorController
}

func (cm *ControllerManager) getController(redo bool) *Controller {
	if redo {
		return cm.redoController
	}
	return cm.controller
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
