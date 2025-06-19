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

// Controller schedules and balance tables
// there are 3 main components in the controller, scheduler, ReplicationDB and operator controller
type Controller struct {
	bootstrapped bool

	schedulerController *pkgscheduler.Controller
	operatorController  *operator.Controller
	replicationDB       *replica.ReplicationDB
	messageCenter       messaging.MessageCenter
	nodeManager         *watcher.NodeManager

	splitter               *split.Splitter
	enableTableAcrossNodes bool
	startCheckpointTs      uint64
	ddlDispatcherID        common.DispatcherID

	cfConfig     *config.ReplicaConfig
	changefeedID common.ChangeFeedID

	taskPool threadpool.ThreadPool

	// Store the task handles, it's used to stop the task handlers when the controller is stopped.
	taskHandles []*threadpool.TaskHandle
}

func NewController(changefeedID common.ChangeFeedID,
	checkpointTs uint64,
	pdAPIClient pdutil.PDAPIClient,
	regionCache split.RegionCache,
	taskPool threadpool.ThreadPool,
	cfConfig *config.ReplicaConfig,
	ddlSpan *replica.SpanReplication,
	batchSize int, balanceInterval time.Duration,
) *Controller {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)

	enableTableAcrossNodes := false
	var splitter *split.Splitter
	if cfConfig != nil && cfConfig.Scheduler.EnableTableAcrossNodes {
		enableTableAcrossNodes = true
		splitter = split.NewSplitter(changefeedID, pdAPIClient, regionCache, cfConfig.Scheduler)
	}

	replicaSetDB := replica.NewReplicaSetDB(changefeedID, ddlSpan, enableTableAcrossNodes)
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)

	oc := operator.NewOperatorController(changefeedID, mc, replicaSetDB, nodeManager, batchSize)

	var schedulerCfg *config.ChangefeedSchedulerConfig
	if cfConfig != nil {
		schedulerCfg = cfConfig.Scheduler
	}
	sc := NewScheduleController(
		changefeedID, batchSize, oc, replicaSetDB, nodeManager, balanceInterval, splitter, schedulerCfg,
	)

	return &Controller{
		startCheckpointTs:      checkpointTs,
		changefeedID:           changefeedID,
		bootstrapped:           false,
		ddlDispatcherID:        ddlSpan.ID,
		schedulerController:    sc,
		operatorController:     oc,
		messageCenter:          mc,
		replicationDB:          replicaSetDB,
		nodeManager:            nodeManager,
		taskPool:               taskPool,
		cfConfig:               cfConfig,
		splitter:               splitter,
		enableTableAcrossNodes: enableTableAcrossNodes,
	}
}

// HandleStatus handle the status report from the node
func (c *Controller) HandleStatus(from node.ID, statusList []*heartbeatpb.TableSpanStatus) {
	for _, status := range statusList {
		dispatcherID := common.NewDispatcherIDFromPB(status.ID)
		c.operatorController.UpdateOperatorStatus(dispatcherID, from, status)
		stm := c.GetTask(dispatcherID)
		if stm == nil {
			if status.ComponentStatus != heartbeatpb.ComponentState_Working {
				continue
			}
			if op := c.operatorController.GetOperator(dispatcherID); op == nil {
				// it's normal case when the span is not found in replication db
				// the span is removed from replication db first, so here we only check if the span status is working or not
				log.Warn("no span found, remove it",
					zap.String("changefeed", c.changefeedID.Name()),
					zap.String("from", from.String()),
					zap.Any("status", status),
					zap.String("dispatcherID", dispatcherID.String()))
				// if the span is not found, and the status is working, we need to remove it from dispatcher
				_ = c.messageCenter.SendCommand(replica.NewRemoveDispatcherMessage(from, c.changefeedID, status.ID))
			}
			continue
		}
		nodeID := stm.GetNodeID()
		if nodeID != from {
			// todo: handle the case that the node id is mismatch
			log.Warn("node id not match",
				zap.String("changefeed", c.changefeedID.Name()),
				zap.Any("from", from),
				zap.Stringer("node", nodeID))
			continue
		}
		c.replicationDB.UpdateStatus(stm, status)
	}
}

func (c *Controller) GetTasksBySchemaID(schemaID int64) []*replica.SpanReplication {
	return c.replicationDB.GetTasksBySchemaID(schemaID)
}

func (c *Controller) GetTaskSizeBySchemaID(schemaID int64) int {
	return c.replicationDB.GetTaskSizeBySchemaID(schemaID)
}

func (c *Controller) GetAllNodes() []node.ID {
	aliveNodes := c.nodeManager.GetAliveNodes()
	nodes := make([]node.ID, 0, len(aliveNodes))
	for id := range aliveNodes {
		nodes = append(nodes, id)
	}
	return nodes
}

func (c *Controller) AddNewTable(table commonEvent.Table, startTs uint64) {
	if c.replicationDB.IsTableExists(table.TableID) {
		log.Warn("table already add, ignore",
			zap.String("changefeed", c.changefeedID.Name()),
			zap.Int64("schema", table.SchemaID),
			zap.Int64("table", table.TableID))
		return
	}
	span := common.TableIDToComparableSpan(table.TableID)
	tableSpan := &heartbeatpb.TableSpan{
		TableID:  table.TableID,
		StartKey: span.StartKey,
		EndKey:   span.EndKey,
	}
	tableSpans := []*heartbeatpb.TableSpan{tableSpan}
	if c.enableTableAcrossNodes && len(c.nodeManager.GetAliveNodes()) > 1 {
		// split the whole table span base on region count if table region count is exceed the limit
		tableSpans = c.splitter.SplitSpansByRegion(context.Background(), tableSpan)
	}
	c.addNewSpans(table.SchemaID, tableSpans, startTs)
}

// FinishBootstrap finalizes the Controller initialization process using bootstrap responses from all nodes.
// This method is the main entry point for Controller initialization and performs several critical steps:
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
//   - Starts the scheduler controller for table distribution
//   - Starts the operator controller for managing table operations
//
// Parameters:
//   - allNodesResp: Bootstrap responses from all nodes containing their current state
//   - isMysqlCompatible: Flag indicating if using MySQL-compatible backend
//
// Returns:
//   - *Barrier: Initialized barrier for consistency tracking
//   - *MaintainerPostBootstrapRequest: Configuration for post-bootstrap setup
//   - error: Any error encountered during the bootstrap process
func (c *Controller) FinishBootstrap(
	allNodesResp map[node.ID]*heartbeatpb.MaintainerBootstrapResponse,
	isMysqlCompatibleBackend bool,
) (*Barrier, *heartbeatpb.MaintainerPostBootstrapRequest, error) {
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
		return nil, nil, errors.Trace(err)
	}

	// Step 3: Build working task map from bootstrap responses
	workingTaskMap := c.buildWorkingTaskMap(allNodesResp)

	// Step 4: Process tables and build schema info
	schemaInfos := c.processTablesAndBuildSchemaInfo(tables, workingTaskMap, isMysqlCompatibleBackend)

	// Step 5: Handle any remaining working tasks (likely dropped tables)
	c.handleRemainingWorkingTasks(workingTaskMap)

	// Step 6: Initialize and start sub components
	barrier := c.initializeComponents(allNodesResp)

	// Step 7: Prepare response
	initSchemaInfos := c.prepareSchemaInfoResponse(schemaInfos)

	// Step 8: Mark the controller as bootstrapped
	c.bootstrapped = true

	return barrier, &heartbeatpb.MaintainerPostBootstrapRequest{
		ChangefeedID:                  c.changefeedID.ToPB(),
		TableTriggerEventDispatcherId: c.ddlDispatcherID.ToPB(),
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
			status := c.replicationDB.GetDDLDispatcher().GetStatus()
			status.CheckpointTs = startTs
			c.replicationDB.UpdateStatus(c.replicationDB.GetDDLDispatcher(), status)
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
) map[int64]utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication] {
	workingTaskMap := make(map[int64]utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication])
	for node, resp := range allNodesResp {
		for _, spanInfo := range resp.Spans {
			dispatcherID := common.NewDispatcherIDFromPB(spanInfo.ID)
			if c.isDDLDispatcher(dispatcherID) {
				continue
			}
			spanReplication := c.createSpanReplication(spanInfo, node)
			c.addToWorkingTaskMap(workingTaskMap, spanInfo.Span, spanReplication)
		}
	}
	return workingTaskMap
}

func (c *Controller) createSpanReplication(spanInfo *heartbeatpb.BootstrapTableSpan, node node.ID) *replica.SpanReplication {
	status := &heartbeatpb.TableSpanStatus{
		ComponentStatus: spanInfo.ComponentStatus,
		ID:              spanInfo.ID,
		CheckpointTs:    spanInfo.CheckpointTs,
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

func (c *Controller) addToWorkingTaskMap(
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

func (c *Controller) processTablesAndBuildSchemaInfo(
	tables []commonEvent.Table,
	workingTaskMap map[int64]utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication],
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
		c.processTableSpans(table, workingTaskMap)
	}

	return schemaInfos
}

func (c *Controller) processTableSpans(
	table commonEvent.Table,
	workingTaskMap map[int64]utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication],
) {
	tableSpans, isTableWorking := workingTaskMap[table.TableID]

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
			zap.Stringer("changefeed", c.changefeedID),
			zap.Int64("tableID", table.TableID))

		c.addWorkingSpans(tableSpans)

		if c.enableTableAcrossNodes {
			c.handleTableHoles(table, tableSpans, tableSpan)
		}
		// Remove processed table from working task map
		delete(workingTaskMap, table.TableID)
	} else {
		c.AddNewTable(table, c.startCheckpointTs)
	}
}

func (c *Controller) handleTableHoles(
	table commonEvent.Table,
	tableSpans utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication],
	tableSpan *heartbeatpb.TableSpan,
) {
	holes := split.FindHoles(tableSpans, tableSpan)
	// Todo: split the hole
	// Add holes to the replicationDB
	c.addNewSpans(table.SchemaID, holes, c.startCheckpointTs)
}

func (c *Controller) handleRemainingWorkingTasks(
	workingTaskMap map[int64]utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication],
) {
	for tableID := range workingTaskMap {
		log.Warn("found a working table that is not in initial table map, just ignore it",
			zap.Stringer("changefeed", c.changefeedID),
			zap.Int64("id", tableID))
	}
}

func (c *Controller) initializeComponents(
	allNodesResp map[node.ID]*heartbeatpb.MaintainerBootstrapResponse,
) *Barrier {
	// Initialize barrier
	barrier := NewBarrier(c, c.cfConfig.Scheduler.EnableTableAcrossNodes)
	barrier.HandleBootstrapResponse(allNodesResp)

	// Start scheduler
	c.taskHandles = append(c.taskHandles, c.schedulerController.Start(c.taskPool)...)

	// Start operator controller
	c.taskHandles = append(c.taskHandles, c.taskPool.Submit(c.operatorController, time.Now()))

	return barrier
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

// GetTask queries a task by dispatcherID, return nil if not found
func (c *Controller) GetTask(dispatcherID common.DispatcherID) *replica.SpanReplication {
	return c.replicationDB.GetTaskByID(dispatcherID)
}

// RemoveAllTasks remove all tasks
func (c *Controller) RemoveAllTasks() {
	c.operatorController.RemoveAllTasks()
}

// RemoveTasksBySchemaID remove all tasks by schema id
func (c *Controller) RemoveTasksBySchemaID(schemaID int64) {
	c.operatorController.RemoveTasksBySchemaID(schemaID)
}

// RemoveTasksByTableIDs remove all tasks by table id
func (c *Controller) RemoveTasksByTableIDs(tables ...int64) {
	c.operatorController.RemoveTasksByTableIDs(tables...)
}

// GetTasksByTableID get all tasks by table id
func (c *Controller) GetTasksByTableID(tableID int64) []*replica.SpanReplication {
	return c.replicationDB.GetTasksByTableID(tableID)
}

// GetAllTasks get all tasks
func (c *Controller) GetAllTasks() []*replica.SpanReplication {
	return c.replicationDB.GetAllTasks()
}

// UpdateSchemaID will update the schema id of the table, and move the task to the new schema map
// it called when rename a table to another schema
func (c *Controller) UpdateSchemaID(tableID, newSchemaID int64) {
	c.replicationDB.UpdateSchemaID(tableID, newSchemaID)
}

// RemoveNode is called when a node is removed
func (c *Controller) RemoveNode(id node.ID) {
	c.operatorController.OnNodeRemoved(id)
}

// ScheduleFinished return false if not all task are running in working state
func (c *Controller) ScheduleFinished() bool {
	return c.operatorController.OperatorSizeWithLock() == 0 && c.replicationDB.GetAbsentSize() == 0
}

func (c *Controller) TaskSize() int {
	return c.replicationDB.TaskSize()
}

func (c *Controller) GetTaskSizeByNodeID(id node.ID) int {
	return c.replicationDB.GetTaskSizeByNodeID(id)
}

func (c *Controller) addWorkingSpans(tableMap utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication]) {
	tableMap.Ascend(func(span *heartbeatpb.TableSpan, stm *replica.SpanReplication) bool {
		c.replicationDB.AddReplicatingSpan(stm)
		return true
	})
}

func (c *Controller) addNewSpans(schemaID int64, tableSpans []*heartbeatpb.TableSpan, startTs uint64) {
	for _, span := range tableSpans {
		dispatcherID := common.NewDispatcherID()
		replicaSet := replica.NewSpanReplication(c.changefeedID, dispatcherID, schemaID, span, startTs)
		c.replicationDB.AddAbsentReplicaSet(replicaSet)
	}
}

func (c *Controller) loadTables(startTs uint64) ([]commonEvent.Table, error) {
	// Use a empty timezone because table filter does not need it.
	f, err := filter.NewFilter(c.cfConfig.Filter, "", c.cfConfig.CaseSensitive, c.cfConfig.ForceReplicate)
	if err != nil {
		return nil, errors.Cause(err)
	}

	schemaStore := appcontext.GetService[schemastore.SchemaStore](appcontext.SchemaStore)
	tables, err := schemaStore.GetAllPhysicalTables(startTs, f)
	log.Info("get table ids", zap.Int("count", len(tables)), zap.String("changefeed", c.changefeedID.Name()))
	return tables, err
}

func (c *Controller) checkParams(tableId int64, targetNode node.ID) error {
	if !c.replicationDB.IsTableExists(tableId) {
		// the table is not exist in this node
		return apperror.ErrTableIsNotFounded.GenWithStackByArgs("tableID", tableId)
	}

	if tableId == 0 {
		return apperror.ErrTableNotSupportMove.GenWithStackByArgs("tableID", tableId)
	}

	nodes := c.nodeManager.GetAliveNodes()
	hasNode := false
	for _, node := range nodes {
		if node.ID == targetNode {
			hasNode = true
			break
		}
	}
	if !hasNode {
		return apperror.ErrNodeIsNotFound.GenWithStackByArgs("targetNode", targetNode)
	}

	return nil
}

func (c *Controller) isDDLDispatcher(dispatcherID common.DispatcherID) bool {
	return dispatcherID == c.ddlDispatcherID
}

func (c *Controller) Stop() {
	for _, handler := range c.taskHandles {
		handler.Cancel()
	}
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

// ========================== methods for HTTP API | Only For Test ==========================
// only for test
// moveTable is used for inner api(which just for make test cases convience) to force move a table to a target node.
// moveTable only works for the complete table, not for the table splited.
func (c *Controller) moveTable(tableId int64, targetNode node.ID) error {
	if err := c.checkParams(tableId, targetNode); err != nil {
		return err
	}

	replications := c.replicationDB.GetTasksByTableID(tableId)
	if len(replications) != 1 {
		return apperror.ErrTableIsNotFounded.GenWithStackByArgs("unexpected number of replications found for table in this node; tableID is %s, replication count is %s", tableId, len(replications))
	}

	replication := replications[0]

	op := c.operatorController.NewMoveOperator(replication, replication.GetNodeID(), targetNode)
	c.operatorController.AddOperator(op)

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
func (c *Controller) moveSplitTable(tableId int64, targetNode node.ID) error {
	if err := c.checkParams(tableId, targetNode); err != nil {
		return err
	}

	replications := c.replicationDB.GetTasksByTableID(tableId)
	opList := make([]pkgoperator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus], 0, len(replications))
	finishList := make([]bool, len(replications))
	for _, replication := range replications {
		if replication.GetNodeID() == targetNode {
			continue
		}
		op := c.operatorController.NewMoveOperator(replication, replication.GetNodeID(), targetNode)
		c.operatorController.AddOperator(op)
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
func (c *Controller) splitTableByRegionCount(tableID int64) error {
	if !c.replicationDB.IsTableExists(tableID) {
		// the table is not exist in this node
		return apperror.ErrTableIsNotFounded.GenWithStackByArgs("tableID", tableID)
	}

	if tableID == 0 {
		return apperror.ErrTableNotSupportMove.GenWithStackByArgs("tableID", tableID)
	}

	replications := c.replicationDB.GetTasksByTableID(tableID)

	span := common.TableIDToComparableSpan(tableID)
	wholeSpan := &heartbeatpb.TableSpan{
		TableID:  span.TableID,
		StartKey: span.StartKey,
		EndKey:   span.EndKey,
	}
	splitTableSpans := c.splitter.SplitSpansByRegion(context.Background(), wholeSpan)

	if len(splitTableSpans) == len(replications) {
		log.Info("Split Table is finished; There is no need to do split", zap.Any("tableID", tableID))
		return nil
	}

	randomIdx := rand.Intn(len(replications))
	primaryID := replications[randomIdx].ID
	primaryOp := operator.NewMergeSplitDispatcherOperator(c.replicationDB, primaryID, replications[randomIdx], replications, splitTableSpans, nil)
	for _, replicaSet := range replications {
		var op *operator.MergeSplitDispatcherOperator
		if replicaSet.ID == primaryID {
			op = primaryOp
		} else {
			op = operator.NewMergeSplitDispatcherOperator(c.replicationDB, primaryID, replicaSet, nil, nil, primaryOp.GetOnFinished())
		}
		c.operatorController.AddOperator(op)
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
func (c *Controller) mergeTable(tableID int64) error {
	if !c.replicationDB.IsTableExists(tableID) {
		// the table is not exist in this node
		return apperror.ErrTableIsNotFounded.GenWithStackByArgs("tableID", tableID)
	}

	if tableID == 0 {
		return apperror.ErrTableNotSupportMove.GenWithStackByArgs("tableID", tableID)
	}

	replications := c.replicationDB.GetTasksByTableID(tableID)

	if len(replications) == 1 {
		log.Info("Merge Table is finished; There is only one replication for this table, so no need to do merge", zap.Any("tableID", tableID))
		return nil
	}

	// sort by startKey
	sort.Slice(replications, func(i, j int) bool {
		return bytes.Compare(replications[i].Span.StartKey, replications[j].Span.StartKey) < 0
	})

	log.Debug("sorted replications in mergeTable", zap.Any("replications", replications))

	// try to select two consecutive spans in the same node to merge
	// if we can't find, we just move one span to make it satisfied.
	idx := 0
	mergeSpanFound := false
	for idx+1 < len(replications) {
		if replications[idx].GetNodeID() == replications[idx+1].GetNodeID() && common.IsTableSpanConsecutive(replications[idx].Span, replications[idx+1].Span) {
			mergeSpanFound = true
			break
		} else {
			idx++
		}
	}

	if !mergeSpanFound {
		idx = 0
		// try to move the second span to the first span's node
		moveOp := c.operatorController.NewMoveOperator(replications[1], replications[1].GetNodeID(), replications[0].GetNodeID())
		c.operatorController.AddOperator(moveOp)

		count := 0
		maxTry := 30
		flag := false
		for count < maxTry {
			if moveOp.IsFinished() {
				flag = true
				break
			}
			time.Sleep(1 * time.Second)
			count += 1
			log.Info("wait for move table table operator finished", zap.Int("count", count))
		}

		if !flag {
			return apperror.ErrTimeout.GenWithStackByArgs("move table operator before merge table is timeout")
		}
	}

	operator := c.operatorController.AddMergeOperator(replications[idx : idx+2])
	if operator == nil {
		return apperror.ErrOperatorIsNil.GenWithStackByArgs("unexpected error in create merge operator")
	}

	count := 0
	maxTry := 30
	for count < maxTry {
		if operator.IsFinished() {
			return nil
		}

		time.Sleep(1 * time.Second)
		count += 1
		log.Info("wait for merge table table operator finished", zap.Int("count", count), zap.Any("operator", operator.String()))
	}

	return apperror.ErrTimeout.GenWithStackByArgs("merge table operator is timeout")
}
