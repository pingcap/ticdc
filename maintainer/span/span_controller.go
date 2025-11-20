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

package span

import (
	"context"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/split"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/node"
	pkgreplica "github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils"
	"go.uber.org/zap"
)

var _ pkgreplica.ReplicationDB[common.DispatcherID, *replica.SpanReplication] = &Controller{}

// Controller manages the lifecycle and scheduling of data replication spans for a changefeed.
// It serves as the central coordinator for span operations, including creation, scheduling,
// status tracking, and cleanup. The controller maintains multiple indexing structures to
// efficiently access spans by different dimensions (dispatcher ID, table ID, schema ID).
//
// Key responsibilities:
// - Span lifecycle management (add, remove, update)
// - Multi-dimensional indexing for efficient span lookup
// - Replication status tracking and state transitions
// - DDL operation handling and schema updates
// - Table splitting for cross-node replication
// - Concurrent access control and thread safety
//
// The controller also implements the ReplicationDB interface to provide standardized replication database functionality
type Controller struct {
	// changefeedID uniquely identifies the changefeed this Controller belongs to
	changefeedID common.ChangeFeedID
	// ddlSpan is a special span that handles DDL operations, it is always on the same node as the maintainer
	// so no need to schedule it
	ddlSpan *replica.SpanReplication

	// mu protects concurrent access to [pkgreplica.ReplicationDB, ddlSpan, allTasks, schemaTasks, tableTasks]
	mu sync.RWMutex
	// ReplicationDB tracks the scheduling status of spans
	pkgreplica.ReplicationDB[common.DispatcherID, *replica.SpanReplication]
	// allTasks maps dispatcher IDs to their spans, including table trigger dispatchers
	allTasks map[common.DispatcherID]*replica.SpanReplication
	// schemaTasks provides quick access to spans by schema ID
	schemaTasks map[int64]map[common.DispatcherID]*replica.SpanReplication
	// tableTasks provides quick access to spans by table ID
	tableTasks map[int64]map[common.DispatcherID]*replica.SpanReplication

	// newGroupChecker creates a GroupChecker for validating span groups
	newGroupChecker func(groupID pkgreplica.GroupID) pkgreplica.GroupChecker[common.DispatcherID, *replica.SpanReplication]

	nodeManager            *watcher.NodeManager
	splitter               *split.Splitter
	enableTableAcrossNodes bool
	ddlDispatcherID        common.DispatcherID
	mode                   int64
	enableSplittableCheck  bool

	keyspaceID uint32
}

// NewController creates a new span controller
func NewController(
	changefeedID common.ChangeFeedID,
	ddlSpan *replica.SpanReplication,
	splitter *split.Splitter,
	schedulerCfg *config.ChangefeedSchedulerConfig,
	keyspaceID uint32,
	mode int64,
) *Controller {
	c := &Controller{
		changefeedID:           changefeedID,
		ddlSpan:                ddlSpan,
		newGroupChecker:        replica.GetNewGroupChecker(changefeedID, schedulerCfg),
		nodeManager:            appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName),
		splitter:               splitter,
		ddlDispatcherID:        ddlSpan.ID,
		mode:                   mode,
		enableTableAcrossNodes: schedulerCfg != nil && schedulerCfg.EnableTableAcrossNodes,
		enableSplittableCheck:  schedulerCfg != nil && schedulerCfg.EnableSplittableCheck,
		keyspaceID:             keyspaceID,
	}

	c.reset(c.ddlSpan)
	return c
}

// doWithRLock is a helper function to execute the action with a read lock
func (c *Controller) doWithRLock(action func()) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	action()
}

// reset resets the maps of Controller
func (c *Controller) reset(ddlSpan *replica.SpanReplication) {
	c.schemaTasks = make(map[int64]map[common.DispatcherID]*replica.SpanReplication)
	c.tableTasks = make(map[int64]map[common.DispatcherID]*replica.SpanReplication)
	c.allTasks = make(map[common.DispatcherID]*replica.SpanReplication)
	c.ReplicationDB = pkgreplica.NewReplicationDB(c.changefeedID.String(), c.doWithRLock, c.newGroupChecker)
	c.initializeDDLSpan(ddlSpan)
}

func (c *Controller) initializeDDLSpan(ddlSpan *replica.SpanReplication) {
	// we don't need to schedule the ddl span, but added it to the allTasks map, so we can access it by id
	c.allTasks[ddlSpan.ID] = ddlSpan
	// dispatcher will report a block event with table ID 0,
	// so we need to add it to the table map
	c.tableTasks[ddlSpan.Span.TableID] = map[common.DispatcherID]*replica.SpanReplication{
		ddlSpan.ID: ddlSpan,
	}
	// also put it to the schema map
	c.schemaTasks[ddlSpan.GetSchemaID()] = map[common.DispatcherID]*replica.SpanReplication{
		ddlSpan.ID: ddlSpan,
	}
}

// AddNewTable adds a new table to the span controller
// This is a complex business logic method that handles table splitting and span creation
func (c *Controller) AddNewTable(table commonEvent.Table, startTs uint64) {
	if c.IsTableExists(table.TableID) {
		log.Warn("table already add, ignore",
			zap.String("changefeed", c.changefeedID.Name()),
			zap.Int64("schema", table.SchemaID),
			zap.Int64("table", table.TableID))
		return
	}

	keyspaceID := c.GetkeyspaceID()

	span := common.TableIDToComparableSpan(keyspaceID, table.TableID)
	tableSpan := &heartbeatpb.TableSpan{
		TableID:    table.TableID,
		StartKey:   span.StartKey,
		EndKey:     span.EndKey,
		KeyspaceID: keyspaceID,
	}
	tableSpans := []*heartbeatpb.TableSpan{tableSpan}

	// Determine if the table can be split based on configuration and table splittable status
	if c.enableTableAcrossNodes && c.splitter != nil && (table.Splitable || !c.enableSplittableCheck) {
		tableSpans = c.splitter.Split(context.Background(), tableSpan, 0, split.SplitTypeRegionCount)
	}
	c.AddNewSpans(table.SchemaID, tableSpans, startTs)
}

// AddWorkingSpans adds working spans
func (c *Controller) AddWorkingSpans(tableMap utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication]) {
	tableMap.Ascend(func(span *heartbeatpb.TableSpan, stm *replica.SpanReplication) bool {
		c.AddReplicatingSpan(stm)
		return true
	})
}

// AddNewSpans creates new spans for the given schema and table spans
// This is a complex business logic method that handles span creation
func (c *Controller) AddNewSpans(schemaID int64, tableSpans []*heartbeatpb.TableSpan, startTs uint64) {
	for _, span := range tableSpans {
		dispatcherID := common.NewDispatcherID()
		span.KeyspaceID = c.GetkeyspaceID()
		replicaSet := replica.NewSpanReplication(c.changefeedID, dispatcherID, schemaID, span, startTs, c.mode)
		c.AddAbsentReplicaSet(replicaSet)
	}
}

func (c *Controller) GetMinCheckpointTsForNonReplicatingSpans(minCheckpointTs uint64) uint64 {
	for _, span := range c.GetAbsent() {
		if span.GetStatus().CheckpointTs < minCheckpointTs {
			minCheckpointTs = span.GetStatus().CheckpointTs
		}
	}
	for _, span := range c.GetScheduling() {
		if span.GetStatus().CheckpointTs < minCheckpointTs {
			minCheckpointTs = span.GetStatus().CheckpointTs
		}
	}
	return minCheckpointTs
}

// GetTaskByID returns the replica set by the id, it will search the replicating, scheduling and absent map
func (c *Controller) GetTaskByID(id common.DispatcherID) *replica.SpanReplication {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.allTasks[id]
}

// GetTasksByTableID returns the spans by the table id
func (c *Controller) GetTasksByTableID(tableID int64) []*replica.SpanReplication {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var tasks []*replica.SpanReplication
	for _, task := range c.tableTasks[tableID] {
		tasks = append(tasks, task)
	}
	return tasks
}

// GetTasksBySchemaID returns the spans by the schema id
func (c *Controller) GetTasksBySchemaID(schemaID int64) []*replica.SpanReplication {
	c.mu.RLock()
	defer c.mu.RUnlock()

	sm, ok := c.schemaTasks[schemaID]
	if !ok {
		return nil
	}
	replicaSets := make([]*replica.SpanReplication, 0, len(sm))
	for _, v := range sm {
		replicaSets = append(replicaSets, v)
	}
	return replicaSets
}

// GetAllTasks returns all the spans in the db, it will also return the ddl span
func (c *Controller) GetAllTasks() []*replica.SpanReplication {
	c.mu.RLock()
	defer c.mu.RUnlock()

	tasks := make([]*replica.SpanReplication, 0, len(c.allTasks))
	for _, task := range c.allTasks {
		tasks = append(tasks, task)
	}
	return tasks
}

// GetTaskSizeBySchemaID returns the size of the task by the schema id
func (c *Controller) GetTaskSizeBySchemaID(schemaID int64) int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	sm, ok := c.schemaTasks[schemaID]
	if ok {
		return len(sm)
	}
	return 0
}

// TaskSize returns the total task size in the db, it includes replicating, scheduling and absent tasks
func (c *Controller) TaskSize() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// the ddl span is a special span, we don't need to schedule it
	return len(c.allTasks)
}

// IsTableExists checks if the table exists in the db
func (c *Controller) IsTableExists(tableID int64) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	tm, ok := c.tableTasks[tableID]
	return ok && len(tm) > 0
}

// UpdateSchemaID will update the schema id of the table, and move the task to the new schema map.
// It is called when a DDL like `ALTER TABLE old_schema.old_tbl RENAME TO new_schema.new_tbl` is executed.
func (c *Controller) UpdateSchemaID(tableID, newSchemaID int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, span := range c.tableTasks[tableID] {
		oldSchemaID := span.GetSchemaID()
		// update schemaID
		span.SetSchemaID(newSchemaID)

		// update schema map
		schemaMap, ok := c.schemaTasks[oldSchemaID]
		if ok {
			delete(schemaMap, span.ID)
			// clear the map if empty
			if len(schemaMap) == 0 {
				delete(c.schemaTasks, oldSchemaID)
			}
		}
		// add it to new schema map
		newMap, ok := c.schemaTasks[newSchemaID]
		if !ok {
			newMap = make(map[common.DispatcherID]*replica.SpanReplication)
			c.schemaTasks[newSchemaID] = newMap
		}
		newMap[span.ID] = span
	}
}

// UpdateStatus updates the status of a span
func (c *Controller) UpdateStatus(span *replica.SpanReplication, status *heartbeatpb.TableSpanStatus) {
	span.UpdateStatus(status)

	if span == c.ddlSpan {
		// ddl span don't need check by checker
		return
	}
	// Note: a read lock is required inside the `GetGroupChecker` method.
	checker := c.GetGroupChecker(span.GetGroupID())

	c.mu.Lock()
	defer c.mu.Unlock()
	checker.UpdateStatus(span)
}

// AddAbsentReplicaSet adds absent replica sets
func (c *Controller) AddAbsentReplicaSet(spans ...*replica.SpanReplication) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.addAbsentReplicaSetWithoutLock(spans...)
}

// AddSchedulingReplicaSet adds scheduling replica sets
func (c *Controller) AddSchedulingReplicaSet(span *replica.SpanReplication, targetNodeID node.ID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.addSchedulingReplicaSetWithoutLock(span, targetNodeID)
}

// AddReplicatingSpan adds replicating span
func (c *Controller) AddReplicatingSpan(span *replica.SpanReplication) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.allTasks[span.ID] = span
	c.addToSchemaAndTableMap(span)
	c.AddReplicatingWithoutLock(span)
}

// MarkSpanAbsent marks span as absent
func (c *Controller) MarkSpanAbsent(span *replica.SpanReplication) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.MarkAbsentWithoutLock(span)
}

// MarkSpanScheduling marks span as scheduling
func (c *Controller) MarkSpanScheduling(span *replica.SpanReplication) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.MarkSchedulingWithoutLock(span)
}

// MarkSpanReplicating marks span as replicating
func (c *Controller) MarkSpanReplicating(span *replica.SpanReplication) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.MarkReplicatingWithoutLock(span)
}

// BindSpanToNode binds span to node
func (c *Controller) BindSpanToNode(old, new node.ID, span *replica.SpanReplication) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.BindReplicaToNodeWithoutLock(old, new, span)
}

// RemoveReplicatingSpan removes replicating span
func (c *Controller) RemoveReplicatingSpan(span *replica.SpanReplication) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeSpanWithoutLock(span)
}

// addAbsentReplicaSetWithoutLock adds spans to absent map
func (c *Controller) addAbsentReplicaSetWithoutLock(spans ...*replica.SpanReplication) {
	for _, span := range spans {
		c.allTasks[span.ID] = span
		c.AddAbsentWithoutLock(span)
		c.addToSchemaAndTableMap(span)
	}
}

// addSchedulingReplicaSetWithoutLock adds scheduling replica set without lock
func (c *Controller) addSchedulingReplicaSetWithoutLock(span *replica.SpanReplication, targetNodeID node.ID) {
	c.allTasks[span.ID] = span
	c.AddSchedulingReplicaWithoutLock(span, targetNodeID)
	c.addToSchemaAndTableMap(span)
}

// ReplaceReplicaSet replaces replica sets
func (c *Controller) ReplaceReplicaSet(
	oldReplications []*replica.SpanReplication,
	newSpans []*heartbeatpb.TableSpan,
	checkpointTs uint64,
	splitTargetNodes []node.ID,
) []*replica.SpanReplication {
	// we need to ensure the create the new spans and drop the old span should be protected by mutex
	// Then GetMinCheckpointTsForNonReplicatingSpans will not get incorrect min checkpoint ts
	c.mu.Lock()
	defer c.mu.Unlock()

	// 1. check if the old replica set exists
	for _, old := range oldReplications {
		if _, ok := c.allTasks[old.ID]; !ok {
			log.Panic("old replica set not found",
				zap.String("changefeed", c.changefeedID.Name()),
				zap.String("span", old.ID.String()))
		}
		oldCheckpointTs := old.GetStatus().GetCheckpointTs()
		if checkpointTs > oldCheckpointTs {
			checkpointTs = oldCheckpointTs
		}
		c.removeSpanWithoutLock(old)
	}

	// 2. create the new replica set
	var news []*replica.SpanReplication
	old := oldReplications[0]
	for _, span := range newSpans {
		new := replica.NewSpanReplication(
			old.ChangefeedID,
			common.NewDispatcherID(),
			old.GetSchemaID(),
			span, checkpointTs,
			old.GetMode())
		news = append(news, new)
	}

	if len(splitTargetNodes) > 0 && len(splitTargetNodes) == len(news) {
		// the spans have the target nodes
		for idx, newSpan := range news {
			c.addSchedulingReplicaSetWithoutLock(newSpan, splitTargetNodes[idx])
		}
	} else {
		c.addAbsentReplicaSetWithoutLock(news...)
	}

	return news
}

// IsDDLDispatcher checks if the dispatcher is a DDL dispatcher
func (c *Controller) IsDDLDispatcher(dispatcherID common.DispatcherID) bool {
	return dispatcherID == c.ddlDispatcherID
}

// GetDDLDispatcherID returns the DDL dispatcher ID
func (c *Controller) GetDDLDispatcherID() common.DispatcherID {
	return c.ddlDispatcherID
}

// GetDDLDispatcher returns the DDL dispatcher
func (c *Controller) GetDDLDispatcher() *replica.SpanReplication {
	return c.GetTaskByID(c.ddlDispatcherID)
}

// GetSplitter returns the splitter
func (c *Controller) GetSplitter() *split.Splitter {
	return c.splitter
}

// CheckByGroup checks by group
func (c *Controller) CheckByGroup(groupID pkgreplica.GroupID, batch int) pkgreplica.GroupCheckResult {
	checker := c.GetGroupChecker(groupID)

	c.mu.RLock()
	defer c.mu.RUnlock()
	return checker.Check(batch)
}

// RemoveAll reset the db and return all the replicating and scheduling tasks
func (c *Controller) RemoveAll() []*replica.SpanReplication {
	c.mu.Lock()
	defer c.mu.Unlock()

	tasks := make([]*replica.SpanReplication, 0)
	tasks = append(tasks, c.GetReplicatingWithoutLock()...)
	tasks = append(tasks, c.GetSchedulingWithoutLock()...)

	c.reset(c.ddlSpan)
	return tasks
}

// RemoveByTableIDs removes the tasks by the table ids and return the scheduled tasks.
// When the split dispatcher operator is running, a TRUNCATE TABLE DDL can potentially drop the dispatcher.
// This leads to the completion of the split dispatcher operator and the subsequent removal of the span.
// However, the operator callback may erroneously mark the span as absent. To avoid this situation,
// we should first remove the replicaSet and then remove the span to ensure it doesn't remain active.
func (c *Controller) RemoveByTableIDs(fn func(task *replica.SpanReplication), tableIDs ...int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, tblID := range tableIDs {
		for _, task := range c.tableTasks[tblID] {
			if task.IsScheduled() {
				fn(task)
			}
			c.removeSpanWithoutLock(task)
		}
	}
}

// RemoveBySchemaID removes the tasks by the schema id and return the scheduled tasks
// The order of removing the span and the replicaSet should align with the RemoveByTableIDs function.
func (c *Controller) RemoveBySchemaID(fn func(replicaSet *replica.SpanReplication), schemaID int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, task := range c.schemaTasks[schemaID] {
		if task.IsScheduled() {
			fn(task)
		}
		c.removeSpanWithoutLock(task)
	}
}

// removeSpanWithoutLock removes the spans from the db without lock
func (c *Controller) removeSpanWithoutLock(spans ...*replica.SpanReplication) {
	for _, span := range spans {
		c.RemoveReplicaWithoutLock(span)

		tableID := span.Span.TableID
		schemaID := span.GetSchemaID()
		delete(c.schemaTasks[schemaID], span.ID)
		delete(c.tableTasks[tableID], span.ID)
		if len(c.schemaTasks[schemaID]) == 0 {
			delete(c.schemaTasks, schemaID)
		}
		if len(c.tableTasks[tableID]) == 0 {
			delete(c.tableTasks, tableID)
		}
		delete(c.allTasks, span.ID)
	}
}

// addToSchemaAndTableMap adds the span to the schema and table map
func (c *Controller) addToSchemaAndTableMap(span *replica.SpanReplication) {
	tableID := span.Span.TableID
	schemaID := span.GetSchemaID()
	// modify the schema map
	schemaMap, ok := c.schemaTasks[schemaID]
	if !ok {
		schemaMap = make(map[common.DispatcherID]*replica.SpanReplication)
		c.schemaTasks[schemaID] = schemaMap
	}
	schemaMap[span.ID] = span

	// modify the table map
	tableMap, ok := c.tableTasks[tableID]
	if !ok {
		tableMap = make(map[common.DispatcherID]*replica.SpanReplication)
		c.tableTasks[tableID] = tableMap
	}
	tableMap[span.ID] = span
}

// GetAbsentForTest returns absent spans for testing
func (c *Controller) GetAbsentForTest(limit int) []*replica.SpanReplication {
	ret := c.GetAbsent()
	limit = min(limit, len(ret))
	return ret[:limit]
}

func (c Controller) GetkeyspaceID() uint32 {
	return c.keyspaceID
}
