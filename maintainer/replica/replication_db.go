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

package replica

import (
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
	"go.uber.org/zap"
)

var _ replica.ReplicationDB[common.DispatcherID, *SpanReplication] = &ReplicationDB{}

// ReplicationDB maintains the state of replication spans in memory. It tracks the mapping
// between dispatchers and spans, organizing them by schema and table IDs for efficient access.
// The struct provides thread-safe operations for managing span replication states (absent,
// scheduling, replicating) and handles DDL span separately.
type ReplicationDB struct {
	// changefeedID uniquely identifies the changefeed this ReplicationDB belongs to
	changefeedID common.ChangeFeedID
	// ddlSpan is a special span that handles DDL operations, it is always on the same node as the maintainer
	// so no need to schedule it
	ddlSpan *SpanReplication

	// mu protects concurrent access to [replica.ReplicationDB, ddlSpan, allTasks, schemaTasks, tableTasks]
	mu sync.RWMutex
	// ReplicationDB tracks the scheduling status of spans
	replica.ReplicationDB[common.DispatcherID, *SpanReplication]
	// allTasks maps dispatcher IDs to their spans, including table trigger dispatchers
	allTasks map[common.DispatcherID]*SpanReplication
	// schemaTasks provides quick access to spans by schema ID
	schemaTasks map[int64]map[common.DispatcherID]*SpanReplication
	// tableTasks provides quick access to spans by table ID
	tableTasks map[int64]map[common.DispatcherID]*SpanReplication

	// newGroupChecker creates a GroupChecker for validating span groups
	newGroupChecker func(groupID replica.GroupID) replica.GroupChecker[common.DispatcherID, *SpanReplication]
}

// NewReplicaSetDB creates a new ReplicationDB and initializes the maps
func NewReplicaSetDB(
	changefeedID common.ChangeFeedID, ddlSpan *SpanReplication, enableTableAcrossNodes bool,
) *ReplicationDB {
	db := &ReplicationDB{
		changefeedID:    changefeedID,
		ddlSpan:         ddlSpan,
		newGroupChecker: getNewGroupChecker(changefeedID, enableTableAcrossNodes),
	}

	db.reset(db.ddlSpan)
	return db
}

func (db *ReplicationDB) GetDDLDispatcher() *SpanReplication {
	return db.ddlSpan
}

// GetTaskByID returns the replica set by the id, it will search the replicating, scheduling and absent map
func (db *ReplicationDB) GetTaskByID(id common.DispatcherID) *SpanReplication {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.allTasks[id]
}

// TaskSize returns the total task size in the db, it includes replicating, scheduling and absent tasks
func (db *ReplicationDB) TaskSize() int {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// the ddl span is a special span, we don't need to schedule it
	return len(db.allTasks)
}

// RemoveAll reset the db and return all the replicating and scheduling tasks
func (db *ReplicationDB) RemoveAll() []*SpanReplication {
	db.mu.Lock()
	defer db.mu.Unlock()

	tasks := make([]*SpanReplication, 0)
	tasks = append(tasks, db.GetReplicatingWithoutLock()...)
	tasks = append(tasks, db.GetSchedulingWithoutLock()...)

	db.reset(db.ddlSpan)
	return tasks
}

// RemoveTasksByTableIDs removes the tasks by the table ids and return the scheduled tasks
func (db *ReplicationDB) RemoveByTableIDs(tableIDs ...int64) []*SpanReplication {
	db.mu.Lock()
	defer db.mu.Unlock()

	tasks := make([]*SpanReplication, 0)
	for _, tblID := range tableIDs {
		for _, task := range db.tableTasks[tblID] {
			db.removeSpanWithoutLock(task)
			if task.IsScheduled() {
				tasks = append(tasks, task)
			}
		}
	}
	return tasks
}

// RemoveBySchemaID removes the tasks by the schema id and return the scheduled tasks
func (db *ReplicationDB) RemoveBySchemaID(schemaID int64) []*SpanReplication {
	db.mu.Lock()
	defer db.mu.Unlock()

	tasks := make([]*SpanReplication, 0)
	for _, task := range db.schemaTasks[schemaID] {
		db.removeSpanWithoutLock(task)
		if task.IsScheduled() {
			tasks = append(tasks, task)
		}
	}
	return tasks
}

// GetTasksByTableID returns the spans by the table id
func (db *ReplicationDB) GetTasksByTableID(tableID int64) []*SpanReplication {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var tasks []*SpanReplication
	for _, task := range db.tableTasks[tableID] {
		tasks = append(tasks, task)
	}
	return tasks
}

// GetAllTasks returns all the spans in the db, it will also return the ddl span
func (db *ReplicationDB) GetAllTasks() []*SpanReplication {
	db.mu.RLock()
	defer db.mu.RUnlock()

	tasks := make([]*SpanReplication, 0, len(db.allTasks))
	for _, task := range db.allTasks {
		tasks = append(tasks, task)
	}
	return tasks
}

// IsTableExists checks if the table exists in the db
func (db *ReplicationDB) IsTableExists(tableID int64) bool {
	db.mu.RLock()
	defer db.mu.RUnlock()

	tm, ok := db.tableTasks[tableID]
	return ok && len(tm) > 0
}

// GetTaskSizeBySchemaID returns the size of the task by the schema id
func (db *ReplicationDB) GetTaskSizeBySchemaID(schemaID int64) int {
	db.mu.RLock()
	defer db.mu.RUnlock()

	sm, ok := db.schemaTasks[schemaID]
	if ok {
		return len(sm)
	}
	return 0
}

// GetTasksBySchemaID returns the spans by the schema id
func (db *ReplicationDB) GetTasksBySchemaID(schemaID int64) []*SpanReplication {
	db.mu.RLock()
	defer db.mu.RUnlock()

	sm, ok := db.schemaTasks[schemaID]
	if !ok {
		return nil
	}
	replicaSets := make([]*SpanReplication, 0, len(sm))
	for _, v := range sm {
		replicaSets = append(replicaSets, v)
	}
	return replicaSets
}

// ReplaceReplicaSet replaces the old replica set with the new ones
func (db *ReplicationDB) ReplaceReplicaSet(
	oldReplications []*SpanReplication,
	newSpans []*heartbeatpb.TableSpan,
	checkpointTs uint64,
) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 1. check if the old replica set exists
	for _, old := range oldReplications {
		if _, ok := db.allTasks[old.ID]; !ok {
			log.Panic("old replica set not found",
				zap.String("changefeed", db.changefeedID.Name()),
				zap.String("span", old.ID.String()))
		}
		oldCheckpointTs := old.GetStatus().GetCheckpointTs()
		if checkpointTs > oldCheckpointTs {
			checkpointTs = oldCheckpointTs
		}
		db.removeSpanWithoutLock(old)
	}

	// 2. create the new replica set
	var news []*SpanReplication
	old := oldReplications[0]
	for _, span := range newSpans {
		new := NewSpanReplication(
			old.ChangefeedID,
			common.NewDispatcherID(),
			old.GetPDClock(),
			old.GetSchemaID(),
			span, checkpointTs)
		news = append(news, new)
	}

	// 3. add the new replica set to the db
	db.addAbsentReplicaSetWithoutLock(news...)
}

// AddReplicatingSpan adds a replicating span to the replicating map, that means the span is already scheduled to a dispatcher
func (db *ReplicationDB) AddReplicatingSpan(span *SpanReplication) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.allTasks[span.ID] = span
	db.addToSchemaAndTableMap(span)
	db.AddReplicatingWithoutLock(span)
}

// AddAbsentReplicaSet adds spans to the absent map
func (db *ReplicationDB) AddAbsentReplicaSet(spans ...*SpanReplication) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.addAbsentReplicaSetWithoutLock(spans...)
}

// MarkSpanAbsent move the span to the absent status
func (db *ReplicationDB) MarkSpanAbsent(span *SpanReplication) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.MarkAbsentWithoutLock(span)
}

// MarkSpanScheduling move the span to the scheduling map
func (db *ReplicationDB) MarkSpanScheduling(span *SpanReplication) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.MarkSchedulingWithoutLock(span)
}

// MarkSpanReplicating move the span to the replicating map
func (db *ReplicationDB) MarkSpanReplicating(span *SpanReplication) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.MarkReplicatingWithoutLock(span)
}

// ForceRemove remove the span from the db
func (db *ReplicationDB) ForceRemove(id common.DispatcherID) {
	db.mu.Lock()
	defer db.mu.Unlock()
	span, ok := db.allTasks[id]
	if !ok {
		log.Warn("span not found, ignore remove action",
			zap.String("changefeed", db.changefeedID.Name()),
			zap.String("span", id.String()))
		return
	}

	log.Info("remove a span",
		zap.String("changefeed", db.changefeedID.Name()),
		zap.String("dispatcher", id.String()),
		zap.String("span", common.FormatTableSpan(span.Span)))

	db.removeSpanWithoutLock(span)
}

// UpdateSchemaID will update the schema id of the table, and move the task to the new schema map.
// It is called when a DDL like `ALTER TABLE old_schema.old_tbl RENAME TO new_schema.new_tbl` is executed.
func (db *ReplicationDB) UpdateSchemaID(tableID, newSchemaID int64) {
	db.mu.Lock()
	defer db.mu.Unlock()

	for _, span := range db.tableTasks[tableID] {
		oldSchemaID := span.GetSchemaID()
		// update schemaID
		span.SetSchemaID(newSchemaID)

		// update schema map
		schemaMap, ok := db.schemaTasks[oldSchemaID]
		if ok {
			delete(schemaMap, span.ID)
			// clear the map if empty
			if len(schemaMap) == 0 {
				delete(db.schemaTasks, oldSchemaID)
			}
		}
		// add it to new schema map
		newMap, ok := db.schemaTasks[newSchemaID]
		if !ok {
			newMap = make(map[common.DispatcherID]*SpanReplication)
			db.schemaTasks[newSchemaID] = newMap
		}
		newMap[span.ID] = span
	}
}

func (db *ReplicationDB) UpdateStatus(span *SpanReplication, status *heartbeatpb.TableSpanStatus) {
	span.UpdateStatus(status)
	// Note: a read lock is required inside the `GetGroupChecker` method.
	checker := db.GetGroupChecker(span.GetGroupID())

	db.mu.Lock()
	defer db.mu.Unlock()
	checker.UpdateStatus(span)
}

// BindSpanToNode binds the span to new node, it will remove the span from the old node and add it to the new node
// It also marks the span as scheduling.
func (db *ReplicationDB) BindSpanToNode(old, new node.ID, span *SpanReplication) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.BindReplicaToNodeWithoutLock(old, new, span)
}

// addAbsentReplicaSetWithoutLock adds spans to absent map
func (db *ReplicationDB) addAbsentReplicaSetWithoutLock(spans ...*SpanReplication) {
	for _, span := range spans {
		db.allTasks[span.ID] = span
		db.AddAbsentWithoutLock(span)
		db.addToSchemaAndTableMap(span)
	}
}

// removeSpanWithoutLock removes the spans from the db without lock
func (db *ReplicationDB) removeSpanWithoutLock(spans ...*SpanReplication) {
	for _, span := range spans {
		db.RemoveReplicaWithoutLock(span)

		tableID := span.Span.TableID
		schemaID := span.GetSchemaID()
		delete(db.schemaTasks[schemaID], span.ID)
		delete(db.tableTasks[tableID], span.ID)
		if len(db.schemaTasks[schemaID]) == 0 {
			delete(db.schemaTasks, schemaID)
		}
		if len(db.tableTasks[tableID]) == 0 {
			delete(db.tableTasks, tableID)
		}
		delete(db.allTasks, span.ID)
	}
}

// addToSchemaAndTableMap adds the span to the schema and table map
func (db *ReplicationDB) addToSchemaAndTableMap(span *SpanReplication) {
	tableID := span.Span.TableID
	schemaID := span.GetSchemaID()
	// modify the schema map
	schemaMap, ok := db.schemaTasks[schemaID]
	if !ok {
		schemaMap = make(map[common.DispatcherID]*SpanReplication)
		db.schemaTasks[schemaID] = schemaMap
	}
	schemaMap[span.ID] = span

	// modify the table map
	tableMap, ok := db.tableTasks[tableID]
	if !ok {
		tableMap = make(map[common.DispatcherID]*SpanReplication)
		db.tableTasks[tableID] = tableMap
	}
	tableMap[span.ID] = span
}

func (db *ReplicationDB) GetAbsentForTest(_ []*SpanReplication, maxSize int) []*SpanReplication {
	ret := db.GetAbsent()
	maxSize = min(maxSize, len(ret))
	return ret[:maxSize]
}

// Optimize the lock usage, maybe control the lock within checker
func (db *ReplicationDB) CheckByGroup(groupID replica.GroupID, batch int) replica.GroupCheckResult {
	checker := db.GetGroupChecker(groupID)

	db.mu.RLock()
	defer db.mu.RUnlock()
	return checker.Check(batch)
}

// doWithRLock is a helper function to execute the action with a read lock
func (db *ReplicationDB) doWithRLock(action func()) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	action()
}

// reset resets the maps of ReplicationDB
func (db *ReplicationDB) reset(ddlSpan *SpanReplication) {
	db.schemaTasks = make(map[int64]map[common.DispatcherID]*SpanReplication)
	db.tableTasks = make(map[int64]map[common.DispatcherID]*SpanReplication)
	db.allTasks = make(map[common.DispatcherID]*SpanReplication)
	db.ReplicationDB = replica.NewReplicationDB(db.changefeedID.String(), db.doWithRLock, db.newGroupChecker)
	db.initializeDDLSpan(ddlSpan)
}

func (db *ReplicationDB) initializeDDLSpan(ddlSpan *SpanReplication) {
	// we don't need to schedule the ddl span, but added it to the allTasks map, so we can access it by id
	db.allTasks[ddlSpan.ID] = ddlSpan
	// dispatcher will report a block event with table ID 0,
	// so we need to add it to the table map
	db.tableTasks[ddlSpan.Span.TableID] = map[common.DispatcherID]*SpanReplication{
		ddlSpan.ID: ddlSpan,
	}
	// also put it to the schema map
	db.schemaTasks[ddlSpan.schemaID] = map[common.DispatcherID]*SpanReplication{
		ddlSpan.ID: ddlSpan,
	}
}
