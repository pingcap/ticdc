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

package changefeed

import (
	"fmt"
	"math"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
	"go.uber.org/zap"
)

// ChangefeedDB is an in memory data struct that maintains all changefeeds
type ChangefeedDB struct {
	id                     string
	changefeeds            map[common.ChangeFeedID]*Changefeed
	changefeedDisplayNames map[common.ChangeFeedDisplayName]common.ChangeFeedID

	replica.ReplicationDB[common.ChangeFeedID, *Changefeed]

	// stopped changefeeds that failed, stopped or finished
	stopped map[common.ChangeFeedID]*Changefeed
	lock    sync.RWMutex
}

func NewChangefeedDB(version int64) *ChangefeedDB {
	db := &ChangefeedDB{
		// id is the unique id of the changefeed db. The prefix `coordinator` distinguishes
		// it from other ReplicationDB. The suffix is the version of the coordinator, which
		// is useful to track the scheduling history.
		id:                     fmt.Sprintf("coordinator-%d", version),
		changefeeds:            make(map[common.ChangeFeedID]*Changefeed),
		changefeedDisplayNames: make(map[common.ChangeFeedDisplayName]common.ChangeFeedID),
		stopped:                make(map[common.ChangeFeedID]*Changefeed),
	}
	db.ReplicationDB = replica.NewReplicationDB[common.ChangeFeedID, *Changefeed](db.id,
		db.withRLock, replica.NewEmptyChecker)
	return db
}

func (db *ChangefeedDB) Init(allChangefeeds map[common.ChangeFeedID]*Changefeed) {
	db.lock.Lock()
	defer db.lock.Unlock()
	// Only add the changefeed to the changefeedDB and do nothing
	for _, cf := range allChangefeeds {
		db.changefeeds[cf.ID] = cf
		db.changefeedDisplayNames[cf.ID.DisplayName] = cf.ID
	}
}

func (db *ChangefeedDB) withRLock(action func()) {
	db.lock.RLock()
	defer db.lock.RUnlock()
	action()
}

// AddAbsentChangefeed adds the changefeed to the absent map
// It will be scheduled later
func (db *ChangefeedDB) AddAbsentChangefeed(tasks ...*Changefeed) {
	db.lock.Lock()
	defer db.lock.Unlock()

	for _, task := range tasks {
		db.changefeeds[task.ID] = task
		db.changefeedDisplayNames[task.ID.DisplayName] = task.ID
		db.AddAbsentWithoutLock(task)
	}
}

// AddStoppedChangefeed adds the changefeed to the stop map
func (db *ChangefeedDB) AddStoppedChangefeed(task *Changefeed) {
	db.lock.Lock()
	defer db.lock.Unlock()

	db.changefeeds[task.ID] = task
	db.changefeedDisplayNames[task.ID.DisplayName] = task.ID
	db.stopped[task.ID] = task
}

// AddReplicatingMaintainer adds a replicating the replicating map, that means the task is already scheduled to a maintainer
func (db *ChangefeedDB) AddReplicatingMaintainer(task *Changefeed, nodeID node.ID) {
	db.lock.Lock()
	defer db.lock.Unlock()

	task.SetNodeID(nodeID)
	log.Info("add an replicating maintainer",
		zap.String("nodeID", nodeID.String()),
		zap.String("changefeed", task.ID.String()))

	db.changefeeds[task.ID] = task
	db.changefeedDisplayNames[task.ID.DisplayName] = task.ID
	db.AddReplicatingWithoutLock(task)
}

// StopByChangefeedID stop a changefeed by the changefeed id
// if remove is true, it will remove the changefeed from the changefeed DB
// if remove is false, moves task to stopped map
// if the changefeed is scheduled, it will return the scheduled node
func (db *ChangefeedDB) StopByChangefeedID(cfID common.ChangeFeedID, remove bool) node.ID {
	db.lock.Lock()
	defer db.lock.Unlock()

	cf, ok := db.changefeeds[cfID]
	if !ok {
		return ""
	}

	// Remove from replication tracking
	db.RemoveReplicaWithoutLock(cf)

	nodeID := cf.GetNodeID()
	if nodeID != "" {
		cf.SetNodeID("")
	}

	if remove {
		log.Info("remove changefeed", zap.String("changefeed", cf.ID.String()))
		delete(db.changefeeds, cfID)
		delete(db.changefeedDisplayNames, cf.ID.DisplayName)
		delete(db.stopped, cfID)
	} else {
		log.Info("stop changefeed", zap.String("changefeed", cfID.String()))
		db.stopped[cfID] = cf
	}

	metrics.ChangefeedStatusGauge.DeleteLabelValues(cfID.Keyspace(), cfID.Name())
	metrics.ChangefeedCheckpointTsLagGauge.DeleteLabelValues(cfID.Keyspace(), cfID.Name())

	return nodeID
}

// GetSize returns the size of the all chagnefeeds
func (db *ChangefeedDB) GetSize() int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return len(db.changefeeds)
}

// GetStoppedSize returns the size of the stopped changefeeds
func (db *ChangefeedDB) GetStoppedSize() int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return len(db.stopped)
}

// GetAllChangefeeds returns all changefeeds
func (db *ChangefeedDB) GetAllChangefeeds() []*Changefeed {
	db.lock.RLock()
	defer db.lock.RUnlock()

	cfs := make([]*Changefeed, 0, len(db.changefeeds))
	for _, cf := range db.changefeeds {
		cfs = append(cfs, cf)
	}
	return cfs
}

// GetAllChangefeedsByKeyspace returns all changefeeds by keyspace
func (db *ChangefeedDB) GetAllChangefeedsByKeyspace(keyspace string) []*Changefeed {
	db.lock.RLock()
	defer db.lock.RUnlock()

	cfs := make([]*Changefeed, 0, len(db.changefeeds))
	for _, cf := range db.changefeeds {
		if cf.ID.DisplayName.Keyspace != keyspace {
			continue
		}
		cfs = append(cfs, cf)
	}
	return cfs
}

// BindChangefeedToNode binds the changefeed to the node, it will remove the task from the old node and add it to the new node
// ,and it also marks the task as scheduling
func (db *ChangefeedDB) BindChangefeedToNode(old, new node.ID, task *Changefeed) {
	db.lock.Lock()
	defer db.lock.Unlock()

	log.Info("bind changefeed to node",
		zap.String("changefeed", task.ID.String()),
		zap.String("oldNode", old.String()),
		zap.String("node", new.String()))
	db.BindReplicaToNodeWithoutLock(old, new, task)
}

// MarkMaintainerReplicating move the maintainer to the replicating map
func (db *ChangefeedDB) MarkMaintainerReplicating(task *Changefeed) {
	db.lock.Lock()
	defer db.lock.Unlock()

	log.Info("marking changefeed replicating",
		zap.String("changefeed", task.ID.String()))
	db.MarkReplicatingWithoutLock(task)
	task.StartFinished()
}

// GetWaitingSchedulingChangefeeds returns the absent maintainers and the working state of each node
func (db *ChangefeedDB) GetWaitingSchedulingChangefeeds(maxSize int) ([]*Changefeed, map[node.ID]int) {
	absent := db.GetAbsent()
	if len(absent) > maxSize {
		absent = absent[:maxSize]
	}
	nodeSize := db.GetTaskSizePerNode()
	return absent, nodeSize
}

func (db *ChangefeedDB) GetByID(id common.ChangeFeedID) *Changefeed {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return db.changefeeds[id]
}

func (db *ChangefeedDB) GetByChangefeedDisplayName(displayName common.ChangeFeedDisplayName) *Changefeed {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return db.changefeeds[db.changefeedDisplayNames[displayName]]
}

func (db *ChangefeedDB) Foreach(fn func(*Changefeed)) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	for _, cf := range db.changefeeds {
		fn(cf)
	}
}

// MoveToSchedulingQueue moves a changefeed to the absent map, and waiting for scheduling
func (db *ChangefeedDB) MoveToSchedulingQueue(
	id common.ChangeFeedID,
	resetBackoff bool,
	overwriteCheckpointTs bool,
) {
	db.lock.Lock()
	defer db.lock.Unlock()

	cf := db.changefeeds[id]
	if cf != nil {
		// Reset the backoff if resetBackoff is true.
		// It means the changefeed is resumed by cli or API.
		if resetBackoff {
			cf.backoff.resetErrRetry()
		}
		delete(db.stopped, id)
		cf.isNew = overwriteCheckpointTs
		db.AddAbsentWithoutLock(cf)
		log.Info("move a changefeed to scheduling queue, it will be scheduled later",
			zap.Stringer("changefeed", id),
			zap.Uint64("checkpointTs", cf.GetStatus().CheckpointTs),
			zap.Bool("overwriteCheckpointTs", overwriteCheckpointTs),
			zap.Time("nextScheduleTime", cf.backoff.nextRetryTime.Load()),
		)
	}
}

func (db *ChangefeedDB) GetByNodeID(id node.ID) []*Changefeed {
	return db.GetTaskByNodeID(id)
}

// MarkMaintainerAbsent move the maintainer to the absent Status
func (db *ChangefeedDB) MarkMaintainerAbsent(cf *Changefeed) {
	db.lock.Lock()
	defer db.lock.Unlock()

	log.Info("marking changefeed absent",
		zap.String("changefeed", cf.ID.String()),
		zap.String("node", cf.GetNodeID().String()))
	db.MarkAbsentWithoutLock(cf)
}

// MarkMaintainerScheduling move the maintainer to the scheduling map
func (db *ChangefeedDB) MarkMaintainerScheduling(cf *Changefeed) {
	db.lock.Lock()
	defer db.lock.Unlock()

	log.Info("marking changefeed scheduling",
		zap.String("ChangefeedDB", cf.ID.String()))
	db.MarkSchedulingWithoutLock(cf)
}

// CalculateGlobalGCSafepoint calculates the global minimum checkpointTs of all changefeeds that replicating the upstream TiDB cluster.
func (db *ChangefeedDB) CalculateGlobalGCSafepoint() uint64 {
	db.lock.RLock()
	defer db.lock.RUnlock()

	var minCpts uint64 = math.MaxUint64

	for _, cf := range db.changefeeds {
		info := cf.GetInfo()
		if info == nil || !info.NeedBlockGC() {
			continue
		}
		checkpointTs := cf.GetLastSavedCheckPointTs()
		if minCpts > checkpointTs {
			minCpts = checkpointTs
		}
	}
	return minCpts
}

// CalculateKeyspaceGCBarrier calculates the minimum keyspace-based checkpointTs of all changefeeds that replicating the upstream TiDB cluster.
func (db *ChangefeedDB) CalculateKeyspaceGCBarrier() map[common.KeyspaceMeta]uint64 {
	db.lock.RLock()
	defer db.lock.RUnlock()

	keyspaceGCBarrier := make(map[common.KeyspaceMeta]uint64)
	for _, cf := range db.changefeeds {
		info := cf.GetInfo()
		if info == nil || !info.NeedBlockGC() {
			continue
		}

		meta := common.KeyspaceMeta{
			ID:   info.KeyspaceID,
			Name: cf.ID.Keyspace(),
		}

		minCpts := keyspaceGCBarrier[meta]
		if minCpts == 0 {
			minCpts = math.MaxUint64
		}

		checkpointTs := cf.GetLastSavedCheckPointTs()
		if minCpts > checkpointTs {
			keyspaceGCBarrier[meta] = checkpointTs
		}

	}
	return keyspaceGCBarrier
}

// ReplaceStoppedChangefeed updates the stopped changefeed
func (db *ChangefeedDB) ReplaceStoppedChangefeed(cf *config.ChangeFeedInfo) {
	db.lock.Lock()
	defer db.lock.Unlock()

	oldCf := db.stopped[cf.ChangefeedID]
	if oldCf == nil {
		log.Warn("changefeed is not stopped, can not be updated", zap.String("changefeed", cf.ChangefeedID.String()))
		return
	}
	// todo: not create a new changefeed here?
	newCf := NewChangefeed(cf.ChangefeedID, cf, oldCf.GetStatus().CheckpointTs, false)
	db.stopped[cf.ChangefeedID] = newCf
	db.changefeeds[cf.ChangefeedID] = newCf
	db.changefeedDisplayNames[cf.ChangefeedID.DisplayName] = newCf.ID
}

func (db *ChangefeedDB) UpdateLogCoordinatorResolvedTsByID(changefeedID common.ChangeFeedID, resolvedTs uint64) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	cf := db.changefeeds[changefeedID]
	if cf != nil {
		cf.SetLogCoordinatorResolvedTs(resolvedTs)
	}
}

func (db *ChangefeedDB) GetLogCoordinatorResolvedTsByName(name common.ChangeFeedDisplayName) uint64 {
	db.lock.RLock()
	defer db.lock.RUnlock()

	cf := db.changefeeds[db.changefeedDisplayNames[name]]
	if cf != nil {
		return cf.GetLogCoordinatorResolvedTs()
	}
	return 0
}

func (db *ChangefeedDB) GetChangefeedIDByName(name common.ChangeFeedDisplayName) common.ChangeFeedID {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return db.changefeedDisplayNames[name]
}
