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
		db.changefeedDisplayNames[task.ID.DisplayName] = task.ID
		db.changefeeds[task.ID] = task
		db.AddAbsentWithoutLock(task)
	}
}

// AddStoppedChangefeed adds the changefeed to the stop map
func (db *ChangefeedDB) AddStoppedChangefeed(task *Changefeed) {
	db.lock.Lock()
	defer db.lock.Unlock()

	db.changefeeds[task.ID] = task
	db.stopped[task.ID] = task
	db.changefeedDisplayNames[task.ID.DisplayName] = task.ID
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
// if remove is true, it will remove the changefeed from the chagnefeed DB
// if remove is false, moves task to stopped map
// if the changefeed is scheduled, it will return the scheduled node
func (db *ChangefeedDB) StopByChangefeedID(cfID common.ChangeFeedID, remove bool) node.ID {
	db.lock.Lock()
	defer db.lock.Unlock()

	cf, ok := db.changefeeds[cfID]
	if ok {
		// remove the changefeed
		delete(db.changefeeds, cfID)
		delete(db.stopped, cf.ID)
		db.RemoveReplicaWithoutLock(cf)

		if remove {
			log.Info("remove changefeed", zap.String("changefeed", cf.ID.String()))
		} else {
			log.Info("stop changefeed", zap.String("changefeed", cfID.String()))
			// push back to stopped
			db.changefeeds[cfID] = cf
			db.stopped[cfID] = cf
		}

		nodeID := cf.GetNodeID()
		if cf.GetNodeID() == "" {
			log.Info("changefeed is not scheduled, delete directly")
			return ""
		}
		cf.SetNodeID("")
		return nodeID
	}
	return ""
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
func (db *ChangefeedDB) GetWaitingSchedulingChangefeeds(absent []*Changefeed, maxSize int) ([]*Changefeed, map[node.ID]int) {
	absent = db.GetAbsent()
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

// CalculateGCSafepoint calculates the minimum checkpointTs of all changefeeds that replicating the upstream TiDB cluster.
func (db *ChangefeedDB) CalculateGCSafepoint() uint64 {
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
}
