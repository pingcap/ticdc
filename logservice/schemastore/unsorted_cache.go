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

package schemastore

import (
	"sync"

	"github.com/google/btree"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type ddlCache struct {
	mutex sync.Mutex
	// ordered by FinishedTs
	ddlEvents *btree.BTreeG[DDLJobWithCommitTs]
}

func ddlEventLess(a, b DDLJobWithCommitTs) bool {
	// TODO: commitTs or finishedTs
	return a.CommitTs < b.CommitTs ||
		(a.CommitTs == b.CommitTs && a.Job.BinlogInfo.SchemaVersion < b.Job.BinlogInfo.SchemaVersion)
}

func newDDLCache() *ddlCache {
	return &ddlCache{
		ddlEvents: btree.NewG[DDLJobWithCommitTs](16, ddlEventLess),
	}
}

func (c *ddlCache) addDDLEvent(ddlEvent DDLJobWithCommitTs) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	oldEvent, duplicated := c.ddlEvents.ReplaceOrInsert(ddlEvent)
	if duplicated {
		log.Debug("ignore duplicated DDL event",
			zap.Any("event", ddlEvent),
			zap.Any("oldEvent", oldEvent))
	}
}

func (c *ddlCache) fetchSortedDDLEventBeforeTS(ts uint64) []DDLJobWithCommitTs {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	events := make([]DDLJobWithCommitTs, 0)
	c.ddlEvents.Ascend(func(event DDLJobWithCommitTs) bool {
		if event.CommitTs <= ts {
			events = append(events, event)
			return true
		}
		return false
	})
	for _, event := range events {
		c.ddlEvents.Delete(event)
	}
	if len(events) > 0 {
		log.Info("schema store resolved ddl events", zap.Uint64("resolvedTs", ts), zap.Int("count", len(events)))
	}
	return events
}
