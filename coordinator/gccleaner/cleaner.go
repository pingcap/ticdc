// Copyright 2026 PingCAP, Inc.
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

package gccleaner

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/txnutil/gc"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

const (
	maxTasksPerTick        = 4
	clearSafepointTimeout  = 10 * time.Second
	maxCleanerBackoffDelay = 30 * time.Second
)

type task struct {
	changefeedID common.ChangeFeedID
	keyspaceID   uint32
	tag          string
}

type pendingEntry struct {
	keyspaceID uint32

	// We intentionally track creating/resuming separately instead of storing a single "tag".
	//
	// A changefeed can have more than one ensure-GC safepoint pending cleanup at the same time.
	// For example:
	// - Create changefeed sets serviceID with "-creating-" and it is queued for cleanup.
	// - If coordinator cannot update GC safepoint for a while (tick/update failures), cleanup is delayed.
	// - Later, an overwrite-resume may happen and sets another serviceID with "-resuming-".
	// In this case, we must clean up BOTH serviceIDs; keeping only one tag would silently drop one cleanup task
	// and leave the corresponding safepoint/barrier behind until TTL expiration.
	//
	// In addition, each tag must be gated by its own "added epoch": a tag added in the current GC tick
	// must not be undone until a later tick has successfully updated GC safepoint.
	creating bool
	resuming bool

	creatingAtEpoch uint64
	resumingAtEpoch uint64
}

// Cleaner cleans up changefeed-level service GC safepoints/barriers set when creating or resuming changefeeds.
// It is triggered after coordinator successfully updates the cluster-level GC state
// (global GC safepoint on classic, or keyspace GC barrier on next-gen).
type Cleaner struct {
	pdClient pd.Client

	// gcServiceIDPrefix is the prefix used to build the service ID passed to PD.
	gcServiceIDPrefix string

	mu sync.Mutex
	// updateEpoch increases at the beginning of each coordinator GC tick.
	updateEpoch uint64
	// lastSucceededEpoch is updated after a successful GC safepoint update.
	// Only tasks added before this epoch are safe to undo.
	lastSucceededEpoch uint64
	// changefeedID -> entry
	pending map[common.ChangeFeedID]*pendingEntry

	triggerCh chan struct{}
}

func New(
	pdClient pd.Client,
	gcServiceIDPrefix string,
) *Cleaner {
	return &Cleaner{
		pdClient:          pdClient,
		gcServiceIDPrefix: gcServiceIDPrefix,
		pending:           make(map[common.ChangeFeedID]*pendingEntry),
		triggerCh:         make(chan struct{}, 1),
	}
}

func (c *Cleaner) Add(changefeedID common.ChangeFeedID, keyspaceID uint32, tag string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry := c.pending[changefeedID]
	if entry == nil {
		entry = &pendingEntry{}
		c.pending[changefeedID] = entry
	}
	entry.keyspaceID = keyspaceID

	switch tag {
	case gc.EnsureGCServiceCreating:
		entry.creating = true
		entry.creatingAtEpoch = c.updateEpoch
	case gc.EnsureGCServiceResuming:
		entry.resuming = true
		entry.resumingAtEpoch = c.updateEpoch
	default:
		log.Warn("unknown gc service tag, skip",
			zap.String("gcServiceTag", tag),
			zap.String("changefeed", changefeedID.String()))
	}
}

func (c *Cleaner) BeginGCTick() {
	c.mu.Lock()
	c.updateEpoch++
	c.mu.Unlock()
}

func (c *Cleaner) OnUpdateGCSafepointSucceeded() {
	c.mu.Lock()
	c.lastSucceededEpoch = c.updateEpoch
	c.mu.Unlock()

	c.trigger()
}

func (c *Cleaner) trigger() {
	select {
	case c.triggerCh <- struct{}{}:
	default:
	}
}

// PendingLen returns the number of changefeeds with pending cleanup tasks.
func (c *Cleaner) PendingLen() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.pending)
}

func (c *Cleaner) Run(ctx context.Context) error {
	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-c.triggerCh:
			ok := c.tryClearEnsureGCSafepoint(ctx)
			if ok {
				backoff = time.Second
				continue
			}

			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > maxCleanerBackoffDelay {
				backoff = maxCleanerBackoffDelay
			}
		}
	}
}

func (c *Cleaner) getEnsureGCServiceID(tag string) string {
	return c.gcServiceIDPrefix + tag
}

func (c *Cleaner) takeReady(maxTasks int) []task {
	c.mu.Lock()
	defer c.mu.Unlock()

	epoch := c.lastSucceededEpoch
	tasks := make([]task, 0, maxTasks)
	for changefeedID, entry := range c.pending {
		if len(tasks) >= maxTasks {
			break
		}
		if entry.creating && entry.creatingAtEpoch < epoch && len(tasks) < maxTasks {
			tasks = append(tasks, task{
				changefeedID: changefeedID,
				keyspaceID:   entry.keyspaceID,
				tag:          gc.EnsureGCServiceCreating,
			})
			entry.creating = false
		}
		if entry.resuming && entry.resumingAtEpoch < epoch && len(tasks) < maxTasks {
			tasks = append(tasks, task{
				changefeedID: changefeedID,
				keyspaceID:   entry.keyspaceID,
				tag:          gc.EnsureGCServiceResuming,
			})
			entry.resuming = false
		}
		if !entry.creating && !entry.resuming {
			delete(c.pending, changefeedID)
		}
	}
	return tasks
}

func (c *Cleaner) requeue(tasks []task) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, task := range tasks {
		entry := c.pending[task.changefeedID]
		if entry == nil {
			entry = &pendingEntry{}
			c.pending[task.changefeedID] = entry
		}
		entry.keyspaceID = task.keyspaceID
		switch task.tag {
		case gc.EnsureGCServiceCreating:
			entry.creating = true
			entry.creatingAtEpoch = c.updateEpoch
		case gc.EnsureGCServiceResuming:
			entry.resuming = true
			entry.resumingAtEpoch = c.updateEpoch
		default:
			log.Warn("unknown gc service tag, skip",
				zap.String("gcServiceTag", task.tag),
				zap.String("changefeed", task.changefeedID.String()))
		}
	}
}

// tryClearEnsureGCSafepoint clears up to maxTasksPerTick ensure-GC safepoints.
//
// It returns true when the current run finishes successfully (or there is nothing to do),
// and returns false after the first failure (the remaining tasks are re-queued).
//
// We intentionally return bool instead of error because the caller only needs to decide
// whether to apply backoff and retry, while the error details are logged here.
func (c *Cleaner) tryClearEnsureGCSafepoint(ctx context.Context) bool {
	tasks := c.takeReady(maxTasksPerTick)
	for i, task := range tasks {
		childCtx, cancel := context.WithTimeout(ctx, clearSafepointTimeout)
		gcServiceID := c.getEnsureGCServiceID(task.tag)
		err := gc.UndoEnsureChangefeedStartTsSafety(childCtx, c.pdClient, task.keyspaceID, gcServiceID, task.changefeedID)
		cancel()
		if err == nil {
			continue
		}

		switch task.tag {
		case gc.EnsureGCServiceCreating:
			log.Warn("failed to delete create changefeed gc safepoint", zap.Error(err))
		case gc.EnsureGCServiceResuming:
			log.Warn("failed to delete resume changefeed gc safepoint", zap.Error(err))
		default:
			log.Warn("failed to delete changefeed gc safepoint",
				zap.String("gcServiceTag", task.tag),
				zap.Error(err))
		}

		c.requeue(tasks[i:])
		return false
	}
	return true
}
