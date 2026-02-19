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

package watcher

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// errChangefeedKeyDeleted is a sentinel error indicating that the changefeed
// status key has been deleted from etcd. This is a non-recoverable error
// that should not be retried.
var errChangefeedKeyDeleted = errors.New("changefeed status key is deleted")

const (
	// retryBackoffBase is the initial backoff duration for retries
	retryBackoffBase = 500 * time.Millisecond
	// retryBackoffMax is the maximum backoff duration for retries
	retryBackoffMax = 30 * time.Second
	// retryBackoffMultiplier is the multiplier for exponential backoff
	retryBackoffMultiplier = 2.0
)

type Watcher interface {
	AdvanceCheckpointTs(ctx context.Context, minCheckpointTs uint64) (uint64, error)
	Close()
}

type waitCheckpointTask struct {
	respCh          chan uint64
	minCheckpointTs uint64
}

type CheckpointWatcher struct {
	localClusterID      string
	replicatedClusterID string
	changefeedID        common.ChangeFeedID
	etcdClient          etcd.CDCEtcdClient

	ctx    context.Context
	cancel context.CancelFunc

	mu               sync.Mutex
	latestCheckpoint uint64
	pendingTasks     []*waitCheckpointTask
	watchErr         error
	closed           bool
}

func NewCheckpointWatcher(
	ctx context.Context,
	localClusterID, replicatedClusterID, changefeedID string,
	etcdClient etcd.CDCEtcdClient,
) *CheckpointWatcher {
	cctx, cancel := context.WithCancel(ctx)
	watcher := &CheckpointWatcher{
		localClusterID:      localClusterID,
		replicatedClusterID: replicatedClusterID,
		changefeedID:        common.NewChangeFeedIDWithName(changefeedID, "default"),
		etcdClient:          etcdClient,

		ctx:    cctx,
		cancel: cancel,
	}
	go watcher.run()
	return watcher
}

// AdvanceCheckpointTs waits for the checkpoint to exceed minCheckpointTs
func (cw *CheckpointWatcher) AdvanceCheckpointTs(ctx context.Context, minCheckpointTs uint64) (uint64, error) {
	cw.mu.Lock()

	// Check if watcher has encountered an error
	if cw.watchErr != nil {
		err := cw.watchErr
		cw.mu.Unlock()
		return 0, err
	}

	// Check if watcher is closed
	if cw.closed {
		cw.mu.Unlock()
		return 0, errors.Errorf("checkpoint watcher is closed")
	}

	// Check if current checkpoint already exceeds minCheckpointTs
	if cw.latestCheckpoint > minCheckpointTs {
		checkpoint := cw.latestCheckpoint
		cw.mu.Unlock()
		return checkpoint, nil
	}

	// Create a task and wait for the background goroutine to notify
	task := &waitCheckpointTask{
		respCh:          make(chan uint64, 1),
		minCheckpointTs: minCheckpointTs,
	}
	cw.pendingTasks = append(cw.pendingTasks, task)
	cw.mu.Unlock()

	// Wait for response or context cancellation
	select {
	case <-ctx.Done():
		// Remove the task from pending list
		cw.mu.Lock()
		for i, t := range cw.pendingTasks {
			if t == task {
				cw.pendingTasks = append(cw.pendingTasks[:i], cw.pendingTasks[i+1:]...)
				break
			}
		}
		cw.mu.Unlock()
		return 0, errors.Annotate(ctx.Err(), "context canceled while waiting for checkpoint")
	case <-cw.ctx.Done():
		return 0, errors.Annotate(cw.ctx.Err(), "watcher context canceled")
	case checkpoint, ok := <-task.respCh:
		if !ok {
			return 0, errors.Errorf("checkpoint watcher is closed")
		}
		return checkpoint, nil
	}
}

// Close stops the watcher
func (cw *CheckpointWatcher) Close() {
	cw.cancel()
	cw.mu.Lock()
	cw.closed = true
	// Notify all pending tasks that watcher is closing
	for _, task := range cw.pendingTasks {
		close(task.respCh)
	}
	cw.pendingTasks = nil
	cw.mu.Unlock()
}

func (cw *CheckpointWatcher) run() {
	backoff := retryBackoffBase
	for {
		select {
		case <-cw.ctx.Done():
			cw.mu.Lock()
			cw.watchErr = errors.Annotate(cw.ctx.Err(), "context canceled")
			cw.mu.Unlock()
			return
		default:
		}

		err := cw.watchOnce()
		if err == nil {
			// Normal exit (context canceled)
			return
		}

		// Check if this is a non-recoverable error
		if errors.Is(err, errChangefeedKeyDeleted) {
			cw.mu.Lock()
			cw.watchErr = err
			cw.mu.Unlock()
			return
		}

		// Log and retry with backoff
		log.Warn("checkpoint watcher encountered error, will retry",
			zap.String("changefeedID", cw.changefeedID.String()),
			zap.Duration("backoff", backoff),
			zap.Error(err))

		select {
		case <-cw.ctx.Done():
			cw.mu.Lock()
			cw.watchErr = errors.Annotate(cw.ctx.Err(), "context canceled")
			cw.mu.Unlock()
			return
		case <-time.After(backoff):
		}

		// Increase backoff for next retry (exponential backoff with cap)
		backoff = time.Duration(float64(backoff) * retryBackoffMultiplier)
		backoff = min(backoff, retryBackoffMax)
	}
}

// watchOnce performs one watch cycle. Returns nil if context is canceled,
// returns error if watch fails and should be retried.
func (cw *CheckpointWatcher) watchOnce() error {
	// First, get the current checkpoint status from etcd
	status, modRev, err := cw.etcdClient.GetChangeFeedStatus(cw.ctx, cw.changefeedID)
	if err != nil {
		// Check if context is canceled
		if cw.ctx.Err() != nil {
			return nil
		}
		return errors.Annotate(err, "failed to get changefeed status")
	}

	// Update latest checkpoint
	cw.mu.Lock()
	cw.latestCheckpoint = status.CheckpointTs
	cw.notifyPendingTasksLocked()
	cw.mu.Unlock()

	statusKey := etcd.GetEtcdKeyJob(cw.etcdClient.GetClusterID(), cw.changefeedID.DisplayName)

	log.Debug("Starting to watch checkpoint",
		zap.String("changefeed ID", cw.changefeedID.String()),
		zap.String("statusKey", statusKey),
		zap.String("local cluster ID", cw.localClusterID),
		zap.String("replicated cluster ID", cw.replicatedClusterID),
		zap.Uint64("checkpoint", status.CheckpointTs),
		zap.Int64("startRev", modRev+1))

	watchCh := cw.etcdClient.GetEtcdClient().Watch(
		cw.ctx,
		statusKey,
		"checkpoint-watcher",
		clientv3.WithRev(modRev+1),
	)

	for {
		select {
		case <-cw.ctx.Done():
			return nil
		case watchResp, ok := <-watchCh:
			if !ok {
				return errors.Errorf("[changefeedID: %s] watch channel closed", cw.changefeedID.String())
			}

			if err := watchResp.Err(); err != nil {
				return errors.Annotatef(err, "[changefeedID: %s] watch error", cw.changefeedID.String())
			}

			for _, event := range watchResp.Events {
				if event.Type == clientv3.EventTypeDelete {
					return errors.Annotatef(errChangefeedKeyDeleted, "[changefeedID: %s]", cw.changefeedID.String())
				}

				// Parse the updated status
				newStatus := &config.ChangeFeedStatus{}
				if err := newStatus.Unmarshal(event.Kv.Value); err != nil {
					log.Warn("failed to unmarshal changefeed status, skipping",
						zap.String("changefeedID", cw.changefeedID.String()),
						zap.Error(err))
					continue
				}

				checkpointTs := newStatus.CheckpointTs
				log.Debug("Checkpoint updated",
					zap.String("changefeedID", cw.changefeedID.String()),
					zap.Uint64("checkpoint", checkpointTs))

				// Update latest checkpoint and notify pending tasks
				cw.mu.Lock()
				if checkpointTs > cw.latestCheckpoint {
					cw.latestCheckpoint = checkpointTs
					cw.notifyPendingTasksLocked()
				}
				cw.mu.Unlock()
			}
		}
	}
}

// notifyPendingTasksLocked notifies pending tasks whose minCheckpointTs has been exceeded
// Must be called with mu locked
func (cw *CheckpointWatcher) notifyPendingTasksLocked() {
	remaining := cw.pendingTasks[:0]
	for _, task := range cw.pendingTasks {
		if cw.latestCheckpoint > task.minCheckpointTs {
			// Non-blocking send since channel has buffer of 1
			select {
			case task.respCh <- cw.latestCheckpoint:
			default:
			}
		} else {
			remaining = append(remaining, task)
		}
	}
	cw.pendingTasks = remaining
}
