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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestCheckpointWatcher_AdvanceCheckpointTs_AlreadyExceeds(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEtcdClient := etcd.NewMockCDCEtcdClient(ctrl)
	mockClient := etcd.NewMockClient(ctrl)

	initialCheckpoint := uint64(1000)

	// Setup mock expectations
	mockEtcdClient.EXPECT().GetChangeFeedStatus(gomock.Any(), gomock.Any()).Return(
		&config.ChangeFeedStatus{CheckpointTs: initialCheckpoint},
		int64(100),
		nil,
	)
	mockEtcdClient.EXPECT().GetClusterID().Return("test-cluster").AnyTimes()
	mockEtcdClient.EXPECT().GetEtcdClient().Return(mockClient).AnyTimes()

	// Create a watch channel that won't send anything during this test
	watchCh := make(chan clientv3.WatchResponse)
	mockClient.EXPECT().Watch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(watchCh)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	watcher := NewCheckpointWatcher(ctx, "local-1", "replicated-1", "test-cf", mockEtcdClient)
	defer watcher.Close()

	// Wait for watcher to initialize
	time.Sleep(50 * time.Millisecond)

	// Request checkpoint that's already exceeded
	minCheckpointTs := uint64(500)
	checkpoint, err := watcher.AdvanceCheckpointTs(t.Context(), minCheckpointTs)
	require.NoError(t, err)
	require.Equal(t, initialCheckpoint, checkpoint)
}

func TestCheckpointWatcher_AdvanceCheckpointTs_WaitForUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEtcdClient := etcd.NewMockCDCEtcdClient(ctrl)
	mockClient := etcd.NewMockClient(ctrl)

	initialCheckpoint := uint64(1000)
	updatedCheckpoint := uint64(2000)

	// Setup mock expectations
	mockEtcdClient.EXPECT().GetChangeFeedStatus(gomock.Any(), gomock.Any()).Return(
		&config.ChangeFeedStatus{CheckpointTs: initialCheckpoint},
		int64(100),
		nil,
	)
	mockEtcdClient.EXPECT().GetClusterID().Return("test-cluster").AnyTimes()
	mockEtcdClient.EXPECT().GetEtcdClient().Return(mockClient).AnyTimes()

	watchCh := make(chan clientv3.WatchResponse, 1)
	mockClient.EXPECT().Watch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(watchCh)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	watcher := NewCheckpointWatcher(ctx, "local-1", "replicated-1", "test-cf", mockEtcdClient)
	defer watcher.Close()

	// Wait for watcher to initialize
	time.Sleep(50 * time.Millisecond)

	// Start waiting for checkpoint in a goroutine
	var checkpoint uint64
	var advanceErr error
	done := make(chan struct{})
	go func() {
		checkpoint, advanceErr = watcher.AdvanceCheckpointTs(context.Background(), uint64(1500))
		close(done)
	}()

	// Give some time for the task to be registered
	time.Sleep(50 * time.Millisecond)

	// Simulate checkpoint update via watch channel
	newStatus := &config.ChangeFeedStatus{CheckpointTs: updatedCheckpoint}
	statusStr, err := newStatus.Marshal()
	require.NoError(t, err)

	watchCh <- clientv3.WatchResponse{
		Events: []*clientv3.Event{
			{
				Type: clientv3.EventTypePut,
				Kv: &mvccpb.KeyValue{
					Value: []byte(statusStr),
				},
			},
		},
	}

	select {
	case <-done:
		require.NoError(t, advanceErr)
		require.Equal(t, updatedCheckpoint, checkpoint)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for checkpoint advance")
	}
}

func TestCheckpointWatcher_AdvanceCheckpointTs_ContextCanceled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEtcdClient := etcd.NewMockCDCEtcdClient(ctrl)
	mockClient := etcd.NewMockClient(ctrl)

	initialCheckpoint := uint64(1000)

	// Setup mock expectations
	mockEtcdClient.EXPECT().GetChangeFeedStatus(gomock.Any(), gomock.Any()).Return(
		&config.ChangeFeedStatus{CheckpointTs: initialCheckpoint},
		int64(100),
		nil,
	)
	mockEtcdClient.EXPECT().GetClusterID().Return("test-cluster").AnyTimes()
	mockEtcdClient.EXPECT().GetEtcdClient().Return(mockClient).AnyTimes()

	watchCh := make(chan clientv3.WatchResponse)
	mockClient.EXPECT().Watch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(watchCh)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	watcher := NewCheckpointWatcher(ctx, "local-1", "replicated-1", "test-cf", mockEtcdClient)
	defer watcher.Close()

	// Wait for watcher to initialize
	time.Sleep(50 * time.Millisecond)

	// Create a context that will be canceled
	advanceCtx, advanceCancel := context.WithCancel(t.Context())

	var advanceErr error
	done := make(chan struct{})
	go func() {
		_, advanceErr = watcher.AdvanceCheckpointTs(advanceCtx, uint64(2000))
		close(done)
	}()

	// Give some time for the task to be registered
	time.Sleep(50 * time.Millisecond)

	// Cancel the context
	advanceCancel()

	select {
	case <-done:
		require.Error(t, advanceErr)
		require.Contains(t, advanceErr.Error(), "context canceled")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for context cancellation")
	}
}

func TestCheckpointWatcher_Close(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEtcdClient := etcd.NewMockCDCEtcdClient(ctrl)
	mockClient := etcd.NewMockClient(ctrl)

	initialCheckpoint := uint64(1000)

	// Setup mock expectations
	mockEtcdClient.EXPECT().GetChangeFeedStatus(gomock.Any(), gomock.Any()).Return(
		&config.ChangeFeedStatus{CheckpointTs: initialCheckpoint},
		int64(100),
		nil,
	)
	mockEtcdClient.EXPECT().GetClusterID().Return("test-cluster").AnyTimes()
	mockEtcdClient.EXPECT().GetEtcdClient().Return(mockClient).AnyTimes()

	watchCh := make(chan clientv3.WatchResponse)
	mockClient.EXPECT().Watch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(watchCh)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	watcher := NewCheckpointWatcher(ctx, "local-1", "replicated-1", "test-cf", mockEtcdClient)

	// Wait for watcher to initialize
	time.Sleep(50 * time.Millisecond)

	// Start waiting for checkpoint in a goroutine
	var advanceErr error
	done := make(chan struct{})
	go func() {
		_, advanceErr = watcher.AdvanceCheckpointTs(context.Background(), uint64(2000))
		close(done)
	}()

	// Give some time for the task to be registered
	time.Sleep(50 * time.Millisecond)

	// Close the watcher
	watcher.Close()

	select {
	case <-done:
		require.Error(t, advanceErr)
		// Error can be "closed" or "canceled" depending on timing
		errMsg := advanceErr.Error()
		require.True(t,
			strings.Contains(errMsg, "closed") || strings.Contains(errMsg, "canceled"),
			"expected error to contain 'closed' or 'canceled', got: %s", errMsg)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for watcher close")
	}

	// Verify that subsequent calls return error
	_, err := watcher.AdvanceCheckpointTs(context.Background(), uint64(100))
	require.Error(t, err)
	// Error can be "closed" or "canceled" depending on timing
	errMsg := err.Error()
	require.True(t,
		strings.Contains(errMsg, "closed") || strings.Contains(errMsg, "canceled"),
		"expected error to contain 'closed' or 'canceled', got: %s", errMsg)
}

func TestCheckpointWatcher_MultiplePendingTasks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEtcdClient := etcd.NewMockCDCEtcdClient(ctrl)
	mockClient := etcd.NewMockClient(ctrl)

	initialCheckpoint := uint64(1000)

	// Setup mock expectations
	mockEtcdClient.EXPECT().GetChangeFeedStatus(gomock.Any(), gomock.Any()).Return(
		&config.ChangeFeedStatus{CheckpointTs: initialCheckpoint},
		int64(100),
		nil,
	)
	mockEtcdClient.EXPECT().GetClusterID().Return("test-cluster").AnyTimes()
	mockEtcdClient.EXPECT().GetEtcdClient().Return(mockClient).AnyTimes()

	watchCh := make(chan clientv3.WatchResponse, 10)
	mockClient.EXPECT().Watch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(watchCh)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	watcher := NewCheckpointWatcher(ctx, "local-1", "replicated-1", "test-cf", mockEtcdClient)
	defer watcher.Close()

	// Wait for watcher to initialize
	time.Sleep(50 * time.Millisecond)

	// Start multiple goroutines waiting for different checkpoints
	var wg sync.WaitGroup
	results := make([]struct {
		checkpoint uint64
		err        error
	}, 3)

	for i := range 3 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			minTs := uint64(1100 + idx*500) // 1100, 1600, 2100
			results[idx].checkpoint, results[idx].err = watcher.AdvanceCheckpointTs(context.Background(), minTs)
		}(i)
	}

	// Give some time for tasks to be registered
	time.Sleep(50 * time.Millisecond)

	// Send checkpoint updates
	checkpoints := []uint64{1500, 2000, 2500}
	for _, cp := range checkpoints {
		newStatus := &config.ChangeFeedStatus{CheckpointTs: cp}
		statusStr, err := newStatus.Marshal()
		require.NoError(t, err)

		watchCh <- clientv3.WatchResponse{
			Events: []*clientv3.Event{
				{
					Type: clientv3.EventTypePut,
					Kv: &mvccpb.KeyValue{
						Value: []byte(statusStr),
					},
				},
			},
		}
		// Give some time between updates
		time.Sleep(20 * time.Millisecond)
	}

	// Wait for all goroutines to complete
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Verify results
		for i := range 3 {
			require.NoError(t, results[i].err, "task %d should not have error", i)
			minTs := uint64(1100 + i*500)
			require.Greater(t, results[i].checkpoint, minTs, "task %d checkpoint should exceed minTs", i)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for all tasks to complete")
	}
}

func TestCheckpointWatcher_NotifyPendingTasksLocked(t *testing.T) {
	cw := &CheckpointWatcher{
		latestCheckpoint: 2000,
		pendingTasks: []*waitCheckpointTask{
			{respCh: make(chan uint64, 1), minCheckpointTs: 1000},
			{respCh: make(chan uint64, 1), minCheckpointTs: 1500},
			{respCh: make(chan uint64, 1), minCheckpointTs: 2500},
			{respCh: make(chan uint64, 1), minCheckpointTs: 3000},
		},
	}

	cw.notifyPendingTasksLocked()

	// Tasks with minCheckpointTs < 2000 should be notified and removed
	require.Len(t, cw.pendingTasks, 2)
	require.Equal(t, uint64(2500), cw.pendingTasks[0].minCheckpointTs)
	require.Equal(t, uint64(3000), cw.pendingTasks[1].minCheckpointTs)
}

func TestCheckpointWatcher_InitialCheckpointNotifiesPendingTasks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEtcdClient := etcd.NewMockCDCEtcdClient(ctrl)
	mockClient := etcd.NewMockClient(ctrl)

	initialCheckpoint := uint64(5000)

	// Setup mock expectations - initial checkpoint is high enough
	mockEtcdClient.EXPECT().GetChangeFeedStatus(gomock.Any(), gomock.Any()).Return(
		&config.ChangeFeedStatus{CheckpointTs: initialCheckpoint},
		int64(100),
		nil,
	)
	mockEtcdClient.EXPECT().GetClusterID().Return("test-cluster").AnyTimes()
	mockEtcdClient.EXPECT().GetEtcdClient().Return(mockClient).AnyTimes()

	watchCh := make(chan clientv3.WatchResponse)
	mockClient.EXPECT().Watch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(watchCh)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	watcher := NewCheckpointWatcher(ctx, "local-1", "replicated-1", "test-cf", mockEtcdClient)
	defer watcher.Close()

	// Wait for watcher to initialize and get the initial checkpoint
	time.Sleep(100 * time.Millisecond)

	// Request checkpoint that's already exceeded by initial checkpoint
	checkpoint, err := watcher.AdvanceCheckpointTs(context.Background(), uint64(1000))
	require.NoError(t, err)
	require.Equal(t, initialCheckpoint, checkpoint)
}

func TestCheckpointWatcher_WatchErrorRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEtcdClient := etcd.NewMockCDCEtcdClient(ctrl)
	mockClient := etcd.NewMockClient(ctrl)

	initialCheckpoint := uint64(1000)
	retryCheckpoint := uint64(2000)

	// First call succeeds, second call (retry) also succeeds with updated checkpoint
	firstCall := mockEtcdClient.EXPECT().GetChangeFeedStatus(gomock.Any(), gomock.Any()).Return(
		&config.ChangeFeedStatus{CheckpointTs: initialCheckpoint},
		int64(100),
		nil,
	)
	mockEtcdClient.EXPECT().GetChangeFeedStatus(gomock.Any(), gomock.Any()).Return(
		&config.ChangeFeedStatus{CheckpointTs: retryCheckpoint},
		int64(101),
		nil,
	).After(firstCall)

	mockEtcdClient.EXPECT().GetClusterID().Return("test-cluster").AnyTimes()
	mockEtcdClient.EXPECT().GetEtcdClient().Return(mockClient).AnyTimes()

	// First watch channel will be closed (simulating error), second watch channel will work
	watchCh1 := make(chan clientv3.WatchResponse)
	watchCh2 := make(chan clientv3.WatchResponse, 1)
	firstWatch := mockClient.EXPECT().Watch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(watchCh1)
	mockClient.EXPECT().Watch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(watchCh2).After(firstWatch)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	watcher := NewCheckpointWatcher(ctx, "local-1", "replicated-1", "test-cf", mockEtcdClient)
	defer watcher.Close()

	// Wait for watcher to initialize
	time.Sleep(50 * time.Millisecond)

	// Close the first watch channel to trigger retry
	close(watchCh1)

	// Wait for retry to happen (backoff + processing time)
	time.Sleep(700 * time.Millisecond)

	// After retry, checkpoint should be updated to retryCheckpoint
	checkpoint, err := watcher.AdvanceCheckpointTs(t.Context(), uint64(1500))
	require.NoError(t, err)
	require.Equal(t, retryCheckpoint, checkpoint)
}

func TestCheckpointWatcher_GetStatusRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEtcdClient := etcd.NewMockCDCEtcdClient(ctrl)
	mockClient := etcd.NewMockClient(ctrl)

	successCheckpoint := uint64(2000)

	// First call fails, second call succeeds
	firstCall := mockEtcdClient.EXPECT().GetChangeFeedStatus(gomock.Any(), gomock.Any()).Return(
		nil,
		int64(0),
		errors.Errorf("connection refused"),
	)
	mockEtcdClient.EXPECT().GetChangeFeedStatus(gomock.Any(), gomock.Any()).Return(
		&config.ChangeFeedStatus{CheckpointTs: successCheckpoint},
		int64(100),
		nil,
	).After(firstCall)

	mockEtcdClient.EXPECT().GetClusterID().Return("test-cluster").AnyTimes()
	mockEtcdClient.EXPECT().GetEtcdClient().Return(mockClient).AnyTimes()

	watchCh := make(chan clientv3.WatchResponse)
	mockClient.EXPECT().Watch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(watchCh)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	watcher := NewCheckpointWatcher(ctx, "local-1", "replicated-1", "test-cf", mockEtcdClient)
	defer watcher.Close()

	// Wait for retry to happen (backoff + processing time)
	time.Sleep(700 * time.Millisecond)

	// After retry, checkpoint should be available
	checkpoint, err := watcher.AdvanceCheckpointTs(t.Context(), uint64(1000))
	require.NoError(t, err)
	require.Equal(t, successCheckpoint, checkpoint)
}

func TestCheckpointWatcher_KeyDeleted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEtcdClient := etcd.NewMockCDCEtcdClient(ctrl)
	mockClient := etcd.NewMockClient(ctrl)

	initialCheckpoint := uint64(1000)

	// Setup mock expectations
	mockEtcdClient.EXPECT().GetChangeFeedStatus(gomock.Any(), gomock.Any()).Return(
		&config.ChangeFeedStatus{CheckpointTs: initialCheckpoint},
		int64(100),
		nil,
	)
	mockEtcdClient.EXPECT().GetClusterID().Return("test-cluster").AnyTimes()
	mockEtcdClient.EXPECT().GetEtcdClient().Return(mockClient).AnyTimes()

	watchCh := make(chan clientv3.WatchResponse, 1)
	mockClient.EXPECT().Watch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(watchCh)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	watcher := NewCheckpointWatcher(ctx, "local-1", "replicated-1", "test-cf", mockEtcdClient)
	defer watcher.Close()

	// Wait for watcher to initialize
	time.Sleep(50 * time.Millisecond)

	// Send delete event
	watchCh <- clientv3.WatchResponse{
		Events: []*clientv3.Event{
			{
				Type: clientv3.EventTypeDelete,
			},
		},
	}

	// Give time for the error to be processed
	time.Sleep(50 * time.Millisecond)

	// Now trying to advance should return an error
	_, err := watcher.AdvanceCheckpointTs(context.Background(), uint64(2000))
	require.Error(t, err)
	require.Contains(t, err.Error(), "deleted")
}
