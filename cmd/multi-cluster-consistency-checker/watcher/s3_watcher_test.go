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
	"testing"

	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/consumer"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/recorder"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
)

// mockWatcher is a mock implementation of the Watcher interface for testing.
type mockWatcher struct {
	advanceCheckpointTsFn func(ctx context.Context, minCheckpointTs uint64) (uint64, error)
	closeFn               func()
	closed                bool
}

func (m *mockWatcher) AdvanceCheckpointTs(ctx context.Context, minCheckpointTs uint64) (uint64, error) {
	if m.advanceCheckpointTsFn != nil {
		return m.advanceCheckpointTsFn(ctx, minCheckpointTs)
	}
	return 0, nil
}

func (m *mockWatcher) Close() {
	m.closed = true
	if m.closeFn != nil {
		m.closeFn()
	}
}

func TestS3Watcher_Close(t *testing.T) {
	t.Parallel()

	t.Run("close delegates to checkpoint watcher", func(t *testing.T) {
		t.Parallel()
		mock := &mockWatcher{}
		sw := &S3Watcher{
			checkpointWatcher: mock,
			consumer:          consumer.NewS3Consumer(storage.NewMemStorage(), nil),
		}

		sw.Close()
		require.True(t, mock.closed)
	})

	t.Run("close calls custom close function", func(t *testing.T) {
		t.Parallel()
		closeCalled := false
		mock := &mockWatcher{
			closeFn: func() {
				closeCalled = true
			},
		}
		sw := &S3Watcher{
			checkpointWatcher: mock,
			consumer:          consumer.NewS3Consumer(storage.NewMemStorage(), nil),
		}

		sw.Close()
		require.True(t, closeCalled)
	})
}

func TestS3Watcher_AdvanceS3CheckpointTs(t *testing.T) {
	t.Parallel()

	t.Run("advance checkpoint ts success", func(t *testing.T) {
		t.Parallel()
		expectedCheckpoint := uint64(5000)
		mock := &mockWatcher{
			advanceCheckpointTsFn: func(ctx context.Context, minCheckpointTs uint64) (uint64, error) {
				require.Equal(t, uint64(3000), minCheckpointTs)
				return expectedCheckpoint, nil
			},
		}
		sw := &S3Watcher{
			checkpointWatcher: mock,
			consumer:          consumer.NewS3Consumer(storage.NewMemStorage(), nil),
		}

		checkpoint, err := sw.AdvanceS3CheckpointTs(context.Background(), uint64(3000))
		require.NoError(t, err)
		require.Equal(t, expectedCheckpoint, checkpoint)
	})

	t.Run("advance checkpoint ts error", func(t *testing.T) {
		t.Parallel()
		mock := &mockWatcher{
			advanceCheckpointTsFn: func(ctx context.Context, minCheckpointTs uint64) (uint64, error) {
				return 0, errors.Errorf("connection lost")
			},
		}
		sw := &S3Watcher{
			checkpointWatcher: mock,
			consumer:          consumer.NewS3Consumer(storage.NewMemStorage(), nil),
		}

		checkpoint, err := sw.AdvanceS3CheckpointTs(context.Background(), uint64(3000))
		require.Error(t, err)
		require.Equal(t, uint64(0), checkpoint)
		require.Contains(t, err.Error(), "advance s3 checkpoint timestamp failed")
		require.Contains(t, err.Error(), "connection lost")
	})

	t.Run("advance checkpoint ts with context canceled", func(t *testing.T) {
		t.Parallel()
		mock := &mockWatcher{
			advanceCheckpointTsFn: func(ctx context.Context, minCheckpointTs uint64) (uint64, error) {
				return 0, context.Canceled
			},
		}
		sw := &S3Watcher{
			checkpointWatcher: mock,
			consumer:          consumer.NewS3Consumer(storage.NewMemStorage(), nil),
		}

		checkpoint, err := sw.AdvanceS3CheckpointTs(context.Background(), uint64(3000))
		require.Error(t, err)
		require.Equal(t, uint64(0), checkpoint)
		require.Contains(t, err.Error(), "advance s3 checkpoint timestamp failed")
	})
}

func TestS3Watcher_InitializeFromCheckpoint(t *testing.T) {
	t.Parallel()

	t.Run("nil checkpoint returns nil", func(t *testing.T) {
		t.Parallel()
		mock := &mockWatcher{}
		sw := &S3Watcher{
			checkpointWatcher: mock,
			consumer:          consumer.NewS3Consumer(storage.NewMemStorage(), nil),
		}

		result, err := sw.InitializeFromCheckpoint(context.Background(), "cluster1", nil)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("empty checkpoint returns nil", func(t *testing.T) {
		t.Parallel()
		mock := &mockWatcher{}
		sw := &S3Watcher{
			checkpointWatcher: mock,
			consumer:          consumer.NewS3Consumer(storage.NewMemStorage(), nil),
		}

		checkpoint := recorder.NewCheckpoint()
		result, err := sw.InitializeFromCheckpoint(context.Background(), "cluster1", checkpoint)
		require.NoError(t, err)
		require.Nil(t, result)
	})
}

func TestS3Watcher_ConsumeNewFiles(t *testing.T) {
	t.Parallel()

	t.Run("consume new files with empty tables", func(t *testing.T) {
		t.Parallel()
		mock := &mockWatcher{}
		sw := &S3Watcher{
			checkpointWatcher: mock,
			consumer:          consumer.NewS3Consumer(storage.NewMemStorage(), map[string][]string{}),
		}

		newData, maxVersionMap, err := sw.ConsumeNewFiles(context.Background())
		require.NoError(t, err)
		require.Empty(t, newData)
		require.Empty(t, maxVersionMap)
	})

	t.Run("consume new files with nil tables", func(t *testing.T) {
		t.Parallel()
		mock := &mockWatcher{}
		sw := &S3Watcher{
			checkpointWatcher: mock,
			consumer:          consumer.NewS3Consumer(storage.NewMemStorage(), nil),
		}

		newData, maxVersionMap, err := sw.ConsumeNewFiles(context.Background())
		require.NoError(t, err)
		require.Empty(t, newData)
		require.Empty(t, maxVersionMap)
	})
}
