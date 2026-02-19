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

package advancer

import (
	"context"
	"maps"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/watcher"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

// mockPDClient mocks pd.Client for testing.
// Each call to GetTS returns a monotonically increasing TSO (physical part increases by 1000ms per call).
type mockPDClient struct {
	pd.Client
	seq int64 // accessed atomically
}

func (m *mockPDClient) GetTS(ctx context.Context) (int64, int64, error) {
	n := atomic.AddInt64(&m.seq, 1)
	// Physical timestamp starts at 11000ms and increases by 1000ms per call.
	// oracle.ComposeTS(physical, 0) = physical << 18, so each step is ~262 million.
	return 10000 + n*1000, 0, nil
}

func (m *mockPDClient) Close() {}

// mockAdvancerWatcher mocks watcher.Watcher for testing.
// Returns minCheckpointTs + delta, ensuring the result is always > minCheckpointTs and monotonically increasing.
type mockAdvancerWatcher struct {
	mu      sync.Mutex
	delta   uint64
	history []uint64
}

func (m *mockAdvancerWatcher) AdvanceCheckpointTs(ctx context.Context, minCheckpointTs uint64) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := minCheckpointTs + m.delta
	m.history = append(m.history, result)
	return result, nil
}

func (m *mockAdvancerWatcher) Close() {}

func (m *mockAdvancerWatcher) getHistory() []uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]uint64, len(m.history))
	copy(out, m.history)
	return out
}

func TestNewTimeWindowAdvancer(t *testing.T) {
	checkpointWatchers := map[string]map[string]watcher.Watcher{
		"cluster1": {},
		"cluster2": {},
	}
	s3Watchers := map[string]*watcher.S3Watcher{
		"cluster1": nil,
		"cluster2": nil,
	}
	pdClients := map[string]pd.Client{
		"cluster1": nil,
		"cluster2": nil,
	}

	advancer, _, err := NewTimeWindowAdvancer(context.Background(), checkpointWatchers, s3Watchers, pdClients, nil)
	require.NoError(t, err)
	require.NotNil(t, advancer)
	require.Equal(t, uint64(0), advancer.round)
	require.Len(t, advancer.timeWindowTriplet, 2)
	require.Contains(t, advancer.timeWindowTriplet, "cluster1")
	require.Contains(t, advancer.timeWindowTriplet, "cluster2")
}

// TestTimeWindowAdvancer_AdvanceMultipleRounds simulates 4 rounds of AdvanceTimeWindow
// with 2 clusters (c1, c2) performing bidirectional replication.
//
// The test verifies:
// - Time windows advance correctly (LeftBoundary == previous RightBoundary)
// - RightBoundary > LeftBoundary for each time window
// - Checkpoint timestamps are monotonically increasing across rounds
// - PD TSOs are always greater than checkpoint timestamps
// - NextMinLeftBoundary (PD TSO) > RightBoundary (S3 checkpoint)
// - Mock watcher checkpoint histories are strictly increasing
func TestTimeWindowAdvancer_AdvanceMultipleRounds(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Create mock PD clients for each cluster (monotonically increasing TSO)
	pdC1 := &mockPDClient{}
	pdC2 := &mockPDClient{}
	pdClients := map[string]pd.Client{
		"c1": pdC1,
		"c2": pdC2,
	}

	// Create mock checkpoint watchers for bidirectional replication (c1->c2, c2->c1)
	// Each returns minCheckpointTs + 100
	cpWatcherC1C2 := &mockAdvancerWatcher{delta: 100}
	cpWatcherC2C1 := &mockAdvancerWatcher{delta: 100}
	checkpointWatchers := map[string]map[string]watcher.Watcher{
		"c1": {"c2": cpWatcherC1C2},
		"c2": {"c1": cpWatcherC2C1},
	}

	// Create S3 watchers with mock checkpoint watchers (returns minCheckpointTs + 50)
	// and empty in-memory storage (no actual S3 data)
	s3WatcherMockC1 := &mockAdvancerWatcher{delta: 50}
	s3WatcherMockC2 := &mockAdvancerWatcher{delta: 50}
	s3Watchers := map[string]*watcher.S3Watcher{
		"c1": watcher.NewS3Watcher(s3WatcherMockC1, storage.NewMemStorage(), nil),
		"c2": watcher.NewS3Watcher(s3WatcherMockC2, storage.NewMemStorage(), nil),
	}

	advancer, _, err := NewTimeWindowAdvancer(ctx, checkpointWatchers, s3Watchers, pdClients, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), advancer.round)

	// Track previous round values for cross-round assertions
	prevRightBoundaries := map[string]uint64{"c1": 0, "c2": 0}
	prevCheckpointTs := map[string]map[string]uint64{
		"c1": {"c2": 0},
		"c2": {"c1": 0},
	}
	prevRightBoundary := uint64(0) // max across all clusters

	for round := range 4 {
		result, err := advancer.AdvanceTimeWindow(ctx)
		require.NoError(t, err, "round %d", round)
		require.Len(t, result, 2, "round %d: should have data for both clusters", round)

		for clusterID, twData := range result {
			tw := twData.TimeWindow

			// 1. LeftBoundary == previous RightBoundary
			require.Equal(t, prevRightBoundaries[clusterID], tw.LeftBoundary,
				"round %d, cluster %s: LeftBoundary should equal previous RightBoundary", round, clusterID)

			// 2. RightBoundary > LeftBoundary (time window is non-empty)
			require.Greater(t, tw.RightBoundary, tw.LeftBoundary,
				"round %d, cluster %s: RightBoundary should be > LeftBoundary", round, clusterID)

			// 3. CheckpointTs should be populated and strictly increasing across rounds
			require.NotEmpty(t, tw.CheckpointTs,
				"round %d, cluster %s: CheckpointTs should be populated", round, clusterID)
			for replicatedCluster, cpTs := range tw.CheckpointTs {
				require.Greater(t, cpTs, prevCheckpointTs[clusterID][replicatedCluster],
					"round %d, %s->%s: checkpoint should be strictly increasing", round, clusterID, replicatedCluster)
			}

			// 4. PDTimestampAfterTimeWindow should be populated
			require.NotEmpty(t, tw.PDTimestampAfterTimeWindow,
				"round %d, cluster %s: PDTimestampAfterTimeWindow should be populated", round, clusterID)

			// 5. NextMinLeftBoundary > RightBoundary
			//    (PD TSO is obtained after S3 checkpoint, and PD TSO >> S3 checkpoint)
			require.Greater(t, tw.NextMinLeftBoundary, tw.RightBoundary,
				"round %d, cluster %s: NextMinLeftBoundary (PD TSO) should be > RightBoundary (S3 checkpoint)", round, clusterID)

			// 6. PD TSO values in PDTimestampAfterTimeWindow > all CheckpointTs values
			//    (PD TSOs are obtained after checkpoint advance)
			for otherCluster, pdTs := range tw.PDTimestampAfterTimeWindow {
				for replicatedCluster, cpTs := range tw.CheckpointTs {
					require.Greater(t, pdTs, cpTs,
						"round %d, cluster %s: PD TSO (from %s) should be > checkpoint (%s->%s)",
						round, clusterID, otherCluster, clusterID, replicatedCluster)
				}
			}

			// 7. RightBoundary > previous round's max RightBoundary (time window advances)
			require.Greater(t, tw.RightBoundary, prevRightBoundary,
				"round %d, cluster %s: RightBoundary should be > previous max RightBoundary", round, clusterID)
		}

		// Save current values for next round
		maxRB := uint64(0)
		for clusterID, twData := range result {
			prevRightBoundaries[clusterID] = twData.TimeWindow.RightBoundary
			if twData.TimeWindow.RightBoundary > maxRB {
				maxRB = twData.TimeWindow.RightBoundary
			}
			maps.Copy(prevCheckpointTs[clusterID], twData.TimeWindow.CheckpointTs)
		}
		prevRightBoundary = maxRB
	}

	// After 4 rounds, round counter should be 4
	require.Equal(t, uint64(4), advancer.round)

	// Verify all mock watcher checkpoint histories are strictly monotonically increasing
	allWatchers := []*mockAdvancerWatcher{cpWatcherC1C2, cpWatcherC2C1, s3WatcherMockC1, s3WatcherMockC2}
	watcherNames := []string{"cp c1->c2", "cp c2->c1", "s3 c1", "s3 c2"}
	for idx, w := range allWatchers {
		history := w.getHistory()
		require.GreaterOrEqual(t, len(history), 4,
			"%s: should have at least 4 checkpoint values (one per round)", watcherNames[idx])
		for i := 1; i < len(history); i++ {
			require.Greater(t, history[i], history[i-1],
				"%s: checkpoint values should be strictly increasing (index %d: %d -> %d)",
				watcherNames[idx], i, history[i-1], history[i])
		}
	}

	// Verify PD clients were called (monotonically increasing due to atomic counter)
	require.Greater(t, atomic.LoadInt64(&pdC1.seq), int64(0), "pd-c1 should have been called")
	require.Greater(t, atomic.LoadInt64(&pdC2.seq), int64(0), "pd-c2 should have been called")
}
