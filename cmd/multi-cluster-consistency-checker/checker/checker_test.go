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

package checker

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/config"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/recorder"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/utils"
	"github.com/stretchr/testify/require"
)

func TestNewDataChecker(t *testing.T) {
	t.Parallel()

	t.Run("create data checker", func(t *testing.T) {
		t.Parallel()
		clusterConfig := map[string]config.ClusterConfig{
			"cluster1": {
				PDAddr:         "127.0.0.1:2379",
				S3SinkURI:      "s3://bucket/cluster1/",
				S3ChangefeedID: "s3-cf-1",
			},
			"cluster2": {
				PDAddr:         "127.0.0.1:2479",
				S3SinkURI:      "s3://bucket/cluster2/",
				S3ChangefeedID: "s3-cf-2",
			},
		}

		checker := NewDataChecker(context.Background(), clusterConfig, nil, nil)
		require.NotNil(t, checker)
		require.Equal(t, uint64(0), checker.round)
		require.Len(t, checker.clusterDataCheckers, 2)
		require.Contains(t, checker.clusterDataCheckers, "cluster1")
		require.Contains(t, checker.clusterDataCheckers, "cluster2")
	})
}

func TestNewClusterDataChecker(t *testing.T) {
	t.Parallel()

	t.Run("create cluster data checker", func(t *testing.T) {
		t.Parallel()
		checker := newClusterDataChecker("cluster1")
		require.NotNil(t, checker)
		require.Equal(t, "cluster1", checker.clusterID)
		require.Equal(t, uint64(0), checker.rightBoundary)
		require.NotNil(t, checker.timeWindowDataCaches)
		require.NotNil(t, checker.overDataCaches)
		require.NotNil(t, checker.clusterViolationChecker)
	})
}

func TestNewClusterViolationChecker(t *testing.T) {
	t.Parallel()

	t.Run("create cluster violation checker", func(t *testing.T) {
		t.Parallel()
		checker := newClusterViolationChecker("cluster1")
		require.NotNil(t, checker)
		require.Equal(t, "cluster1", checker.clusterID)
		require.NotNil(t, checker.twoPreviousTimeWindowKeyVersionCache)
	})
}

func TestClusterViolationChecker_Check(t *testing.T) {
	t.Parallel()

	t.Run("check new record", func(t *testing.T) {
		t.Parallel()
		checker := newClusterViolationChecker("cluster1")
		report := recorder.NewClusterReport("cluster1")

		record := &utils.Record{
			Pk: "pk1",
			CdcVersion: utils.CdcVersion{
				CommitTs: 100,
				OriginTs: 0,
			},
		}

		checker.Check(record, report)
		require.Len(t, report.LWWViolationItems, 0)
		require.Contains(t, checker.twoPreviousTimeWindowKeyVersionCache, record.Pk)
	})

	t.Run("check duplicate old version", func(t *testing.T) {
		t.Parallel()
		checker := newClusterViolationChecker("cluster1")
		report := recorder.NewClusterReport("cluster1")

		record1 := &utils.Record{
			Pk: "pk1",
			CdcVersion: utils.CdcVersion{
				CommitTs: 100,
				OriginTs: 0,
			},
		}
		record2 := &utils.Record{
			Pk: "pk1",
			CdcVersion: utils.CdcVersion{
				CommitTs: 50,
				OriginTs: 0,
			},
		}

		checker.Check(record1, report)
		checker.Check(record2, report)
		require.Len(t, report.LWWViolationItems, 0) // Should skip duplicate old version
	})

	t.Run("check lww violation", func(t *testing.T) {
		t.Parallel()
		checker := newClusterViolationChecker("cluster1")
		report := recorder.NewClusterReport("cluster1")

		record1 := &utils.Record{
			Pk: "pk1",
			CdcVersion: utils.CdcVersion{
				CommitTs: 100,
				OriginTs: 0,
			},
		}
		record2 := &utils.Record{
			Pk: "pk1",
			CdcVersion: utils.CdcVersion{
				CommitTs: 150,
				OriginTs: 50, // OriginTs is less than record1's CommitTs, causing violation
			},
		}

		checker.Check(record1, report)
		checker.Check(record2, report)
		require.Len(t, report.LWWViolationItems, 1)
		require.Equal(t, "pk1", report.LWWViolationItems[0].PK)
		require.Equal(t, uint64(0), report.LWWViolationItems[0].ExistingOriginTS)
		require.Equal(t, uint64(100), report.LWWViolationItems[0].ExistingCommitTS)
		require.Equal(t, uint64(50), report.LWWViolationItems[0].OriginTS)
		require.Equal(t, uint64(150), report.LWWViolationItems[0].CommitTS)
	})
}

func TestClusterViolationChecker_UpdateCache(t *testing.T) {
	t.Parallel()

	t.Run("update cache", func(t *testing.T) {
		t.Parallel()
		checker := newClusterViolationChecker("cluster1")
		report := recorder.NewClusterReport("cluster1")

		record := &utils.Record{
			Pk: "pk1",
			CdcVersion: utils.CdcVersion{
				CommitTs: 100,
				OriginTs: 0,
			},
		}

		checker.Check(record, report)
		require.Contains(t, checker.twoPreviousTimeWindowKeyVersionCache, record.Pk)
		entry := checker.twoPreviousTimeWindowKeyVersionCache[record.Pk]
		require.Equal(t, 0, entry.previous)

		checker.UpdateCache()
		entry = checker.twoPreviousTimeWindowKeyVersionCache[record.Pk]
		require.Equal(t, 1, entry.previous)

		checker.UpdateCache()
		entry = checker.twoPreviousTimeWindowKeyVersionCache[record.Pk]
		require.Equal(t, 2, entry.previous)

		checker.UpdateCache()
		// Entry should be removed after 2 updates
		_, exists := checker.twoPreviousTimeWindowKeyVersionCache[record.Pk]
		require.False(t, exists)
	})
}

func TestNewTimeWindowDataCache(t *testing.T) {
	t.Parallel()

	t.Run("create time window data cache", func(t *testing.T) {
		t.Parallel()
		leftBoundary := uint64(100)
		rightBoundary := uint64(200)
		checkpointTs := map[string]uint64{
			"cluster2": 150,
		}

		cache := newTimeWindowDataCache(leftBoundary, rightBoundary, checkpointTs)
		require.Equal(t, leftBoundary, cache.leftBoundary)
		require.Equal(t, rightBoundary, cache.rightBoundary)
		require.Equal(t, checkpointTs, cache.checkpointTs)
		require.NotNil(t, cache.upstreamDataCache)
		require.NotNil(t, cache.downstreamDataCache)
	})
}

func TestTimeWindowDataCache_NewRecord(t *testing.T) {
	t.Parallel()

	t.Run("add upstream record", func(t *testing.T) {
		t.Parallel()
		cache := newTimeWindowDataCache(100, 200, map[string]uint64{})
		record := &utils.Record{
			Pk: "pk1",
			CdcVersion: utils.CdcVersion{
				CommitTs: 150,
				OriginTs: 0,
			},
		}

		cache.NewRecord(record)
		require.Contains(t, cache.upstreamDataCache, record.Pk)
		require.Contains(t, cache.upstreamDataCache[record.Pk], record.CommitTs)
	})

	t.Run("add downstream record", func(t *testing.T) {
		t.Parallel()
		cache := newTimeWindowDataCache(100, 200, map[string]uint64{})
		record := &utils.Record{
			Pk: "pk1",
			CdcVersion: utils.CdcVersion{
				CommitTs: 150,
				OriginTs: 100,
			},
		}

		cache.NewRecord(record)
		require.Contains(t, cache.downstreamDataCache, record.Pk)
		require.Contains(t, cache.downstreamDataCache[record.Pk], record.OriginTs)
	})

	t.Run("skip record before left boundary", func(t *testing.T) {
		t.Parallel()
		cache := newTimeWindowDataCache(100, 200, map[string]uint64{})
		record := &utils.Record{
			Pk: "pk1",
			CdcVersion: utils.CdcVersion{
				CommitTs: 50,
				OriginTs: 0,
			},
		}

		cache.NewRecord(record)
		require.NotContains(t, cache.upstreamDataCache, record.Pk)
		require.NotContains(t, cache.downstreamDataCache, record.Pk)
	})
}

func TestClusterDataChecker_PrepareNextTimeWindowData(t *testing.T) {
	t.Parallel()

	t.Run("prepare next time window data", func(t *testing.T) {
		t.Parallel()
		checker := newClusterDataChecker("cluster1")
		checker.rightBoundary = 100

		timeWindow := utils.TimeWindow{
			LeftBoundary:  100,
			RightBoundary: 200,
			CheckpointTs:  map[string]uint64{"cluster2": 150},
		}

		err := checker.PrepareNextTimeWindowData(timeWindow)
		require.NoError(t, err)
		require.Equal(t, uint64(200), checker.rightBoundary)
	})

	t.Run("mismatch left boundary", func(t *testing.T) {
		t.Parallel()
		checker := newClusterDataChecker("cluster1")
		checker.rightBoundary = 100

		timeWindow := utils.TimeWindow{
			LeftBoundary:  150,
			RightBoundary: 200,
			CheckpointTs:  map[string]uint64{"cluster2": 150},
		}

		err := checker.PrepareNextTimeWindowData(timeWindow)
		require.Error(t, err)
		require.Contains(t, err.Error(), "mismatch")
	})
}

func TestDataChecker_FindClusterDownstreamData(t *testing.T) {
	t.Parallel()

	t.Run("find downstream data", func(t *testing.T) {
		t.Parallel()
		clusterConfig := map[string]config.ClusterConfig{
			"cluster1": {
				PDAddr:         "127.0.0.1:2379",
				S3SinkURI:      "s3://bucket/cluster1/",
				S3ChangefeedID: "s3-cf-1",
			},
			"cluster2": {
				PDAddr:         "127.0.0.1:2479",
				S3SinkURI:      "s3://bucket/cluster2/",
				S3ChangefeedID: "s3-cf-2",
			},
		}

		checker := NewDataChecker(context.Background(), clusterConfig, nil, nil)
		record, skipped := checker.FindClusterDownstreamData("cluster2", "pk1", 100)
		require.Nil(t, record)
		require.False(t, skipped)
	})
}

func TestDataChecker_FindClusterUpstreamData(t *testing.T) {
	t.Parallel()

	t.Run("find upstream data", func(t *testing.T) {
		t.Parallel()
		clusterConfig := map[string]config.ClusterConfig{
			"cluster1": {
				PDAddr:         "127.0.0.1:2379",
				S3SinkURI:      "s3://bucket/cluster1/",
				S3ChangefeedID: "s3-cf-1",
			},
			"cluster2": {
				PDAddr:         "127.0.0.1:2479",
				S3SinkURI:      "s3://bucket/cluster2/",
				S3ChangefeedID: "s3-cf-2",
			},
		}

		checker := NewDataChecker(context.Background(), clusterConfig, nil, nil)
		found := checker.FindClusterUpstreamData("cluster2", "pk1", 100)
		require.False(t, found)
	})
}
