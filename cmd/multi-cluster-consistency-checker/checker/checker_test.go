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
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/config"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/decoder"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/recorder"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/types"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
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

		record := &decoder.Record{
			Pk: "pk1",
			CdcVersion: types.CdcVersion{
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

		record1 := &decoder.Record{
			Pk: "pk1",
			CdcVersion: types.CdcVersion{
				CommitTs: 100,
				OriginTs: 0,
			},
		}
		record2 := &decoder.Record{
			Pk: "pk1",
			CdcVersion: types.CdcVersion{
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

		record1 := &decoder.Record{
			Pk: "pk1",
			CdcVersion: types.CdcVersion{
				CommitTs: 100,
				OriginTs: 0,
			},
		}
		record2 := &decoder.Record{
			Pk: "pk1",
			CdcVersion: types.CdcVersion{
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

		record := &decoder.Record{
			Pk: "pk1",
			CdcVersion: types.CdcVersion{
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
		record := &decoder.Record{
			Pk: "pk1",
			CdcVersion: types.CdcVersion{
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
		record := &decoder.Record{
			Pk: "pk1",
			CdcVersion: types.CdcVersion{
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
		record := &decoder.Record{
			Pk: "pk1",
			CdcVersion: types.CdcVersion{
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

		timeWindow := types.TimeWindow{
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

		timeWindow := types.TimeWindow{
			LeftBoundary:  150,
			RightBoundary: 200,
			CheckpointTs:  map[string]uint64{"cluster2": 150},
		}

		err := checker.PrepareNextTimeWindowData(timeWindow)
		require.Error(t, err)
		require.Contains(t, err.Error(), "mismatch")
	})
}

// makeCanalJSON builds a canal-JSON formatted record for testing.
// pkID is the primary key value, commitTs is the TiDB commit timestamp,
// originTs is the origin timestamp (0 for upstream records, non-zero for downstream),
// val is a varchar column value.
func makeCanalJSON(pkID int, commitTs uint64, originTs uint64, val string) string {
	originTsVal := "null"
	if originTs > 0 {
		originTsVal = fmt.Sprintf(`"%d"`, originTs)
	}
	return fmt.Sprintf(
		`{"id":0,"database":"test","table":"t1","pkNames":["id"],"isDdl":false,"type":"INSERT",`+
			`"es":0,"ts":0,"sql":"","sqlType":{"id":4,"val":12,"_tidb_origin_ts":-5},`+
			`"mysqlType":{"id":"int","val":"varchar","_tidb_origin_ts":"bigint"},`+
			`"old":null,"data":[{"id":"%d","val":"%s","_tidb_origin_ts":%s}],`+
			`"_tidb":{"commitTs":%d}}`,
		pkID, val, originTsVal, commitTs)
}

// makeContent combines canal-JSON records with CRLF terminator.
func makeContent(records ...string) []byte {
	return []byte(strings.Join(records, "\r\n"))
}

// makeTWData builds a TimeWindowData for testing.
func makeTWData(left, right uint64, checkpointTs map[string]uint64, content []byte) types.TimeWindowData {
	data := map[cloudstorage.DmlPathKey]types.IncrementalData{}
	if content != nil {
		data[cloudstorage.DmlPathKey{}] = types.IncrementalData{
			DataContentSlices: map[cloudstorage.FileIndexKey][][]byte{
				{}: {content},
			},
		}
	}
	return types.TimeWindowData{
		TimeWindow: types.TimeWindow{
			LeftBoundary:  left,
			RightBoundary: right,
			CheckpointTs:  checkpointTs,
		},
		Data: data,
	}
}

// TestDataChecker_FourRoundsCheck simulates 4 rounds with increasing data and verifies check results.
// Setup: 2 clusters (c1 upstream, c2 downstream from c1).
// Rounds 0-2: accumulate data, check not yet active (checkableRound < 3).
// Round 3: first real check runs, detecting violations.
func TestDataChecker_FourRoundsCheck(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	clusterCfg := map[string]config.ClusterConfig{"c1": {}, "c2": {}}

	// makeBaseRounds creates shared rounds 0 and 1 data for all subtests.
	// c1 produces upstream data, c2 receives matching downstream from c1.
	makeBaseRounds := func() [2]map[string]types.TimeWindowData {
		return [2]map[string]types.TimeWindowData{
			// Round 0: [0, 100]
			{
				"c1": makeTWData(0, 100, map[string]uint64{"c2": 80},
					makeContent(makeCanalJSON(1, 50, 0, "a"))),
				"c2": makeTWData(0, 100, nil,
					makeContent(makeCanalJSON(1, 60, 50, "a"))),
			},
			// Round 1: [100, 200]
			{
				"c1": makeTWData(100, 200, map[string]uint64{"c2": 180},
					makeContent(makeCanalJSON(2, 150, 0, "b"))),
				"c2": makeTWData(100, 200, nil,
					makeContent(makeCanalJSON(2, 160, 150, "b"))),
			},
		}
	}

	t.Run("all consistent", func(t *testing.T) {
		t.Parallel()
		checker := NewDataChecker(ctx, clusterCfg, nil, nil)
		base := makeBaseRounds()

		round2 := map[string]types.TimeWindowData{
			"c1": makeTWData(200, 300, map[string]uint64{"c2": 240},
				makeContent(makeCanalJSON(3, 250, 0, "c"))),
			"c2": makeTWData(200, 300, nil,
				makeContent(makeCanalJSON(3, 260, 250, "c"))),
		}
		round3 := map[string]types.TimeWindowData{
			"c1": makeTWData(300, 400, map[string]uint64{"c2": 380},
				makeContent(makeCanalJSON(4, 350, 0, "d"))),
			"c2": makeTWData(300, 400, nil,
				makeContent(makeCanalJSON(4, 360, 350, "d"))),
		}

		rounds := [4]map[string]types.TimeWindowData{base[0], base[1], round2, round3}
		for i, roundData := range rounds {
			report, err := checker.CheckInNextTimeWindow(ctx, roundData)
			require.NoError(t, err, "round %d", i)
			require.Equal(t, uint64(i), report.Round)
			if i < 3 {
				require.Empty(t, report.ClusterReports, "round %d should have no cluster reports", i)
				require.False(t, report.NeedFlush(), "round %d should not need flush", i)
			} else {
				require.Len(t, report.ClusterReports, 2)
				require.False(t, report.NeedFlush(), "round 3 should not need flush (all consistent)")
				for clusterID, cr := range report.ClusterReports {
					require.Empty(t, cr.DataLossItems, "cluster %s should have no data loss", clusterID)
					require.Empty(t, cr.DataRedundantItems, "cluster %s should have no data redundant", clusterID)
					require.Empty(t, cr.LWWViolationItems, "cluster %s should have no LWW violation", clusterID)
				}
			}
		}
	})

	t.Run("data loss detected", func(t *testing.T) {
		t.Parallel()
		checker := NewDataChecker(ctx, clusterCfg, nil, nil)
		base := makeBaseRounds()

		// Round 2: c1 has upstream pk=3 but c2 has NO matching downstream
		round2 := map[string]types.TimeWindowData{
			"c1": makeTWData(200, 300, map[string]uint64{"c2": 240},
				makeContent(makeCanalJSON(3, 250, 0, "c"))),
			"c2": makeTWData(200, 300, nil, nil),
		}
		round3 := map[string]types.TimeWindowData{
			"c1": makeTWData(300, 400, map[string]uint64{"c2": 380},
				makeContent(makeCanalJSON(4, 350, 0, "d"))),
			"c2": makeTWData(300, 400, nil,
				makeContent(makeCanalJSON(4, 360, 350, "d"))),
		}

		rounds := [4]map[string]types.TimeWindowData{base[0], base[1], round2, round3}
		var lastReport *recorder.Report
		for i, roundData := range rounds {
			report, err := checker.CheckInNextTimeWindow(ctx, roundData)
			require.NoError(t, err, "round %d", i)
			lastReport = report
		}

		require.True(t, lastReport.NeedFlush())
		// c1 should detect data loss: pk=3 (commitTs=250) missing in c2's downstream
		c1Report := lastReport.ClusterReports["c1"]
		require.NotNil(t, c1Report)
		require.Len(t, c1Report.DataLossItems, 1)
		require.Equal(t, "c2", c1Report.DataLossItems[0].DownstreamClusterID)
		require.Equal(t, uint64(0), c1Report.DataLossItems[0].OriginTS)
		require.Equal(t, uint64(250), c1Report.DataLossItems[0].CommitTS)
		require.False(t, c1Report.DataLossItems[0].Inconsistent)
		// c2 should have no issues
		c2Report := lastReport.ClusterReports["c2"]
		require.Empty(t, c2Report.DataLossItems)
		require.Empty(t, c2Report.DataRedundantItems)
	})

	t.Run("data inconsistent detected", func(t *testing.T) {
		t.Parallel()
		checker := NewDataChecker(ctx, clusterCfg, nil, nil)
		base := makeBaseRounds()

		// Round 2: c2 has downstream for pk=3 but with wrong column value
		round2 := map[string]types.TimeWindowData{
			"c1": makeTWData(200, 300, map[string]uint64{"c2": 240},
				makeContent(makeCanalJSON(3, 250, 0, "c"))),
			"c2": makeTWData(200, 300, nil,
				makeContent(makeCanalJSON(3, 260, 250, "WRONG"))),
		}
		round3 := map[string]types.TimeWindowData{
			"c1": makeTWData(300, 400, map[string]uint64{"c2": 380},
				makeContent(makeCanalJSON(4, 350, 0, "d"))),
			"c2": makeTWData(300, 400, nil,
				makeContent(makeCanalJSON(4, 360, 350, "d"))),
		}

		rounds := [4]map[string]types.TimeWindowData{base[0], base[1], round2, round3}
		var lastReport *recorder.Report
		for i, roundData := range rounds {
			report, err := checker.CheckInNextTimeWindow(ctx, roundData)
			require.NoError(t, err, "round %d", i)
			lastReport = report
		}

		require.True(t, lastReport.NeedFlush())
		c1Report := lastReport.ClusterReports["c1"]
		require.Len(t, c1Report.DataLossItems, 1)
		require.Equal(t, "c2", c1Report.DataLossItems[0].DownstreamClusterID)
		require.Equal(t, uint64(250), c1Report.DataLossItems[0].CommitTS)
		require.True(t, c1Report.DataLossItems[0].Inconsistent) // data inconsistent, not pure data loss
	})

	t.Run("data redundant detected", func(t *testing.T) {
		t.Parallel()
		checker := NewDataChecker(ctx, clusterCfg, nil, nil)
		base := makeBaseRounds()

		round2 := map[string]types.TimeWindowData{
			"c1": makeTWData(200, 300, map[string]uint64{"c2": 240},
				makeContent(makeCanalJSON(3, 250, 0, "c"))),
			"c2": makeTWData(200, 300, nil,
				makeContent(makeCanalJSON(3, 260, 250, "c"))),
		}
		// Round 3: c2 has an extra downstream pk=99 (originTs=330) that doesn't match
		// any upstream record in c1
		round3 := map[string]types.TimeWindowData{
			"c1": makeTWData(300, 400, map[string]uint64{"c2": 380},
				makeContent(makeCanalJSON(4, 350, 0, "d"))),
			"c2": makeTWData(300, 400, nil,
				makeContent(
					makeCanalJSON(4, 360, 350, "d"),
					makeCanalJSON(99, 340, 330, "x"),
				)),
		}

		rounds := [4]map[string]types.TimeWindowData{base[0], base[1], round2, round3}
		var lastReport *recorder.Report
		for i, roundData := range rounds {
			report, err := checker.CheckInNextTimeWindow(ctx, roundData)
			require.NoError(t, err, "round %d", i)
			lastReport = report
		}

		require.True(t, lastReport.NeedFlush())
		// c1 should have no data loss
		c1Report := lastReport.ClusterReports["c1"]
		require.Empty(t, c1Report.DataLossItems)
		// c2 should detect data redundant: pk=99 has no matching upstream in c1
		c2Report := lastReport.ClusterReports["c2"]
		require.Len(t, c2Report.DataRedundantItems, 1)
		require.Equal(t, uint64(330), c2Report.DataRedundantItems[0].OriginTS)
		require.Equal(t, uint64(340), c2Report.DataRedundantItems[0].CommitTS)
	})

	t.Run("lww violation detected", func(t *testing.T) {
		t.Parallel()
		checker := NewDataChecker(ctx, clusterCfg, nil, nil)
		base := makeBaseRounds()

		round2 := map[string]types.TimeWindowData{
			"c1": makeTWData(200, 300, map[string]uint64{"c2": 240},
				makeContent(makeCanalJSON(3, 250, 0, "c"))),
			"c2": makeTWData(200, 300, nil,
				makeContent(makeCanalJSON(3, 260, 250, "c"))),
		}
		// Round 3: c1 has upstream pk=5 (commitTs=350, compareTs=350) and
		// downstream pk=5 from c2 (commitTs=370, originTs=310, compareTs=310).
		// Since 350 >= 310 with commitTs 350 < 370, this is an LWW violation.
		// c2 also has matching records to avoid data loss/redundant noise.
		round3 := map[string]types.TimeWindowData{
			"c1": makeTWData(300, 400, map[string]uint64{"c2": 380},
				makeContent(
					makeCanalJSON(5, 350, 0, "e"),
					makeCanalJSON(5, 370, 310, "e"),
				)),
			"c2": makeTWData(300, 400, nil,
				makeContent(
					makeCanalJSON(5, 310, 0, "e"),
					makeCanalJSON(5, 360, 350, "e"),
				)),
		}

		rounds := [4]map[string]types.TimeWindowData{base[0], base[1], round2, round3}
		var lastReport *recorder.Report
		for i, roundData := range rounds {
			report, err := checker.CheckInNextTimeWindow(ctx, roundData)
			require.NoError(t, err, "round %d", i)
			lastReport = report
		}

		require.True(t, lastReport.NeedFlush())
		c1Report := lastReport.ClusterReports["c1"]
		require.Len(t, c1Report.LWWViolationItems, 1)
		require.Equal(t, uint64(0), c1Report.LWWViolationItems[0].ExistingOriginTS)
		require.Equal(t, uint64(350), c1Report.LWWViolationItems[0].ExistingCommitTS)
		require.Equal(t, uint64(310), c1Report.LWWViolationItems[0].OriginTS)
		require.Equal(t, uint64(370), c1Report.LWWViolationItems[0].CommitTS)
		// c2 should have no LWW violation (its records are ordered correctly:
		// upstream commitTs=310 compareTs=310, downstream commitTs=360 compareTs=350, 310 < 350)
		c2Report := lastReport.ClusterReports["c2"]
		require.Empty(t, c2Report.LWWViolationItems)
	})
}
