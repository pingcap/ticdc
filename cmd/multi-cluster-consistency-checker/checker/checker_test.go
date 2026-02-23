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
				PDAddrs:        []string{"127.0.0.1:2379"},
				S3SinkURI:      "s3://bucket/cluster1/",
				S3ChangefeedID: "s3-cf-1",
			},
			"cluster2": {
				PDAddrs:        []string{"127.0.0.1:2479"},
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

	const schemaKey = "test_schema"

	t.Run("check new record", func(t *testing.T) {
		t.Parallel()
		checker := newClusterViolationChecker("cluster1")
		report := recorder.NewClusterReport("cluster1", types.TimeWindow{})

		record := &decoder.Record{
			Pk:    "pk1",
			PkMap: map[string]any{"id": "1"},
			CdcVersion: types.CdcVersion{
				CommitTs: 100,
				OriginTs: 0,
			},
		}

		checker.Check(schemaKey, record, report)
		require.Empty(t, report.TableFailureItems)
		require.Contains(t, checker.twoPreviousTimeWindowKeyVersionCache, schemaKey)
		require.Contains(t, checker.twoPreviousTimeWindowKeyVersionCache[schemaKey], record.Pk)
	})

	t.Run("check duplicate old version", func(t *testing.T) {
		t.Parallel()
		checker := newClusterViolationChecker("cluster1")
		report := recorder.NewClusterReport("cluster1", types.TimeWindow{})

		record1 := &decoder.Record{
			Pk:    "pk1",
			PkMap: map[string]any{"id": "1"},
			CdcVersion: types.CdcVersion{
				CommitTs: 100,
				OriginTs: 0,
			},
		}
		record2 := &decoder.Record{
			Pk:    "pk1",
			PkMap: map[string]any{"id": "1"},
			CdcVersion: types.CdcVersion{
				CommitTs: 50,
				OriginTs: 0,
			},
		}

		checker.Check(schemaKey, record1, report)
		checker.Check(schemaKey, record2, report)
		require.Empty(t, report.TableFailureItems) // Should skip duplicate old version
	})

	t.Run("check lww violation", func(t *testing.T) {
		t.Parallel()
		checker := newClusterViolationChecker("cluster1")
		report := recorder.NewClusterReport("cluster1", types.TimeWindow{})

		record1 := &decoder.Record{
			Pk:    "pk1",
			PkMap: map[string]any{"id": "1"},
			CdcVersion: types.CdcVersion{
				CommitTs: 100,
				OriginTs: 0,
			},
		}
		record2 := &decoder.Record{
			Pk:    "pk1",
			PkMap: map[string]any{"id": "1"},
			CdcVersion: types.CdcVersion{
				CommitTs: 150,
				OriginTs: 50, // OriginTs is less than record1's CommitTs, causing violation
			},
		}

		checker.Check(schemaKey, record1, report)
		checker.Check(schemaKey, record2, report)
		require.Len(t, report.TableFailureItems, 1)
		require.Contains(t, report.TableFailureItems, schemaKey)
		tableItems := report.TableFailureItems[schemaKey]
		require.Len(t, tableItems.LWWViolationItems, 1)
		require.Equal(t, map[string]any{"id": "1"}, tableItems.LWWViolationItems[0].PK)
		require.Equal(t, uint64(0), tableItems.LWWViolationItems[0].ExistingOriginTS)
		require.Equal(t, uint64(100), tableItems.LWWViolationItems[0].ExistingCommitTS)
		require.Equal(t, uint64(50), tableItems.LWWViolationItems[0].OriginTS)
		require.Equal(t, uint64(150), tableItems.LWWViolationItems[0].CommitTS)
	})
}

func TestClusterViolationChecker_UpdateCache(t *testing.T) {
	t.Parallel()

	const schemaKey = "test_schema"

	t.Run("update cache", func(t *testing.T) {
		t.Parallel()
		checker := newClusterViolationChecker("cluster1")
		report := recorder.NewClusterReport("cluster1", types.TimeWindow{})

		record := &decoder.Record{
			Pk:    "pk1",
			PkMap: map[string]any{"id": "1"},
			CdcVersion: types.CdcVersion{
				CommitTs: 100,
				OriginTs: 0,
			},
		}

		checker.Check(schemaKey, record, report)
		require.Contains(t, checker.twoPreviousTimeWindowKeyVersionCache, schemaKey)
		entry := checker.twoPreviousTimeWindowKeyVersionCache[schemaKey][record.Pk]
		require.Equal(t, 0, entry.previous)

		checker.UpdateCache()
		entry = checker.twoPreviousTimeWindowKeyVersionCache[schemaKey][record.Pk]
		require.Equal(t, 1, entry.previous)

		checker.UpdateCache()
		entry = checker.twoPreviousTimeWindowKeyVersionCache[schemaKey][record.Pk]
		require.Equal(t, 2, entry.previous)

		checker.UpdateCache()
		// Entry should be removed after 2 updates
		_, exists := checker.twoPreviousTimeWindowKeyVersionCache[schemaKey]
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
		require.NotNil(t, cache.tableDataCaches)
	})
}

func TestTimeWindowDataCache_NewRecord(t *testing.T) {
	t.Parallel()

	const schemaKey = "test_schema"

	t.Run("add local record", func(t *testing.T) {
		t.Parallel()
		cache := newTimeWindowDataCache(100, 200, map[string]uint64{})
		record := &decoder.Record{
			Pk:    "pk1",
			PkMap: map[string]any{"id": "1"},
			CdcVersion: types.CdcVersion{
				CommitTs: 150,
				OriginTs: 0,
			},
		}

		cache.NewRecord(schemaKey, record)
		require.Contains(t, cache.tableDataCaches, schemaKey)
		require.Contains(t, cache.tableDataCaches[schemaKey].localDataCache, record.Pk)
		require.Contains(t, cache.tableDataCaches[schemaKey].localDataCache[record.Pk], record.CommitTs)
	})

	t.Run("add replicated record", func(t *testing.T) {
		t.Parallel()
		cache := newTimeWindowDataCache(100, 200, map[string]uint64{})
		record := &decoder.Record{
			Pk:    "pk1",
			PkMap: map[string]any{"id": "1"},
			CdcVersion: types.CdcVersion{
				CommitTs: 150,
				OriginTs: 100,
			},
		}

		cache.NewRecord(schemaKey, record)
		require.Contains(t, cache.tableDataCaches, schemaKey)
		require.Contains(t, cache.tableDataCaches[schemaKey].replicatedDataCache, record.Pk)
		require.Contains(t, cache.tableDataCaches[schemaKey].replicatedDataCache[record.Pk], record.OriginTs)
	})

	t.Run("skip record before left boundary", func(t *testing.T) {
		t.Parallel()
		cache := newTimeWindowDataCache(100, 200, map[string]uint64{})
		record := &decoder.Record{
			Pk:    "pk1",
			PkMap: map[string]any{"id": "1"},
			CdcVersion: types.CdcVersion{
				CommitTs: 50,
				OriginTs: 0,
			},
		}

		cache.NewRecord(schemaKey, record)
		require.NotContains(t, cache.tableDataCaches, schemaKey)
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
// originTs is the origin timestamp (0 for locally-written records, non-zero for replicated records),
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

// defaultSchemaKey is the schema key produced by DmlPathKey{}.GetKey()
// which is QuoteSchema("", "") = "“.“"
var defaultSchemaKey = (&cloudstorage.DmlPathKey{}).GetKey()

// TestDataChecker_FourRoundsCheck simulates 4 rounds with increasing data and verifies check results.
// Setup: 2 clusters (c1 locally-written, c2 replicated from c1).
// - Round 0: LWW cache is seeded (data is empty by convention).
// - Round 1+: LWW violation detection is active.
// - Round 2+: data loss / inconsistent detection is active.
// - Round 3+: data redundant detection is also active (needs [0],[1],[2] all populated).
func TestDataChecker_FourRoundsCheck(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	clusterCfg := map[string]config.ClusterConfig{"c1": {}, "c2": {}}

	// makeBaseRounds creates shared rounds 0 and 1 data for all subtests.
	// c1 produces locally-written data, c2 receives matching replicated data from c1.
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
			report, err := checker.CheckInNextTimeWindow(roundData)
			require.NoError(t, err, "round %d", i)
			require.Equal(t, uint64(i), report.Round)
			// Every round now produces cluster reports (LWW always runs).
			require.Len(t, report.ClusterReports, 2, "round %d should have 2 cluster reports", i)
			require.False(t, report.NeedFlush(), "round %d should not need flush (all consistent)", i)
			for clusterID, cr := range report.ClusterReports {
				require.Empty(t, cr.TableFailureItems, "round %d cluster %s should have no table failure items", i, clusterID)
			}
		}
	})

	t.Run("data loss detected", func(t *testing.T) {
		t.Parallel()
		checker := NewDataChecker(ctx, clusterCfg, nil, nil)
		base := makeBaseRounds()

		// Round 2: c1 has locally-written pk=3 but c2 has NO matching replicated data
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
			report, err := checker.CheckInNextTimeWindow(roundData)
			require.NoError(t, err, "round %d", i)
			lastReport = report
		}

		require.True(t, lastReport.NeedFlush())
		// c1 should detect data loss: pk=3 (commitTs=250) missing in c2's replicated data
		c1Report := lastReport.ClusterReports["c1"]
		require.NotNil(t, c1Report)
		require.Contains(t, c1Report.TableFailureItems, defaultSchemaKey)
		tableItems := c1Report.TableFailureItems[defaultSchemaKey]
		require.Len(t, tableItems.DataLossItems, 1)
		require.Equal(t, "c2", tableItems.DataLossItems[0].PeerClusterID)
		require.Equal(t, uint64(250), tableItems.DataLossItems[0].CommitTS)
		// c2 should have no issues
		c2Report := lastReport.ClusterReports["c2"]
		require.Empty(t, c2Report.TableFailureItems)
	})

	t.Run("data inconsistent detected", func(t *testing.T) {
		t.Parallel()
		checker := NewDataChecker(ctx, clusterCfg, nil, nil)
		base := makeBaseRounds()

		// Round 2: c2 has replicated data for pk=3 but with wrong column value
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
			report, err := checker.CheckInNextTimeWindow(roundData)
			require.NoError(t, err, "round %d", i)
			lastReport = report
		}

		require.True(t, lastReport.NeedFlush())
		c1Report := lastReport.ClusterReports["c1"]
		require.Contains(t, c1Report.TableFailureItems, defaultSchemaKey)
		tableItems := c1Report.TableFailureItems[defaultSchemaKey]
		require.Empty(t, tableItems.DataLossItems)
		require.Len(t, tableItems.DataInconsistentItems, 1)
		require.Equal(t, "c2", tableItems.DataInconsistentItems[0].PeerClusterID)
		require.Equal(t, uint64(250), tableItems.DataInconsistentItems[0].OriginTS)
		require.Equal(t, uint64(250), tableItems.DataInconsistentItems[0].LocalCommitTS)
		require.Equal(t, uint64(260), tableItems.DataInconsistentItems[0].ReplicatedCommitTS)
		require.Len(t, tableItems.DataInconsistentItems[0].InconsistentColumns, 1)
		require.Equal(t, "val", tableItems.DataInconsistentItems[0].InconsistentColumns[0].Column)
		require.Equal(t, "c", tableItems.DataInconsistentItems[0].InconsistentColumns[0].Local)
		require.Equal(t, "WRONG", tableItems.DataInconsistentItems[0].InconsistentColumns[0].Replicated)
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
		// Round 3: c2 has an extra replicated pk=99 (originTs=330) that doesn't match
		// any locally-written record in c1
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
			report, err := checker.CheckInNextTimeWindow(roundData)
			require.NoError(t, err, "round %d", i)
			lastReport = report
		}

		require.True(t, lastReport.NeedFlush())
		// c1 should have no data loss
		c1Report := lastReport.ClusterReports["c1"]
		require.Empty(t, c1Report.TableFailureItems)
		// c2 should detect data redundant: pk=99 has no matching locally-written record in c1
		c2Report := lastReport.ClusterReports["c2"]
		require.Contains(t, c2Report.TableFailureItems, defaultSchemaKey)
		tableItems := c2Report.TableFailureItems[defaultSchemaKey]
		require.Len(t, tableItems.DataRedundantItems, 1)
		require.Equal(t, uint64(330), tableItems.DataRedundantItems[0].OriginTS)
		require.Equal(t, uint64(340), tableItems.DataRedundantItems[0].CommitTS)
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
		// Round 3: c1 has locally-written pk=5 (commitTs=350, compareTs=350) and
		// replicated pk=5 from c2 (commitTs=370, originTs=310, compareTs=310).
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
			report, err := checker.CheckInNextTimeWindow(roundData)
			require.NoError(t, err, "round %d", i)
			lastReport = report
		}

		require.True(t, lastReport.NeedFlush())
		c1Report := lastReport.ClusterReports["c1"]
		require.Contains(t, c1Report.TableFailureItems, defaultSchemaKey)
		c1TableItems := c1Report.TableFailureItems[defaultSchemaKey]
		require.Len(t, c1TableItems.LWWViolationItems, 1)
		require.Equal(t, uint64(0), c1TableItems.LWWViolationItems[0].ExistingOriginTS)
		require.Equal(t, uint64(350), c1TableItems.LWWViolationItems[0].ExistingCommitTS)
		require.Equal(t, uint64(310), c1TableItems.LWWViolationItems[0].OriginTS)
		require.Equal(t, uint64(370), c1TableItems.LWWViolationItems[0].CommitTS)
		// c2 should have no LWW violation (its records are ordered correctly:
		// locally-written commitTs=310 compareTs=310, replicated commitTs=360 compareTs=350, 310 < 350)
		c2Report := lastReport.ClusterReports["c2"]
		if c2TableItems, ok := c2Report.TableFailureItems[defaultSchemaKey]; ok {
			require.Empty(t, c2TableItems.LWWViolationItems)
		}
	})

	// lww violation detected at round 1: LWW is active from round 1,
	// so a violation introduced in round 1 data should surface immediately.
	t.Run("lww violation detected at round 1", func(t *testing.T) {
		t.Parallel()
		checker := NewDataChecker(ctx, clusterCfg, nil, nil)

		// Round 0: [0, 100] — c1 writes pk=1 (commitTs=50, compareTs=50)
		round0 := map[string]types.TimeWindowData{
			"c1": makeTWData(0, 100, nil,
				makeContent(makeCanalJSON(1, 50, 0, "a"))),
			"c2": makeTWData(0, 100, nil, nil),
		}
		// Round 1: [100, 200] — c1 writes pk=1 again:
		//   locally-written  pk=1 (commitTs=150, originTs=0,  compareTs=150)
		//   replicated       pk=1 (commitTs=180, originTs=120, compareTs=120)
		// The LWW cache already has pk=1 compareTs=50 from round 0.
		// Record order: commitTs=150 (compareTs=150 > cached 50 → update cache),
		//               commitTs=180 (compareTs=120 < cached 150 → VIOLATION)
		round1 := map[string]types.TimeWindowData{
			"c1": makeTWData(100, 200, nil,
				makeContent(
					makeCanalJSON(1, 150, 0, "b"),
					makeCanalJSON(1, 180, 120, "b"),
				)),
			"c2": makeTWData(100, 200, nil, nil),
		}

		report0, err := checker.CheckInNextTimeWindow(round0)
		require.NoError(t, err)
		require.False(t, report0.NeedFlush(), "round 0 should not need flush")

		report1, err := checker.CheckInNextTimeWindow(round1)
		require.NoError(t, err)

		// LWW violation should be detected at round 1
		require.True(t, report1.NeedFlush(), "round 1 should detect LWW violation")
		c1Report := report1.ClusterReports["c1"]
		require.Contains(t, c1Report.TableFailureItems, defaultSchemaKey)
		c1TableItems := c1Report.TableFailureItems[defaultSchemaKey]
		require.Len(t, c1TableItems.LWWViolationItems, 1)
		require.Equal(t, uint64(0), c1TableItems.LWWViolationItems[0].ExistingOriginTS)
		require.Equal(t, uint64(150), c1TableItems.LWWViolationItems[0].ExistingCommitTS)
		require.Equal(t, uint64(120), c1TableItems.LWWViolationItems[0].OriginTS)
		require.Equal(t, uint64(180), c1TableItems.LWWViolationItems[0].CommitTS)
	})

	// data loss detected at round 2: Data loss detection is active from round 2.
	// A record in round 1 whose commitTs > checkpointTs will enter [1] at round 2,
	// and if the replicated counterpart is missing, data loss is detected at round 2.
	t.Run("data loss detected at round 2", func(t *testing.T) {
		t.Parallel()
		checker := NewDataChecker(ctx, clusterCfg, nil, nil)

		round0 := map[string]types.TimeWindowData{
			"c1": makeTWData(0, 100, nil, nil),
			"c2": makeTWData(0, 100, nil, nil),
		}
		// Round 1: c1 writes pk=1 (commitTs=150), checkpointTs["c2"]=140
		// Since 150 > 140, this record needs replication checking.
		round1 := map[string]types.TimeWindowData{
			"c1": makeTWData(100, 200, map[string]uint64{"c2": 140},
				makeContent(makeCanalJSON(1, 150, 0, "a"))),
			"c2": makeTWData(100, 200, nil, nil), // c2 has NO replicated data
		}
		// Round 2: round 1 data is now in [1], data loss detection enabled.
		round2 := map[string]types.TimeWindowData{
			"c1": makeTWData(200, 300, map[string]uint64{"c2": 280},
				makeContent(makeCanalJSON(2, 250, 0, "b"))),
			"c2": makeTWData(200, 300, nil,
				makeContent(makeCanalJSON(2, 260, 250, "b"))),
		}

		report0, err := checker.CheckInNextTimeWindow(round0)
		require.NoError(t, err)
		require.False(t, report0.NeedFlush())

		report1, err := checker.CheckInNextTimeWindow(round1)
		require.NoError(t, err)
		require.False(t, report1.NeedFlush(), "round 1 should not detect data loss yet")

		report2, err := checker.CheckInNextTimeWindow(round2)
		require.NoError(t, err)
		require.True(t, report2.NeedFlush(), "round 2 should detect data loss")
		c1Report := report2.ClusterReports["c1"]
		require.Contains(t, c1Report.TableFailureItems, defaultSchemaKey)
		tableItems := c1Report.TableFailureItems[defaultSchemaKey]
		require.Len(t, tableItems.DataLossItems, 1)
		require.Equal(t, "c2", tableItems.DataLossItems[0].PeerClusterID)
		require.Equal(t, uint64(150), tableItems.DataLossItems[0].CommitTS)
	})

	// data redundant detected at round 3 (not round 2):
	// dataRedundantDetection checks timeWindowDataCaches[2] (latest round).
	// At round 2 [0]=round 0 (empty) so FindSourceLocalData may miss data in
	// that window → enableDataRedundant is false to avoid false positives.
	// At round 3 [0]=round 1, [1]=round 2, [2]=round 3 are all populated
	// with real data, so enableDataRedundant=true and an orphan in [2] is caught.
	//
	// This test puts the SAME orphan pk=99 in both round 2 and round 3:
	//   - Round 2: orphan in [2] but enableDataRedundant=false → NOT flagged.
	//   - Round 3: orphan in [2] and enableDataRedundant=true  → flagged.
	t.Run("data redundant detected at round 3 not round 2", func(t *testing.T) {
		t.Parallel()
		checker := NewDataChecker(ctx, clusterCfg, nil, nil)

		round0 := map[string]types.TimeWindowData{
			"c1": makeTWData(0, 100, nil, nil),
			"c2": makeTWData(0, 100, nil, nil),
		}
		// Round 1: normal consistent data.
		round1 := map[string]types.TimeWindowData{
			"c1": makeTWData(100, 200, map[string]uint64{"c2": 180},
				makeContent(makeCanalJSON(1, 150, 0, "a"))),
			"c2": makeTWData(100, 200, nil,
				makeContent(makeCanalJSON(1, 160, 150, "a"))),
		}
		// Round 2: c2 has orphan replicated pk=99 (originTs=230) in [2].
		// enableDataRedundant=false at round 2, so it must NOT be flagged.
		round2 := map[string]types.TimeWindowData{
			"c1": makeTWData(200, 300, map[string]uint64{"c2": 280},
				makeContent(makeCanalJSON(2, 250, 0, "b"))),
			"c2": makeTWData(200, 300, nil,
				makeContent(
					makeCanalJSON(2, 260, 250, "b"),
					makeCanalJSON(99, 240, 230, "x"), // orphan replicated
				)),
		}
		// Round 3: c2 has another orphan replicated pk=99 (originTs=330) in [2].
		// enableDataRedundant=true at round 3, so it IS caught.
		round3 := map[string]types.TimeWindowData{
			"c1": makeTWData(300, 400, map[string]uint64{"c2": 380},
				makeContent(makeCanalJSON(3, 350, 0, "c"))),
			"c2": makeTWData(300, 400, nil,
				makeContent(
					makeCanalJSON(3, 360, 350, "c"),
					makeCanalJSON(99, 340, 330, "y"), // orphan replicated
				)),
		}

		report0, err := checker.CheckInNextTimeWindow(round0)
		require.NoError(t, err)
		require.False(t, report0.NeedFlush(), "round 0 should not need flush")

		report1, err := checker.CheckInNextTimeWindow(round1)
		require.NoError(t, err)
		require.False(t, report1.NeedFlush(), "round 1 should not need flush")

		report2, err := checker.CheckInNextTimeWindow(round2)
		require.NoError(t, err)
		// Round 2: redundant detection is NOT enabled; the orphan pk=99 should NOT be flagged.
		require.False(t, report2.NeedFlush(), "round 2 should not flag data redundant yet")

		report3, err := checker.CheckInNextTimeWindow(round3)
		require.NoError(t, err)
		// Round 3: redundant detection is enabled; the orphan pk=99 in [2] (round 3)
		// is now caught.
		require.True(t, report3.NeedFlush(), "round 3 should detect data redundant")
		c2Report := report3.ClusterReports["c2"]
		require.Contains(t, c2Report.TableFailureItems, defaultSchemaKey)
		c2TableItems := c2Report.TableFailureItems[defaultSchemaKey]
		require.Len(t, c2TableItems.DataRedundantItems, 1)
		require.Equal(t, uint64(330), c2TableItems.DataRedundantItems[0].OriginTS)
		require.Equal(t, uint64(340), c2TableItems.DataRedundantItems[0].CommitTS)
	})
}
