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

package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/advancer"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/checker"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/config"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/recorder"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/types"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/stretchr/testify/require"
)

// schemaKey is the schema key for data stored via S3 path "test/t1/1/...".
// It equals QuoteSchema("test", "t1") = "`test`.`t1`".
var schemaKey = (&cloudstorage.DmlPathKey{
	SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "test", Table: "t1", TableVersion: 1},
}).GetKey()

// testEnv holds the initialized test environment.
type testEnv struct {
	ctx      context.Context
	mc       *MockMultiCluster
	advancer *advancer.TimeWindowAdvancer
	checker  *checker.DataChecker
}

// setupEnv creates a test environment with 2 clusters (c1, c2), both
// replicating to each other, with in-memory S3 storage and mock PD/watchers.
func setupEnv(t *testing.T) *testEnv {
	t.Helper()
	ctx := context.Background()
	tables := map[string][]string{"test": {"t1"}}

	mc := NewMockMultiCluster(
		[]string{"c1", "c2"},
		tables,
		0,   // pdBase: start physical time at 0ms
		100, // pdStep: 100ms per PD GetTS call
		100, // cpDelta: checkpoint = minCheckpointTs + 100
		50,  // s3Delta: s3 checkpoint = minCheckpointTs + 50
	)

	require.NoError(t, mc.InitSchemaFiles(ctx))

	twa, _, err := advancer.NewTimeWindowAdvancer(
		ctx, mc.CPWatchers, mc.S3Watchers, mc.GetPDClients(), nil,
	)
	require.NoError(t, err)

	clusterCfg := map[string]config.ClusterConfig{"c1": {}, "c2": {}}
	dc := checker.NewDataChecker(ctx, clusterCfg, nil, nil)

	return &testEnv{ctx: ctx, mc: mc, advancer: twa, checker: dc}
}

// roundResult holds the output of a single round.
type roundResult struct {
	report *recorder.Report
	twData map[string]types.TimeWindowData
}

// executeRound writes data to clusters' S3 storage, advances the time window,
// and runs the checker for one round.
func (e *testEnv) executeRound(t *testing.T, c1Content, c2Content []byte) roundResult {
	t.Helper()
	if c1Content != nil {
		require.NoError(t, e.mc.WriteDMLFile(e.ctx, "c1", c1Content))
	}
	if c2Content != nil {
		require.NoError(t, e.mc.WriteDMLFile(e.ctx, "c2", c2Content))
	}

	twData, err := e.advancer.AdvanceTimeWindow(e.ctx)
	require.NoError(t, err)

	report, err := e.checker.CheckInNextTimeWindow(twData)
	require.NoError(t, err)

	return roundResult{report: report, twData: twData}
}

// maxRightBoundary returns the maximum RightBoundary across all clusters.
func maxRightBoundary(twData map[string]types.TimeWindowData) uint64 {
	maxRB := uint64(0)
	for _, tw := range twData {
		if tw.TimeWindow.RightBoundary > maxRB {
			maxRB = tw.TimeWindow.RightBoundary
		}
	}
	return maxRB
}

// The test architecture simulates a 2-cluster active-active setup:
//
//	c1 (locally-written records) ──CDC──> c2 (replicated records)
//	c2 (locally-written records) ──CDC──> c1 (replicated records)
//
// Each cluster writes locally-written records (originTs=0) and receives replicated
// records from the other cluster (originTs>0).
//
// The checker needs 3 warm-up rounds before it starts checking (checkableRound >= 3).
// Data written in round 0 is tracked by the S3 consumer but not downloaded
// (skipDownloadData=true for the first round). From round 1 onwards, only
// NEW files (with higher indices) are downloaded.
//
// Data commitTs is set to prevMaxRightBoundary+1 to ensure records fall
// within the current time window (leftBoundary, rightBoundary].
//
// TestIntegration_AllConsistent verifies that no errors are reported
// when all locally-written records have matching replicated records.
func TestIntegration_AllConsistent(t *testing.T) {
	t.Parallel()
	env := setupEnv(t)
	defer env.mc.Close()

	prevMaxRB := uint64(0)

	for round := 0; round < 6; round++ {
		cts := prevMaxRB + 1
		// c1: locally-written records write (originTs=0)
		c1 := MakeContent(MakeCanalJSON(round+1, cts, 0, fmt.Sprintf("v%d", round)))
		// c2: replicated records replicated from c1 (originTs = c1's commitTs)
		c2 := MakeContent(MakeCanalJSON(round+1, cts+1, cts, fmt.Sprintf("v%d", round)))

		result := env.executeRound(t, c1, c2)
		prevMaxRB = maxRightBoundary(result.twData)

		t.Logf("Round %d: c1 TW=[%d, %d], c2 TW=[%d, %d], commitTs=%d",
			round,
			result.twData["c1"].TimeWindow.LeftBoundary, result.twData["c1"].TimeWindow.RightBoundary,
			result.twData["c2"].TimeWindow.LeftBoundary, result.twData["c2"].TimeWindow.RightBoundary,
			cts)

		if round >= 3 {
			require.Len(t, result.report.ClusterReports, 2, "round %d", round)
			require.False(t, result.report.NeedFlush(),
				"round %d: all data should be consistent, no report needed", round)
			for clusterID, cr := range result.report.ClusterReports {
				require.Empty(t, cr.TableFailureItems,
					"round %d, cluster %s: should have no failures", round, clusterID)
			}
		}
	}
}

// TestIntegration_AllConsistent_CrossRoundReplicatedRecords verifies that the checker
// treats data as consistent when a locally-written record's commitTs exceeds the
// round's checkpointTs, and the matching replicated records only appears in the next
// round.
//
// This occurs when locally-written records commitTs happen late in the time window, after
// the checkpoint has already been determined. For TW[2], records with
// commitTs > checkpointTs are deferred (skipped). In the next round they
// become TW[1], where the check condition is commitTs > checkpointTs (checked),
// and the replicated records are searched in TW[1] + TW[2] — finding the match in
// the current round's TW[2].
func TestIntegration_AllConsistent_CrossRoundReplicatedRecords(t *testing.T) {
	t.Parallel()
	env := setupEnv(t)
	defer env.mc.Close()

	prevMaxRB := uint64(0)

	// Offset to place commitTs between checkpointTs and rightBoundary.
	// With pdStep=100 and 2 clusters, each round's time window spans
	// approximately ComposeTS(300, 0) = 78643200, and checkpointTs sits
	// at roughly ComposeTS(200, 0) from leftBoundary.
	// Using ComposeTS(250, 0) = 65536000 lands safely between them.
	crossRoundOffset := uint64(250 << 18) // ComposeTS(250, 0) = 65536000

	var lateLocallyWrittenRecordsCommitTs uint64

	for round := 0; round < 7; round++ {
		cts := prevMaxRB + 1

		var c1, c2 []byte

		switch round {
		case 4:
			// Round N: c1 local has two records:
			//   pk=round+1  normal commitTs (checked in this round's TW[2])
			//   pk=100      large commitTs > checkpointTs
			//                (deferred in TW[2], checked via TW[1] next round)
			// c2 replicated only matches pk=round+1.
			lateLocallyWrittenRecordsCommitTs = prevMaxRB + crossRoundOffset
			c1 = MakeContent(
				MakeCanalJSON(round+1, cts, 0, fmt.Sprintf("v%d", round)),
				MakeCanalJSON(100, lateLocallyWrittenRecordsCommitTs, 0, "late"),
			)
			c2 = MakeContent(MakeCanalJSON(round+1, cts+1, cts, fmt.Sprintf("v%d", round)))

		case 5:
			// Round N+1: c2 now includes the replicated record for pk=100.
			// The checker evaluates TW[1] (= round 4), finds pk=100 with
			// commitTs > checkpointTs, and searches c2's TW[1] + TW[2].
			// pk=100's matching replicated record is in c2's TW[2] (this round).
			c1 = MakeContent(MakeCanalJSON(round+1, cts, 0, fmt.Sprintf("v%d", round)))
			c2 = MakeContent(
				MakeCanalJSON(round+1, cts+1, cts, fmt.Sprintf("v%d", round)),
				MakeCanalJSON(100, cts+2, lateLocallyWrittenRecordsCommitTs, "late"),
			)

		default:
			c1 = MakeContent(MakeCanalJSON(round+1, cts, 0, fmt.Sprintf("v%d", round)))
			c2 = MakeContent(MakeCanalJSON(round+1, cts+1, cts, fmt.Sprintf("v%d", round)))
		}

		result := env.executeRound(t, c1, c2)
		prevMaxRB = maxRightBoundary(result.twData)

		t.Logf("Round %d: c1 TW=[%d, %d], cpTs=%v, commitTs=%d",
			round,
			result.twData["c1"].TimeWindow.LeftBoundary,
			result.twData["c1"].TimeWindow.RightBoundary,
			result.twData["c1"].TimeWindow.CheckpointTs,
			cts)

		if round == 4 {
			// Verify the late commitTs falls between checkpointTs and rightBoundary.
			c1TW := result.twData["c1"].TimeWindow
			cpTs := c1TW.CheckpointTs["c2"]
			require.Greater(t, lateLocallyWrittenRecordsCommitTs, cpTs,
				"lateLocallyWrittenRecordsCommitTs must be > checkpointTs for cross-round detection")
			require.LessOrEqual(t, lateLocallyWrittenRecordsCommitTs, c1TW.RightBoundary,
				"lateLocallyWrittenRecordsCommitTs must be <= rightBoundary to stay in this time window")
			t.Logf("Round 4 verification: lateCommitTs=%d, checkpointTs=%d, rightBoundary=%d",
				lateLocallyWrittenRecordsCommitTs, cpTs, c1TW.RightBoundary)
		}

		if round >= 3 {
			require.Len(t, result.report.ClusterReports, 2, "round %d", round)
			require.False(t, result.report.NeedFlush(),
				"round %d: data should be consistent (cross-round matching should work)", round)
			for clusterID, cr := range result.report.ClusterReports {
				require.Empty(t, cr.TableFailureItems,
					"round %d, cluster %s: should have no failures", round, clusterID)
			}
		}
	}
}

// TestIntegration_AllConsistent_LWWSkippedReplicatedRecords verifies that no errors
// are reported when a replicated record is "LWW-skipped" during data-loss
// detection, combined with cross-time-window matching.
//
// pk=100: single-cluster overwrite (c1 writes old+new, c2 only has newer replicated records)
//
//	Round N:   c1 locally-written records pk=100 × 2 (commitTs=A, B; both > checkpointTs)
//	           c2 has NO replicated records for pk=100
//	Round N+1: c2 replicated records pk=100 (originTs=B, matches newer locally-written records only)
//	  → old locally-written records LWW-skipped (c2 replicated records compareTs=B >= A)
//
// pk=200: bidirectional write (c1 and c2 both write the same pk)
//
//	Round N:   c1 locally-written records pk=200 (commitTs=A, deferred)
//	Round N+1: c1 locally-written records pk=200 (commitTs=E, newer), c1 replicated records pk=200 (originTs=D, from c2)
//	           c2 replicated records pk=200 (commitTs=D, D < E),  c2 replicated records pk=200 (originTs=E, from c1)
//
//	Key constraint: c1 local commitTs (E) > c2 local commitTs (D).
//	This ensures that on c2, the replicated (compareTs=E) > local (compareTs=D),
//	so the LWW violation checker sees monotonically increasing compareTs.
//
//	c1 data loss for old pk=200 (commitTs=A):
//	  → c2 replicated has originTs=E, compareTs=E >= A → LWW-skipped ✓
//	c1 data loss for new pk=200 (commitTs=E):
//	  → c2 replicated has originTs=E → exact match ✓
//	c2 data loss for c2 local pk=200 (commitTs=D):
//	  → c1 replicated has originTs=D → exact match ✓
func TestIntegration_AllConsistent_LWWSkippedReplicatedRecords(t *testing.T) {
	t.Parallel()
	env := setupEnv(t)
	defer env.mc.Close()

	prevMaxRB := uint64(0)

	// Place both commitTs values between checkpointTs and rightBoundary.
	// With pdStep=100 and 2 clusters:
	//   window width ≈ ComposeTS(300, 0), checkpointTs ≈ leftBoundary + ComposeTS(200, 0)
	// Using ComposeTS(250, 0) puts us safely in the gap.
	crossRoundOffset := uint64(250 << 18) // ComposeTS(250, 0) = 65536000

	var oldCommitTs, newCommitTs uint64

	for round := 0; round < 7; round++ {
		cts := prevMaxRB + 1

		var c1, c2 []byte

		switch round {
		case 4:
			// Round N: c1 local writes pk=100 twice + pk=200 once, all > checkpointTs.
			// c2 has NO replicated record for pk=100 or pk=200; they arrive next round.
			oldCommitTs = prevMaxRB + crossRoundOffset
			newCommitTs = oldCommitTs + 5
			c1 = MakeContent(
				MakeCanalJSON(round+1, cts, 0, fmt.Sprintf("v%d", round)),
				MakeCanalJSON(100, oldCommitTs, 0, "old_write"),
				MakeCanalJSON(100, newCommitTs, 0, "new_write"),
				MakeCanalJSON(200, oldCommitTs, 0, "old_write"),
			)
			c2 = MakeContent(
				MakeCanalJSON(round+1, cts+1, cts, fmt.Sprintf("v%d", round)),
			)

		case 5:
			// Round N+1: replicated data arrives for both pk=100 and pk=200.
			//
			// pk=200 bidirectional: c1 local at cts+5 (> c2 local at cts+2)
			// ensures c2's LWW check sees increasing compareTs.
			//   c1: replicated(commitTs=cts+4, originTs=cts+2) then local(commitTs=cts+5)
			//     → compareTs order: cts+2 < cts+5 ✓
			//   c2: local(commitTs=cts+2) then replicated(commitTs=cts+6, originTs=cts+5)
			//     → compareTs order: cts+2 < cts+5 ✓
			c1 = MakeContent(
				MakeCanalJSON(round+1, cts, 0, fmt.Sprintf("v%d", round)),
				MakeCanalJSON(200, cts+4, cts+2, "pk200_c2"), // c1 replicated pk=200 from c2
				MakeCanalJSON(200, cts+5, 0, "pk200_c1"),     // c1 local pk=200 (newer)
			)
			c2 = MakeContent(
				MakeCanalJSON(round+1, cts+1, cts, fmt.Sprintf("v%d", round)),
				MakeCanalJSON(100, cts+2, newCommitTs, "new_write"),
				MakeCanalJSON(200, cts+2, 0, "pk200_c2"),     // c2 local pk=200
				MakeCanalJSON(200, cts+6, cts+5, "pk200_c1"), // c2 replicated pk=200 from c1
			)

		default:
			c1 = MakeContent(MakeCanalJSON(round+1, cts, 0, fmt.Sprintf("v%d", round)))
			c2 = MakeContent(MakeCanalJSON(round+1, cts+1, cts, fmt.Sprintf("v%d", round)))
		}

		result := env.executeRound(t, c1, c2)
		prevMaxRB = maxRightBoundary(result.twData)

		t.Logf("Round %d: NeedFlush=%v, commitTs=%d", round, result.report.NeedFlush(), cts)

		if round == 4 {
			// Verify both commitTs fall between checkpointTs and rightBoundary.
			c1TW := result.twData["c1"].TimeWindow
			cpTs := c1TW.CheckpointTs["c2"]
			require.Greater(t, oldCommitTs, cpTs,
				"oldCommitTs must be > checkpointTs for cross-round deferral")
			require.LessOrEqual(t, newCommitTs, c1TW.RightBoundary,
				"newCommitTs must be <= rightBoundary to stay in this time window")
			t.Logf("Round 4 verification: oldCommitTs=%d, newCommitTs=%d, checkpointTs=%d, rightBoundary=%d",
				oldCommitTs, newCommitTs, cpTs, c1TW.RightBoundary)
		}

		if round >= 3 {
			require.Len(t, result.report.ClusterReports, 2, "round %d", round)
			require.False(t, result.report.NeedFlush(),
				"round %d: cross-round LWW-skipped replicated should not cause errors", round)
			for clusterID, cr := range result.report.ClusterReports {
				require.Empty(t, cr.TableFailureItems,
					"round %d, cluster %s: should have no failures", round, clusterID)
			}
		}
	}
}

// TestIntegration_DataLoss verifies that the checker detects data loss
// when a locally-written record has no matching replicated record in the other cluster.
func TestIntegration_DataLoss(t *testing.T) {
	t.Parallel()
	env := setupEnv(t)
	defer env.mc.Close()

	prevMaxRB := uint64(0)
	dataLossDetected := false

	for round := 0; round < 6; round++ {
		cts := prevMaxRB + 1

		// c1 always produces local data
		c1 := MakeContent(MakeCanalJSON(round+1, cts, 0, fmt.Sprintf("v%d", round)))

		var c2 []byte
		if round == 4 {
			// Round 4: c2 has NO matching replicated record → data loss expected
			// (round 4's data is checked in the same round since checkableRound >= 3)
			c2 = nil
		} else {
			// Normal: c2 has matching replicated record
			c2 = MakeContent(MakeCanalJSON(round+1, cts+1, cts, fmt.Sprintf("v%d", round)))
		}

		result := env.executeRound(t, c1, c2)
		prevMaxRB = maxRightBoundary(result.twData)

		t.Logf("Round %d: NeedFlush=%v, commitTs=%d", round, result.report.NeedFlush(), cts)

		if round >= 3 && result.report.NeedFlush() {
			c1Report := result.report.ClusterReports["c1"]
			if c1Report != nil {
				if items, ok := c1Report.TableFailureItems[schemaKey]; ok {
					if len(items.DataLossItems) > 0 {
						t.Logf("Round %d: detected data loss: %+v", round, items.DataLossItems)
						dataLossDetected = true
						// Verify the data loss item
						for _, item := range items.DataLossItems {
							require.Equal(t, "c2", item.PeerClusterID)
						}
					}
				}
			}
		}
	}

	require.True(t, dataLossDetected, "data loss should have been detected")
}

// TestIntegration_DataInconsistent verifies that the checker detects data
// inconsistency when a replicated record has different column values
// from the locally-written record.
func TestIntegration_DataInconsistent(t *testing.T) {
	t.Parallel()
	env := setupEnv(t)
	defer env.mc.Close()

	prevMaxRB := uint64(0)
	inconsistentDetected := false

	for round := 0; round < 6; round++ {
		cts := prevMaxRB + 1

		c1 := MakeContent(MakeCanalJSON(round+1, cts, 0, fmt.Sprintf("v%d", round)))

		var c2 []byte
		if round == 4 {
			// Round 4: c2 has replicated record with WRONG column value
			c2 = MakeContent(MakeCanalJSON(round+1, cts+1, cts, "WRONG_VALUE"))
		} else {
			c2 = MakeContent(MakeCanalJSON(round+1, cts+1, cts, fmt.Sprintf("v%d", round)))
		}

		result := env.executeRound(t, c1, c2)
		prevMaxRB = maxRightBoundary(result.twData)

		t.Logf("Round %d: NeedFlush=%v, commitTs=%d", round, result.report.NeedFlush(), cts)

		if round >= 3 && result.report.NeedFlush() {
			c1Report := result.report.ClusterReports["c1"]
			if c1Report != nil {
				if items, ok := c1Report.TableFailureItems[schemaKey]; ok {
					for _, item := range items.DataInconsistentItems {
						t.Logf("Round %d: detected data inconsistency: %+v", round, item)
						inconsistentDetected = true
						require.Equal(t, "c2", item.PeerClusterID)
					}
				}
			}
		}
	}

	require.True(t, inconsistentDetected, "data inconsistency should have been detected")
}

// TestIntegration_DataRedundant verifies that the checker detects redundant
// replicated data that has no matching locally-written record.
func TestIntegration_DataRedundant(t *testing.T) {
	t.Parallel()
	env := setupEnv(t)
	defer env.mc.Close()

	prevMaxRB := uint64(0)
	redundantDetected := false

	for round := 0; round < 6; round++ {
		cts := prevMaxRB + 1

		c1 := MakeContent(MakeCanalJSON(round+1, cts, 0, fmt.Sprintf("v%d", round)))
		c2 := MakeContent(MakeCanalJSON(round+1, cts+1, cts, fmt.Sprintf("v%d", round)))

		if round == 4 {
			// Round 4: c2 has an EXTRA replicated record (pk=999) with a fake
			// originTs that doesn't match any c1 local commitTs.
			fakeOriginTs := cts - 5 // Doesn't match any c1 local commitTs
			c2 = MakeContent(
				MakeCanalJSON(round+1, cts+1, cts, fmt.Sprintf("v%d", round)),
				MakeCanalJSON(999, cts+2, fakeOriginTs, "extra"),
			)
		}

		result := env.executeRound(t, c1, c2)
		prevMaxRB = maxRightBoundary(result.twData)

		t.Logf("Round %d: NeedFlush=%v, commitTs=%d", round, result.report.NeedFlush(), cts)

		if round >= 3 && result.report.NeedFlush() {
			c2Report := result.report.ClusterReports["c2"]
			if c2Report != nil {
				if items, ok := c2Report.TableFailureItems[schemaKey]; ok {
					if len(items.DataRedundantItems) > 0 {
						t.Logf("Round %d: detected data redundant: %+v", round, items.DataRedundantItems)
						redundantDetected = true
					}
				}
			}
		}
	}

	require.True(t, redundantDetected, "data redundancy should have been detected")
}

// TestIntegration_LWWViolation verifies that the checker detects Last Write Wins
// violations when records for the same primary key have non-monotonic origin timestamps.
func TestIntegration_LWWViolation(t *testing.T) {
	t.Parallel()
	env := setupEnv(t)
	defer env.mc.Close()

	prevMaxRB := uint64(0)
	lwwViolationDetected := false

	for round := 0; round < 6; round++ {
		cts := prevMaxRB + 1

		var c1, c2 []byte

		if round == 4 {
			// Round 4: inject LWW violation in c1.
			// Record A: pk=5, commitTs=cts, originTs=0 → compareTs = cts
			// Record B: pk=5, commitTs=cts+2, originTs=cts-10 → compareTs = cts-10
			// Since A's compareTs (cts) >= B's compareTs (cts-10) and A's commitTs < B's commitTs,
			// this is a Last Write Wins violation.
			c1 = MakeContent(
				MakeCanalJSON(5, cts, 0, "original"),
				MakeCanalJSON(5, cts+2, cts-10, "replicated"),
			)
			// c2: provide matching replicated record to avoid data loss noise
			c2 = MakeContent(
				MakeCanalJSON(5, cts+1, cts, "original"),
			)
		} else {
			c1 = MakeContent(MakeCanalJSON(round+1, cts, 0, fmt.Sprintf("v%d", round)))
			c2 = MakeContent(MakeCanalJSON(round+1, cts+1, cts, fmt.Sprintf("v%d", round)))
		}

		result := env.executeRound(t, c1, c2)
		prevMaxRB = maxRightBoundary(result.twData)

		t.Logf("Round %d: NeedFlush=%v, commitTs=%d", round, result.report.NeedFlush(), cts)

		if round >= 3 && result.report.NeedFlush() {
			c1Report := result.report.ClusterReports["c1"]
			if c1Report != nil {
				if items, ok := c1Report.TableFailureItems[schemaKey]; ok {
					if len(items.LWWViolationItems) > 0 {
						t.Logf("Round %d: detected LWW violation: %+v", round, items.LWWViolationItems)
						lwwViolationDetected = true
					}
				}
			}
		}
	}

	require.True(t, lwwViolationDetected, "LWW violation should have been detected")
}

// TestIntegration_LWWViolation_AcrossRounds verifies that the checker detects
// LWW violations when conflicting records for the same pk appear in rounds N
// and N+2, with no data for that pk in round N+1.
//
// The clusterViolationChecker keeps cache entries for up to 3 rounds
// (previous: 0 → 1 → 2). Since Check runs before UpdateCache, an entry
// created in round N (previous=0) is still available at previous=2 when
// round N+2 runs.
//
// Timeline:
//
//	Round N:   c1 local pk=50 (originTs=0, compareTs=A)  → cached
//	Round N+1: no pk=50 data                                → cache ages (prev 1→2)
//	Round N+2: c1 replicated pk=50 (originTs=B<A)           → violation detected
func TestIntegration_LWWViolation_AcrossRounds(t *testing.T) {
	t.Parallel()
	env := setupEnv(t)
	defer env.mc.Close()

	prevMaxRB := uint64(0)
	lwwViolationDetected := false
	var firstRecordCommitTs uint64

	for round := 0; round < 7; round++ {
		cts := prevMaxRB + 1

		var c1, c2 []byte

		switch round {
		case 4:
			// Round N: c1 local pk=50. The violation checker caches:
			//   pk=50 → {previous: 0, commitTs: cts, originTs: 0, compareTs: cts}
			firstRecordCommitTs = cts
			c1 = MakeContent(MakeCanalJSON(50, cts, 0, "first"))
			c2 = MakeContent(MakeCanalJSON(50, cts+1, cts, "first"))

		case 5:
			// Round N+1: no pk=50 data. After UpdateCache the cached entry
			// for pk=50 ages to previous=2 (still within retention window).
			c1 = MakeContent(MakeCanalJSON(round+1, cts, 0, fmt.Sprintf("v%d", round)))
			c2 = MakeContent(MakeCanalJSON(round+1, cts+1, cts, fmt.Sprintf("v%d", round)))

		case 6:
			// Round N+2: c1 has a replicated record for pk=50 whose originTs
			// is less than the cached compareTs from round N.
			//   existing:  compareTs = firstRecordCommitTs
			//   new:       compareTs = firstRecordCommitTs - 10
			//   existing.commitTs < new.commitTs, but existing.compareTs >= new.compareTs
			//   → LWW violation across 2-round gap.
			violatingOriginTs := firstRecordCommitTs - 10
			c1 = MakeContent(
				MakeCanalJSON(round+1, cts, 0, fmt.Sprintf("v%d", round)),
				MakeCanalJSON(50, cts+2, violatingOriginTs, "second"),
			)
			c2 = MakeContent(MakeCanalJSON(round+1, cts+1, cts, fmt.Sprintf("v%d", round)))

		default:
			c1 = MakeContent(MakeCanalJSON(round+1, cts, 0, fmt.Sprintf("v%d", round)))
			c2 = MakeContent(MakeCanalJSON(round+1, cts+1, cts, fmt.Sprintf("v%d", round)))
		}

		result := env.executeRound(t, c1, c2)
		prevMaxRB = maxRightBoundary(result.twData)

		t.Logf("Round %d: NeedFlush=%v, commitTs=%d", round, result.report.NeedFlush(), cts)

		if round >= 3 && result.report.NeedFlush() {
			c1Report := result.report.ClusterReports["c1"]
			if c1Report != nil {
				if items, ok := c1Report.TableFailureItems[schemaKey]; ok {
					if len(items.LWWViolationItems) > 0 {
						t.Logf("Round %d: LWW violation across rounds: %+v",
							round, items.LWWViolationItems)
						lwwViolationDetected = true
						// Verify the violation details
						item := items.LWWViolationItems[0]
						require.Equal(t, uint64(0), item.ExistingOriginTS,
							"existing record should be local (originTs=0)")
						require.Equal(t, firstRecordCommitTs, item.ExistingCommitTS,
							"existing record should be from round N")
					}
				}
			}
		}
	}

	require.True(t, lwwViolationDetected,
		"LWW violation across round N and N+2 should have been detected")
}

// TestIntegration_MultipleErrorTypes verifies that the checker can detect
// multiple error types simultaneously across different clusters and rounds.
func TestIntegration_MultipleErrorTypes(t *testing.T) {
	t.Parallel()
	env := setupEnv(t)
	defer env.mc.Close()

	prevMaxRB := uint64(0)
	dataLossDetected := false
	redundantDetected := false

	for round := 0; round < 7; round++ {
		cts := prevMaxRB + 1

		var c1, c2 []byte

		switch round {
		case 4:
			// Data loss: c1 local pk=5, c2 has NO replicated record
			c1 = MakeContent(MakeCanalJSON(5, cts, 0, "lost"))
			c2 = nil
		case 5:
			// Data redundant: c2 has extra replicated pk=888
			c1 = MakeContent(MakeCanalJSON(6, cts, 0, "normal"))
			fakeOriginTs := cts - 3
			c2 = MakeContent(
				MakeCanalJSON(6, cts+1, cts, "normal"),
				MakeCanalJSON(888, cts+2, fakeOriginTs, "ghost"),
			)
		default:
			c1 = MakeContent(MakeCanalJSON(round+1, cts, 0, fmt.Sprintf("v%d", round)))
			c2 = MakeContent(MakeCanalJSON(round+1, cts+1, cts, fmt.Sprintf("v%d", round)))
		}

		result := env.executeRound(t, c1, c2)
		prevMaxRB = maxRightBoundary(result.twData)

		t.Logf("Round %d: NeedFlush=%v, commitTs=%d, ClusterReports=%d",
			round, result.report.NeedFlush(), cts, len(result.report.ClusterReports))

		if round >= 3 && result.report.NeedFlush() {
			// Check c1 for data loss
			if c1Report := result.report.ClusterReports["c1"]; c1Report != nil {
				if items, ok := c1Report.TableFailureItems[schemaKey]; ok {
					if len(items.DataLossItems) > 0 {
						dataLossDetected = true
						t.Logf("Round %d: data loss detected in c1: %d items",
							round, len(items.DataLossItems))
					}
				}
			}
			// Check c2 for data redundant
			if c2Report := result.report.ClusterReports["c2"]; c2Report != nil {
				if items, ok := c2Report.TableFailureItems[schemaKey]; ok {
					if len(items.DataRedundantItems) > 0 {
						redundantDetected = true
						t.Logf("Round %d: data redundant detected in c2: %d items",
							round, len(items.DataRedundantItems))
					}
				}
			}
		}
	}

	require.True(t, dataLossDetected, "data loss should have been detected")
	require.True(t, redundantDetected, "data redundancy should have been detected")
}
