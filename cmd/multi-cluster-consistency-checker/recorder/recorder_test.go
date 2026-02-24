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

package recorder

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/config"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/types"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestNewRecorder(t *testing.T) {
	t.Parallel()

	t.Run("creates directories", func(t *testing.T) {
		t.Parallel()
		dataDir := t.TempDir()
		r, err := NewRecorder(dataDir, map[string]config.ClusterConfig{"c1": {}}, 0)
		require.NoError(t, err)
		require.NotNil(t, r)

		// Verify directories exist
		info, err := os.Stat(filepath.Join(dataDir, "report"))
		require.NoError(t, err)
		require.True(t, info.IsDir())

		info, err = os.Stat(filepath.Join(dataDir, "checkpoint"))
		require.NoError(t, err)
		require.True(t, info.IsDir())
	})

	t.Run("checkpoint is initialized empty", func(t *testing.T) {
		t.Parallel()
		dataDir := t.TempDir()
		r, err := NewRecorder(dataDir, map[string]config.ClusterConfig{"c1": {}}, 0)
		require.NoError(t, err)

		cp := r.GetCheckpoint()
		require.NotNil(t, cp)
		require.Nil(t, cp.CheckpointItems[0])
		require.Nil(t, cp.CheckpointItems[1])
		require.Nil(t, cp.CheckpointItems[2])
	})

	t.Run("loads existing checkpoint on startup", func(t *testing.T) {
		t.Parallel()
		dataDir := t.TempDir()

		// First recorder: write a checkpoint
		r1, err := NewRecorder(dataDir, map[string]config.ClusterConfig{"c1": {}}, 0)
		require.NoError(t, err)

		twData := map[string]types.TimeWindowData{
			"c1": {
				TimeWindow: types.TimeWindow{LeftBoundary: 1, RightBoundary: 10},
				MaxVersion: map[types.SchemaTableKey]types.VersionKey{
					{Schema: "db", Table: "tbl"}: {Version: 1, VersionPath: "vp1", DataPath: "dp1"},
				},
			},
		}
		report := NewReport(0)
		err = r1.RecordTimeWindow(twData, report)
		require.NoError(t, err)

		// Second recorder: should load the checkpoint
		r2, err := NewRecorder(dataDir, map[string]config.ClusterConfig{"c1": {}}, 0)
		require.NoError(t, err)

		cp := r2.GetCheckpoint()
		require.NotNil(t, cp.CheckpointItems[2])
		require.Equal(t, uint64(0), cp.CheckpointItems[2].Round)
		info := cp.CheckpointItems[2].ClusterInfo["c1"]
		require.Equal(t, uint64(1), info.TimeWindow.LeftBoundary)
		require.Equal(t, uint64(10), info.TimeWindow.RightBoundary)
	})

	t.Run("cluster count mismatch rejects checkpoint", func(t *testing.T) {
		t.Parallel()
		dataDir := t.TempDir()

		// Write a checkpoint with 2 clusters
		r1, err := NewRecorder(dataDir, map[string]config.ClusterConfig{"c1": {}, "c2": {}}, 0)
		require.NoError(t, err)
		twData := map[string]types.TimeWindowData{
			"c1": {TimeWindow: types.TimeWindow{LeftBoundary: 0, RightBoundary: 10}},
			"c2": {TimeWindow: types.TimeWindow{LeftBoundary: 0, RightBoundary: 10}},
		}
		err = r1.RecordTimeWindow(twData, NewReport(0))
		require.NoError(t, err)

		// Try to load with only 1 cluster — should fail
		_, err = NewRecorder(dataDir, map[string]config.ClusterConfig{"c1": {}}, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cluster info length mismatch")
	})

	t.Run("cluster ID missing rejects checkpoint", func(t *testing.T) {
		t.Parallel()
		dataDir := t.TempDir()

		// Write a checkpoint with clusters c1 and c2
		r1, err := NewRecorder(dataDir, map[string]config.ClusterConfig{"c1": {}, "c2": {}}, 0)
		require.NoError(t, err)
		twData := map[string]types.TimeWindowData{
			"c1": {TimeWindow: types.TimeWindow{LeftBoundary: 0, RightBoundary: 10}},
			"c2": {TimeWindow: types.TimeWindow{LeftBoundary: 0, RightBoundary: 10}},
		}
		err = r1.RecordTimeWindow(twData, NewReport(0))
		require.NoError(t, err)

		// Try to load with c1 and c3 (same count, different ID) — should fail
		_, err = NewRecorder(dataDir, map[string]config.ClusterConfig{"c1": {}, "c3": {}}, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cluster info missing for cluster c3")
	})

	t.Run("matching clusters loads checkpoint successfully", func(t *testing.T) {
		t.Parallel()
		dataDir := t.TempDir()

		clusters := map[string]config.ClusterConfig{"c1": {}, "c2": {}}

		// Write checkpoint across 3 rounds so all 3 slots are filled
		r1, err := NewRecorder(dataDir, clusters, 0)
		require.NoError(t, err)
		for i := range 3 {
			twData := map[string]types.TimeWindowData{
				"c1": {TimeWindow: types.TimeWindow{LeftBoundary: uint64(i * 10), RightBoundary: uint64((i + 1) * 10)}},
				"c2": {TimeWindow: types.TimeWindow{LeftBoundary: uint64(i * 10), RightBoundary: uint64((i + 1) * 10)}},
			}
			err = r1.RecordTimeWindow(twData, NewReport(uint64(i)))
			require.NoError(t, err)
		}

		// Reload with the same clusters — should succeed
		r2, err := NewRecorder(dataDir, clusters, 0)
		require.NoError(t, err)
		cp := r2.GetCheckpoint()
		require.NotNil(t, cp.CheckpointItems[0])
		require.NotNil(t, cp.CheckpointItems[1])
		require.NotNil(t, cp.CheckpointItems[2])
	})

	t.Run("nil checkpoint items are skipped during validation", func(t *testing.T) {
		t.Parallel()
		dataDir := t.TempDir()

		// Write only 1 round — items[0] and items[1] stay nil
		r1, err := NewRecorder(dataDir, map[string]config.ClusterConfig{"c1": {}}, 0)
		require.NoError(t, err)
		twData := map[string]types.TimeWindowData{
			"c1": {TimeWindow: types.TimeWindow{LeftBoundary: 0, RightBoundary: 10}},
		}
		err = r1.RecordTimeWindow(twData, NewReport(0))
		require.NoError(t, err)

		// Reload — should succeed even with a different cluster count since
		// only the non-nil item[2] is validated
		_, err = NewRecorder(dataDir, map[string]config.ClusterConfig{"c1": {}}, 0)
		require.NoError(t, err)
	})

	t.Run("no checkpoint file skips validation", func(t *testing.T) {
		t.Parallel()
		dataDir := t.TempDir()

		// Fresh start with any cluster config — should always succeed
		r, err := NewRecorder(dataDir, map[string]config.ClusterConfig{"c1": {}, "c2": {}, "c3": {}}, 0)
		require.NoError(t, err)
		require.NotNil(t, r)

		cp := r.GetCheckpoint()
		require.Nil(t, cp.CheckpointItems[0])
		require.Nil(t, cp.CheckpointItems[1])
		require.Nil(t, cp.CheckpointItems[2])
	})
}

func TestRecorder_RecordTimeWindow(t *testing.T) {
	t.Parallel()

	t.Run("without report flush writes only checkpoint", func(t *testing.T) {
		t.Parallel()
		dataDir := t.TempDir()
		r, err := NewRecorder(dataDir, map[string]config.ClusterConfig{"c1": {}}, 0)
		require.NoError(t, err)

		twData := map[string]types.TimeWindowData{
			"c1": {TimeWindow: types.TimeWindow{LeftBoundary: 1, RightBoundary: 10}},
		}
		report := NewReport(0) // needFlush = false
		err = r.RecordTimeWindow(twData, report)
		require.NoError(t, err)

		// checkpoint.json should exist
		_, err = os.Stat(filepath.Join(dataDir, "checkpoint", "checkpoint.json"))
		require.NoError(t, err)

		// No report files
		entries, err := os.ReadDir(filepath.Join(dataDir, "report"))
		require.NoError(t, err)
		require.Empty(t, entries)
	})

	t.Run("with report flush writes both checkpoint and report", func(t *testing.T) {
		t.Parallel()
		dataDir := t.TempDir()
		r, err := NewRecorder(dataDir, map[string]config.ClusterConfig{"c1": {}}, 0)
		require.NoError(t, err)

		twData := map[string]types.TimeWindowData{
			"c1": {TimeWindow: types.TimeWindow{LeftBoundary: 1, RightBoundary: 10}},
		}
		report := NewReport(5)
		cr := NewClusterReport("c1", types.TimeWindow{LeftBoundary: 1, RightBoundary: 10})
		cr.AddDataLossItem("d1", "test_table", map[string]any{"id": "1"}, `[id: 1]`, 200)
		report.AddClusterReport("c1", cr)
		require.True(t, report.NeedFlush())

		err = r.RecordTimeWindow(twData, report)
		require.NoError(t, err)

		// checkpoint.json should exist
		_, err = os.Stat(filepath.Join(dataDir, "checkpoint", "checkpoint.json"))
		require.NoError(t, err)

		// Report files should exist
		_, err = os.Stat(filepath.Join(dataDir, "report", "report-5.report"))
		require.NoError(t, err)
		_, err = os.Stat(filepath.Join(dataDir, "report", "report-5.json"))
		require.NoError(t, err)

		// Verify report content
		reportData, err := os.ReadFile(filepath.Join(dataDir, "report", "report-5.report"))
		require.NoError(t, err)
		require.Contains(t, string(reportData), "round: 5")
		require.Contains(t, string(reportData), `[id: 1]`)

		// Verify json report is valid JSON
		jsonData, err := os.ReadFile(filepath.Join(dataDir, "report", "report-5.json"))
		require.NoError(t, err)
		var parsed Report
		err = json.Unmarshal(jsonData, &parsed)
		require.NoError(t, err)
		require.Equal(t, uint64(5), parsed.Round)
	})

	t.Run("multiple rounds advance checkpoint", func(t *testing.T) {
		t.Parallel()
		dataDir := t.TempDir()
		r, err := NewRecorder(dataDir, map[string]config.ClusterConfig{"c1": {}}, 0)
		require.NoError(t, err)

		for i := uint64(0); i < 4; i++ {
			twData := map[string]types.TimeWindowData{
				"c1": {
					TimeWindow: types.TimeWindow{LeftBoundary: i * 10, RightBoundary: (i + 1) * 10},
					MaxVersion: map[types.SchemaTableKey]types.VersionKey{
						{Schema: "db", Table: "tbl"}: {Version: i + 1},
					},
				},
			}
			report := NewReport(i)
			err = r.RecordTimeWindow(twData, report)
			require.NoError(t, err)
		}

		// After 4 rounds, checkpoint should have rounds 1, 2, 3 (oldest evicted)
		cp := r.GetCheckpoint()
		require.NotNil(t, cp.CheckpointItems[0])
		require.NotNil(t, cp.CheckpointItems[1])
		require.NotNil(t, cp.CheckpointItems[2])
		require.Equal(t, uint64(1), cp.CheckpointItems[0].Round)
		require.Equal(t, uint64(2), cp.CheckpointItems[1].Round)
		require.Equal(t, uint64(3), cp.CheckpointItems[2].Round)
	})
}

func TestRecorder_CheckpointPersistence(t *testing.T) {
	t.Parallel()

	t.Run("checkpoint survives restart", func(t *testing.T) {
		t.Parallel()
		dataDir := t.TempDir()

		stk := types.SchemaTableKey{Schema: "db", Table: "tbl"}

		// Simulate 3 rounds
		r1, err := NewRecorder(dataDir, map[string]config.ClusterConfig{"c1": {}}, 0)
		require.NoError(t, err)
		for i := uint64(0); i < 3; i++ {
			twData := map[string]types.TimeWindowData{
				"c1": {
					TimeWindow: types.TimeWindow{LeftBoundary: i * 10, RightBoundary: (i + 1) * 10},
					MaxVersion: map[types.SchemaTableKey]types.VersionKey{
						stk: {Version: i + 1, VersionPath: fmt.Sprintf("vp%d", i), DataPath: fmt.Sprintf("dp%d", i)},
					},
				},
			}
			report := NewReport(i)
			err = r1.RecordTimeWindow(twData, report)
			require.NoError(t, err)
		}

		// Restart: new recorder from same dir
		r2, err := NewRecorder(dataDir, map[string]config.ClusterConfig{"c1": {}}, 0)
		require.NoError(t, err)

		cp := r2.GetCheckpoint()
		require.Equal(t, uint64(0), cp.CheckpointItems[0].Round)
		require.Equal(t, uint64(1), cp.CheckpointItems[1].Round)
		require.Equal(t, uint64(2), cp.CheckpointItems[2].Round)

		// Verify ToScanRange works after restart
		scanRange, err := cp.ToScanRange("c1")
		require.NoError(t, err)
		require.Len(t, scanRange, 1)
		sr := scanRange[stk]
		require.Equal(t, "vp0", sr.StartVersionKey)
		require.Equal(t, "vp2", sr.EndVersionKey)
		require.Equal(t, "dp0", sr.StartDataPath)
		require.Equal(t, "dp2", sr.EndDataPath)
	})

	t.Run("old report files are cleaned up when exceeding max", func(t *testing.T) {
		t.Parallel()
		dataDir := t.TempDir()
		maxReportFiles := 3
		r, err := NewRecorder(dataDir, map[string]config.ClusterConfig{"c1": {}}, maxReportFiles)
		require.NoError(t, err)

		// Write 5 reports (each creates 2 files: .report and .json)
		for i := uint64(0); i < 5; i++ {
			twData := map[string]types.TimeWindowData{
				"c1": {TimeWindow: types.TimeWindow{LeftBoundary: i * 10, RightBoundary: (i + 1) * 10}},
			}
			report := NewReport(i)
			cr := NewClusterReport("c1", types.TimeWindow{LeftBoundary: i * 10, RightBoundary: (i + 1) * 10})
			cr.AddDataLossItem("d1", "test_table", map[string]any{"id": "1"}, `[id: 1]`, i+1)
			report.AddClusterReport("c1", cr)
			require.True(t, report.NeedFlush())

			err = r.RecordTimeWindow(twData, report)
			require.NoError(t, err)
		}

		// Should have at most maxReportFiles * 2 = 6 files (rounds 2, 3, 4)
		entries, err := os.ReadDir(filepath.Join(dataDir, "report"))
		require.NoError(t, err)
		require.Equal(t, maxReportFiles*2, len(entries))

		// Oldest files (round 0 and 1) should be deleted
		_, err = os.Stat(filepath.Join(dataDir, "report", "report-0.report"))
		require.True(t, os.IsNotExist(err))
		_, err = os.Stat(filepath.Join(dataDir, "report", "report-1.report"))
		require.True(t, os.IsNotExist(err))

		// Newest files should still exist
		_, err = os.Stat(filepath.Join(dataDir, "report", "report-2.report"))
		require.NoError(t, err)
		_, err = os.Stat(filepath.Join(dataDir, "report", "report-3.report"))
		require.NoError(t, err)
		_, err = os.Stat(filepath.Join(dataDir, "report", "report-4.report"))
		require.NoError(t, err)
	})

	t.Run("no cleanup when under max report files", func(t *testing.T) {
		t.Parallel()
		dataDir := t.TempDir()
		maxReportFiles := 10
		r, err := NewRecorder(dataDir, map[string]config.ClusterConfig{"c1": {}}, maxReportFiles)
		require.NoError(t, err)

		// Write 3 reports
		for i := uint64(0); i < 3; i++ {
			twData := map[string]types.TimeWindowData{
				"c1": {TimeWindow: types.TimeWindow{LeftBoundary: i * 10, RightBoundary: (i + 1) * 10}},
			}
			report := NewReport(i)
			cr := NewClusterReport("c1", types.TimeWindow{LeftBoundary: i * 10, RightBoundary: (i + 1) * 10})
			cr.AddDataLossItem("d1", "test_table", map[string]any{"id": "1"}, `[id: 1]`, i+1)
			report.AddClusterReport("c1", cr)

			err = r.RecordTimeWindow(twData, report)
			require.NoError(t, err)
		}

		// All 6 files should exist (3 rounds * 2 files each)
		entries, err := os.ReadDir(filepath.Join(dataDir, "report"))
		require.NoError(t, err)
		require.Equal(t, 6, len(entries))
	})

	t.Run("checkpoint json is valid", func(t *testing.T) {
		t.Parallel()
		dataDir := t.TempDir()
		r, err := NewRecorder(dataDir, map[string]config.ClusterConfig{"c1": {}}, 0)
		require.NoError(t, err)

		twData := map[string]types.TimeWindowData{
			"c1": {
				TimeWindow: types.TimeWindow{
					LeftBoundary:  100,
					RightBoundary: 200,
					CheckpointTs:  map[string]uint64{"c2": 150},
				},
			},
		}
		report := NewReport(0)
		err = r.RecordTimeWindow(twData, report)
		require.NoError(t, err)

		// Read and parse checkpoint.json
		data, err := os.ReadFile(filepath.Join(dataDir, "checkpoint", "checkpoint.json"))
		require.NoError(t, err)

		var cp Checkpoint
		err = json.Unmarshal(data, &cp)
		require.NoError(t, err)
		require.NotNil(t, cp.CheckpointItems[2])
		require.Equal(t, uint64(100), cp.CheckpointItems[2].ClusterInfo["c1"].TimeWindow.LeftBoundary)
		require.Equal(t, uint64(200), cp.CheckpointItems[2].ClusterInfo["c1"].TimeWindow.RightBoundary)
	})
}

func TestErrCheckpointCorruption(t *testing.T) {
	t.Parallel()

	t.Run("corrupted checkpoint file returns ErrCheckpointCorruption", func(t *testing.T) {
		t.Parallel()
		dataDir := t.TempDir()

		// Create report and checkpoint directories
		require.NoError(t, os.MkdirAll(filepath.Join(dataDir, "report"), 0755))
		require.NoError(t, os.MkdirAll(filepath.Join(dataDir, "checkpoint"), 0755))

		// Write invalid JSON to checkpoint.json
		err := os.WriteFile(filepath.Join(dataDir, "checkpoint", "checkpoint.json"), []byte("{bad json"), 0600)
		require.NoError(t, err)

		_, err = NewRecorder(dataDir, map[string]config.ClusterConfig{"c1": {}}, 0)
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrCheckpointCorruption))
	})

	t.Run("cluster count mismatch returns ErrCheckpointCorruption", func(t *testing.T) {
		t.Parallel()
		dataDir := t.TempDir()

		// Write a valid checkpoint with 2 clusters
		r1, err := NewRecorder(dataDir, map[string]config.ClusterConfig{"c1": {}, "c2": {}}, 0)
		require.NoError(t, err)
		twData := map[string]types.TimeWindowData{
			"c1": {TimeWindow: types.TimeWindow{LeftBoundary: 0, RightBoundary: 10}},
			"c2": {TimeWindow: types.TimeWindow{LeftBoundary: 0, RightBoundary: 10}},
		}
		err = r1.RecordTimeWindow(twData, NewReport(0))
		require.NoError(t, err)

		// Reload with 1 cluster — should be ErrCheckpointCorruption
		_, err = NewRecorder(dataDir, map[string]config.ClusterConfig{"c1": {}}, 0)
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrCheckpointCorruption))
	})

	t.Run("missing cluster ID returns ErrCheckpointCorruption", func(t *testing.T) {
		t.Parallel()
		dataDir := t.TempDir()

		// Write a valid checkpoint with clusters c1 and c2
		r1, err := NewRecorder(dataDir, map[string]config.ClusterConfig{"c1": {}, "c2": {}}, 0)
		require.NoError(t, err)
		twData := map[string]types.TimeWindowData{
			"c1": {TimeWindow: types.TimeWindow{LeftBoundary: 0, RightBoundary: 10}},
			"c2": {TimeWindow: types.TimeWindow{LeftBoundary: 0, RightBoundary: 10}},
		}
		err = r1.RecordTimeWindow(twData, NewReport(0))
		require.NoError(t, err)

		// Reload with c1 and c3 — should be ErrCheckpointCorruption
		_, err = NewRecorder(dataDir, map[string]config.ClusterConfig{"c1": {}, "c3": {}}, 0)
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrCheckpointCorruption))
	})

	t.Run("fresh start does not return ErrCheckpointCorruption", func(t *testing.T) {
		t.Parallel()
		dataDir := t.TempDir()

		_, err := NewRecorder(dataDir, map[string]config.ClusterConfig{"c1": {}}, 0)
		require.NoError(t, err)
	})

	t.Run("unreadable checkpoint file does not return ErrCheckpointCorruption", func(t *testing.T) {
		t.Parallel()
		dataDir := t.TempDir()

		// Create directories
		require.NoError(t, os.MkdirAll(filepath.Join(dataDir, "report"), 0755))
		require.NoError(t, os.MkdirAll(filepath.Join(dataDir, "checkpoint"), 0755))

		// Create checkpoint.json as a directory so ReadFile fails with a non-corruption I/O error
		require.NoError(t, os.MkdirAll(filepath.Join(dataDir, "checkpoint", "checkpoint.json"), 0755))

		_, err := NewRecorder(dataDir, map[string]config.ClusterConfig{"c1": {}}, 0)
		require.Error(t, err)
		require.False(t, errors.Is(err, ErrCheckpointCorruption),
			"I/O errors should NOT be classified as ErrCheckpointCorruption, got: %v", err)
	})

	t.Run("unreadable backup checkpoint does not return ErrCheckpointCorruption", func(t *testing.T) {
		t.Parallel()
		dataDir := t.TempDir()

		// Create directories
		require.NoError(t, os.MkdirAll(filepath.Join(dataDir, "report"), 0755))
		require.NoError(t, os.MkdirAll(filepath.Join(dataDir, "checkpoint"), 0755))

		// Make checkpoint.json.bak a directory so ReadFile fails with an I/O error
		require.NoError(t, os.MkdirAll(filepath.Join(dataDir, "checkpoint", "checkpoint.json.bak"), 0755))

		_, err := NewRecorder(dataDir, map[string]config.ClusterConfig{"c1": {}}, 0)
		require.Error(t, err)
		require.False(t, errors.Is(err, ErrCheckpointCorruption),
			"I/O errors should NOT be classified as ErrCheckpointCorruption, got: %v", err)
	})

	t.Run("corrupted backup checkpoint returns ErrCheckpointCorruption", func(t *testing.T) {
		t.Parallel()
		dataDir := t.TempDir()

		// Create directories
		require.NoError(t, os.MkdirAll(filepath.Join(dataDir, "report"), 0755))
		require.NoError(t, os.MkdirAll(filepath.Join(dataDir, "checkpoint"), 0755))

		// Write invalid JSON to checkpoint.json.bak (simulate crash recovery with corrupted backup)
		err := os.WriteFile(filepath.Join(dataDir, "checkpoint", "checkpoint.json.bak"), []byte("not valid json"), 0600)
		require.NoError(t, err)

		_, err = NewRecorder(dataDir, map[string]config.ClusterConfig{"c1": {}}, 0)
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrCheckpointCorruption))
	})
}
