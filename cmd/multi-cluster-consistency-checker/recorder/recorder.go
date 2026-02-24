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
	"sort"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/config"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/types"
	"github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

// ErrCheckpointCorruption is a sentinel error indicating that the persisted
// checkpoint data is corrupted and requires manual intervention to fix.
var ErrCheckpointCorruption = errors.New("checkpoint corruption")

type Recorder struct {
	reportDir      string
	checkpointDir  string
	maxReportFiles int

	// reportFiles caches the sorted list of report file names in reportDir.
	// Updated in-memory after flush and cleanup to avoid repeated os.ReadDir calls.
	reportFiles []string

	checkpoint *Checkpoint
}

func NewRecorder(dataDir string, clusters map[string]config.ClusterConfig, maxReportFiles int) (*Recorder, error) {
	if err := os.MkdirAll(filepath.Join(dataDir, "report"), 0755); err != nil {
		return nil, errors.Trace(err)
	}
	if err := os.MkdirAll(filepath.Join(dataDir, "checkpoint"), 0755); err != nil {
		return nil, errors.Trace(err)
	}
	if maxReportFiles <= 0 {
		maxReportFiles = config.DefaultMaxReportFiles
	}

	// Read existing report files once at startup
	entries, err := os.ReadDir(filepath.Join(dataDir, "report"))
	if err != nil {
		return nil, errors.Trace(err)
	}
	reportFiles := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			reportFiles = append(reportFiles, entry.Name())
		}
	}
	sort.Strings(reportFiles)

	r := &Recorder{
		reportDir:      filepath.Join(dataDir, "report"),
		checkpointDir:  filepath.Join(dataDir, "checkpoint"),
		maxReportFiles: maxReportFiles,
		reportFiles:    reportFiles,

		checkpoint: NewCheckpoint(),
	}
	if err := r.initializeCheckpoint(); err != nil {
		return nil, errors.Trace(err)
	}
	for _, item := range r.checkpoint.CheckpointItems {
		if item == nil {
			continue
		}
		if len(item.ClusterInfo) != len(clusters) {
			return nil, errors.Annotatef(ErrCheckpointCorruption, "checkpoint item (round %d) cluster info length mismatch, expected %d, got %d", item.Round, len(clusters), len(item.ClusterInfo))
		}
		for clusterID := range clusters {
			if _, ok := item.ClusterInfo[clusterID]; !ok {
				return nil, errors.Annotatef(ErrCheckpointCorruption, "checkpoint item (round %d) cluster info missing for cluster %s", item.Round, clusterID)
			}
		}
	}

	return r, nil
}

func (r *Recorder) GetCheckpoint() *Checkpoint {
	return r.checkpoint
}

func (r *Recorder) initializeCheckpoint() error {
	checkpointFile := filepath.Join(r.checkpointDir, "checkpoint.json")
	bakFile := filepath.Join(r.checkpointDir, "checkpoint.json.bak")

	// If checkpoint.json exists, use it directly.
	if _, err := os.Stat(checkpointFile); err == nil {
		data, err := os.ReadFile(checkpointFile)
		if err != nil {
			return errors.Trace(err) // transient I/O error
		}
		if err := json.Unmarshal(data, r.checkpoint); err != nil {
			return errors.Annotatef(ErrCheckpointCorruption, "failed to unmarshal checkpoint.json: %v", err)
		}
		return nil
	}

	// checkpoint.json is missing — try recovering from the backup.
	// This can happen when the process crashed after rename but before the
	// new temp file was renamed into place.
	if _, err := os.Stat(bakFile); err == nil {
		log.Warn("checkpoint.json not found, recovering from checkpoint.json.bak")
		data, err := os.ReadFile(bakFile)
		if err != nil {
			return errors.Trace(err) // transient I/O error
		}
		if err := json.Unmarshal(data, r.checkpoint); err != nil {
			return errors.Annotatef(ErrCheckpointCorruption, "failed to unmarshal checkpoint.json.bak: %v", err)
		}
		// Restore the backup as the primary file
		if err := os.Rename(bakFile, checkpointFile); err != nil {
			return errors.Trace(err) // transient I/O error
		}
		return nil
	}

	// Neither file exists — fresh start.
	return nil
}

func (r *Recorder) RecordTimeWindow(timeWindowData map[string]types.TimeWindowData, report *Report) error {
	for clusterID, timeWindow := range timeWindowData {
		log.Info("time window advanced",
			zap.Uint64("round", report.Round),
			zap.String("clusterID", clusterID),
			zap.Uint64("window left boundary", timeWindow.LeftBoundary),
			zap.Uint64("window right boundary", timeWindow.RightBoundary),
			zap.Any("checkpoint ts", timeWindow.CheckpointTs))
	}
	if report.NeedFlush() {
		if err := r.flushReport(report); err != nil {
			return errors.Trace(err)
		}
		r.cleanupOldReports()
	}
	if err := r.flushCheckpoint(report.Round, timeWindowData); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (r *Recorder) flushReport(report *Report) error {
	reportName := fmt.Sprintf("report-%d.report", report.Round)
	if err := atomicWriteFile(filepath.Join(r.reportDir, reportName), []byte(report.MarshalReport())); err != nil {
		return errors.Trace(err)
	}

	jsonName := fmt.Sprintf("report-%d.json", report.Round)
	dataBytes, err := json.Marshal(report)
	if err != nil {
		return errors.Trace(err)
	}
	if err := atomicWriteFile(filepath.Join(r.reportDir, jsonName), dataBytes); err != nil {
		return errors.Trace(err)
	}

	// Append new file names to the cache (they are always the latest, so append at end)
	r.reportFiles = append(r.reportFiles, reportName, jsonName)
	return nil
}

// atomicWriteFile writes data to a temporary file, fsyncs it to ensure
// durability, and then atomically renames it to the target path.
// This prevents partial / corrupt files on crash.
func atomicWriteFile(targetPath string, data []byte) error {
	tempPath := targetPath + ".tmp"
	if err := syncWriteFile(tempPath, data); err != nil {
		return errors.Trace(err)
	}
	if err := os.Rename(tempPath, targetPath); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// syncWriteFile writes data to a file and fsyncs it before returning,
// guaranteeing that the content is durable on disk.
func syncWriteFile(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return errors.Trace(err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return errors.Trace(err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return errors.Trace(err)
	}
	return errors.Trace(f.Close())
}

// cleanupOldReports removes the oldest report files from the in-memory cache
// when the total number exceeds maxReportFiles * 2 (each round produces .report + .json).
func (r *Recorder) cleanupOldReports() {
	if len(r.reportFiles) <= r.maxReportFiles*2 {
		return
	}

	toDelete := len(r.reportFiles) - r.maxReportFiles*2
	for i := 0; i < toDelete; i++ {
		path := filepath.Join(r.reportDir, r.reportFiles[i])
		if err := os.Remove(path); err != nil {
			log.Warn("failed to remove old report file",
				zap.String("path", path),
				zap.Error(err))
		} else {
			log.Info("removed old report file", zap.String("path", path))
		}
	}
	r.reportFiles = r.reportFiles[toDelete:]
}

func (r *Recorder) flushCheckpoint(round uint64, timeWindowData map[string]types.TimeWindowData) error {
	r.checkpoint.NewTimeWindowData(round, timeWindowData)

	checkpointFile := filepath.Join(r.checkpointDir, "checkpoint.json")
	bakFile := filepath.Join(r.checkpointDir, "checkpoint.json.bak")
	tempFile := filepath.Join(r.checkpointDir, "checkpoint_temp.json")

	data, err := json.Marshal(r.checkpoint)
	if err != nil {
		return errors.Trace(err)
	}

	// 1. Write the new content to a temp file first and fsync it.
	if err := syncWriteFile(tempFile, data); err != nil {
		return errors.Trace(err)
	}

	// 2. Rename the existing checkpoint to .bak (ignore error if it doesn't exist yet).
	if err := os.Rename(checkpointFile, bakFile); err != nil && !os.IsNotExist(err) {
		return errors.Trace(err)
	}

	// 3. Rename the temp file to be the new checkpoint.
	if err := os.Rename(tempFile, checkpointFile); err != nil {
		return errors.Trace(err)
	}

	// 4. Remove the backup — no longer needed.
	_ = os.Remove(bakFile)

	return nil
}
