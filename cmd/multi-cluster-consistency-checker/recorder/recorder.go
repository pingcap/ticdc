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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/config"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/types"
	"github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

type Recorder struct {
	reportDir     string
	checkpointDir string

	checkpoint *Checkpoint
}

func NewRecorder(dataDir string, clusters map[string]config.ClusterConfig) (*Recorder, error) {
	if err := os.MkdirAll(filepath.Join(dataDir, "report"), 0755); err != nil {
		return nil, errors.Trace(err)
	}
	if err := os.MkdirAll(filepath.Join(dataDir, "checkpoint"), 0755); err != nil {
		return nil, errors.Trace(err)
	}
	r := &Recorder{
		reportDir:     filepath.Join(dataDir, "report"),
		checkpointDir: filepath.Join(dataDir, "checkpoint"),

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
			return nil, errors.Errorf("checkpoint item (round %d) cluster info length mismatch, expected %d, got %d", item.Round, len(clusters), len(item.ClusterInfo))
		}
		for clusterID := range clusters {
			if _, ok := item.ClusterInfo[clusterID]; !ok {
				return nil, errors.Errorf("checkpoint item (round %d) cluster info missing for cluster %s", item.Round, clusterID)
			}
		}
	}

	return r, nil
}

func (r *Recorder) GetCheckpoint() *Checkpoint {
	return r.checkpoint
}

func (r *Recorder) initializeCheckpoint() error {
	_, err := os.Stat(filepath.Join(r.checkpointDir, "checkpoint.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return errors.Trace(err)
	}
	data, err := os.ReadFile(filepath.Join(r.checkpointDir, "checkpoint.json"))
	if err != nil {
		return errors.Trace(err)
	}
	if err := json.Unmarshal(data, r.checkpoint); err != nil {
		return errors.Trace(err)
	}

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
	}
	if err := r.flushCheckpoint(report.Round, timeWindowData); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (r *Recorder) flushReport(report *Report) error {
	filename := filepath.Join(r.reportDir, fmt.Sprintf("report-%d.report", report.Round))
	data := report.MarshalReport()
	if err := os.WriteFile(filename, []byte(data), 0600); err != nil {
		return errors.Trace(err)
	}
	filename = filepath.Join(r.reportDir, fmt.Sprintf("report-%d.json", report.Round))
	dataBytes, err := json.Marshal(report)
	if err != nil {
		return errors.Trace(err)
	}
	if err := os.WriteFile(filename, dataBytes, 0600); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (r *Recorder) flushCheckpoint(round uint64, timeWindowData map[string]types.TimeWindowData) error {
	r.checkpoint.NewTimeWindowData(round, timeWindowData)
	filename := filepath.Join(r.checkpointDir, "checkpoint.json")
	data, err := json.Marshal(r.checkpoint)
	if err != nil {
		return errors.Trace(err)
	}
	if err := os.WriteFile(filename, data, 0600); err != nil {
		return errors.Trace(err)
	}
	return nil
}
