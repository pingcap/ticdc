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
	"fmt"
	"os"
	"path"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/advancer"
	"github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

type Recorder struct {
	recordDir string
}

func NewRecorder(reportDir string) (*Recorder, error) {
	err := os.MkdirAll(reportDir, 0755)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Recorder{
		recordDir: reportDir,
	}, nil
}

func (r *Recorder) RecordTimeWindow(timeWindowData map[string]advancer.TimeWindowData, report *Report) error {
	for clusterID, timeWindow := range timeWindowData {
		log.Info("time window advanced",
			zap.Uint64("round", report.Round),
			zap.String("clusterID", clusterID),
			zap.Uint64("window left boundary", timeWindow.LeftBoundary),
			zap.Uint64("window right boundary", timeWindow.RightBoundary),
			zap.Any("checkpoint ts", timeWindow.CheckpointTs))
	}
	if err := r.flushReport(report); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (r *Recorder) flushReport(report *Report) error {
	filename := path.Join(r.recordDir, fmt.Sprintf("report-%d.log", report.Round))
	data := report.MarshalReport()
	return os.WriteFile(filename, []byte(data), 0600)
}
