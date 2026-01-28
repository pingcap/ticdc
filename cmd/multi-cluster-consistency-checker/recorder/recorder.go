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
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/advancer"
	"go.uber.org/zap"
)

type Recorder struct {
	round uint64
}

func NewRecorder() *Recorder {
	return &Recorder{
		round: 0,
	}
}

func (r *Recorder) RecordTimeWindow(timeWindowData map[string]advancer.TimeWindowData) {
	for clusterID, timeWindow := range timeWindowData {
		log.Info("time window advanced",
			zap.Uint64("round", r.round),
			zap.String("clusterID", clusterID),
			zap.Uint64("window left boundary", timeWindow.LeftBoundary),
			zap.Uint64("window right boundary", timeWindow.RightBoundary),
			zap.Any("checkpoint ts", timeWindow.CheckpointTs))
	}
	r.round += 1
}
