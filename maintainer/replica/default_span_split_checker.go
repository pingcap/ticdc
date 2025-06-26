// Copyright 2025 PingCAP, Inc.
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

package replica

import (
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
	"go.uber.org/zap"
)

const (
	trafficScoreThreshold = 3
	regionScoreThreshold  = 3
	regionCheckInterval   = time.Second * 10
)

// defaultSpanSplitChecker is used to check whether spans in the default group need to be split
// based on multiple thresholds including write traffic and region count.
//
// This checker monitors spans in the default group (GroupDefault) and determines if they should
// be split into multiple subspans when:
//  1. The write traffic (EventSizePerSecond) exceeds the configured threshold
//  2. The number of regions within the span exceeds the configured threshold
//
// The checker uses a scoring mechanism to avoid frequent split operations caused by temporary
// traffic spikes. A span will only be marked for splitting after maintaining high traffic/region
// count for a certain number of consecutive checks.
type defaultSpanSplitChecker struct {
	changefeedID common.ChangeFeedID

	// allTasks tracks all spans in the default group for monitoring
	allTasks map[common.DispatcherID]*spanSplitStatus

	splitReadyTasks map[common.DispatcherID]*spanSplitStatus

	// writeThreshold defines the traffic threshold for triggering split consideration
	writeThreshold float32

	// regionThreshold defines the maximum number of regions allowed before split
	regionThreshold int

	// scoreThreshold defines how many consecutive checks above threshold are needed
	scoreThreshold int

	// clearTimeout defines how long to keep tracking a span after it goes below threshold
	clearTimeout time.Duration
}

// spanSplitStatus tracks the split status of a span in the default group
type spanSplitStatus struct {
	*SpanReplication
	trafficScore int
	regionScore  int
	// lastUpdateTime records when the status was last updated
	lastUpdateTime  time.Time
	regionCheckTime time.Time
}

func (s *defaultSpanSplitChecker) Name() string {
	return "default span split checker"
}

func (s *defaultSpanSplitChecker) AddReplica(replica *SpanReplication) {
	if _, ok := s.allTasks[replica.ID]; ok {
		return
	}
	s.allTasks[replica.ID] = &spanSplitStatus{
		SpanReplication: replica,
		trafficScore:    0,
		regionScore:     0,
		lastUpdateTime:  time.Now(),
	}
}

func (s *defaultSpanSplitChecker) RemoveReplica(replica *SpanReplication) {
	delete(s.allTasks, replica.ID)
}

func (s *defaultSpanSplitChecker) UpdateStatus(replica *SpanReplication) {
	status, ok := s.allTasks[replica.ID]
	if !ok {
		log.Panic("default span split checker: replica not found", zap.String("changefeed", s.changefeedID.Name()), zap.String("replica", replica.ID.String()))
	}
	if status.GetStatus().ComponentStatus != heartbeatpb.ComponentState_Working {
		return
	}

	// check traffic first
	if status.GetStatus().EventSizePerSecond != 0 {
		if status.GetStatus().EventSizePerSecond < s.writeThreshold {
			status.trafficScore = 0
		} else {
			status.trafficScore++
		}
	}

	// check region count, because the change of region count is not frequent, so we can check less frequently
	if time.Since(status.regionCheckTime) > regionCheckInterval {
		regions, err := s.regionCache.ListRegionIDsInKeyRange(bo, status.Span.StartKey, status.Span.EndKey)
		if err != nil {
			log.Warn("list regions failed, skip split span", zap.String("changefeed", s.changefeedID.Name()), zap.String("span", status.Span.String()), zap.Error(err))
		}
		status.regionCheckTime = time.Now()
	}
}

func (s *defaultSpanSplitChecker) Check(batch int) replica.GroupCheckResult {
	return nil
}

func (s *defaultSpanSplitChecker) Stat() string {
	return ""
}
