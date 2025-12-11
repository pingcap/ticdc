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
	"context"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/split"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

var (
	trafficScoreThreshold = 3
	regionScoreThreshold  = 3
	regionCheckInterval   = time.Second * 120
)

// defaultSpanSplitChecker is used to check whether spans in the default group need to be split
// based on multiple thresholds including write traffic and region count.
// we only track the spans who is enabled to split(in mysqlSink with pk but no uk, or other sink).
//
// This checker monitors spans in the default group (GroupDefault) and determines if they should
// be split into multiple subspans when:
//  1. The write traffic (EventSizePerSecond) exceeds the configured threshold
//  2. The number of regions within the span exceeds the configured threshold
//
// The checker uses a scoring mechanism to avoid frequent split operations caused by temporary
// traffic spikes. A span will only be marked for splitting after maintaining high traffic/region
// count for a certain number of consecutive checks.

// Notice: all methods are NOT thread-safe.(TODO: consider to make thread safe)
type defaultSpanSplitChecker struct {
	changefeedID common.ChangeFeedID

	// allTasks tracks all spans in the default group for monitoring
	allTasks map[common.DispatcherID]*spanSplitStatus

	splitReadyTasks map[common.DispatcherID]*spanSplitStatus

	// writeThreshold defines the traffic threshold for triggering split consideration
	writeThreshold int

	// regionThreshold defines the maximum number of regions allowed before split
	regionThreshold int

	regionCache split.RegionCache
}

func NewDefaultSpanSplitChecker(changefeedID common.ChangeFeedID, schedulerCfg *config.ChangefeedSchedulerConfig) *defaultSpanSplitChecker {
	if schedulerCfg == nil {
		log.Panic("scheduler config is nil, please check the config", zap.String("changefeed", changefeedID.Name()))
	}
	regionCache := appcontext.GetService[split.RegionCache](appcontext.RegionCache)
	return &defaultSpanSplitChecker{
		changefeedID:    changefeedID,
		allTasks:        make(map[common.DispatcherID]*spanSplitStatus),
		splitReadyTasks: make(map[common.DispatcherID]*spanSplitStatus),
		writeThreshold:  util.GetOrZero(schedulerCfg.WriteKeyThreshold),
		regionThreshold: util.GetOrZero(schedulerCfg.RegionThreshold),
		regionCache:     regionCache,
	}
}

// spanSplitStatus tracks the split status of a span in the default group
type spanSplitStatus struct {
	*SpanReplication
	trafficScore    int
	latestTraffic   float64
	regionCount     int
	regionCheckTime time.Time
}

func (s *defaultSpanSplitChecker) Name() string {
	return "default span split checker"
}

func (s *defaultSpanSplitChecker) AddReplica(replica *SpanReplication) {
	if _, ok := s.allTasks[replica.ID]; ok {
		return
	}
	if !replica.enabledSplit {
		log.Debug("default span split checker: replica not enabled to split, skip add", zap.Stringer("changefeed", s.changefeedID), zap.String("replica", replica.ID.String()))
		return
	}
	s.allTasks[replica.ID] = &spanSplitStatus{
		SpanReplication: replica,
		trafficScore:    0,
		regionCount:     0,
		latestTraffic:   0,
		regionCheckTime: time.Now().Add(-regionCheckInterval),
	}
}

func (s *defaultSpanSplitChecker) RemoveReplica(replica *SpanReplication) {
	delete(s.allTasks, replica.ID)
	delete(s.splitReadyTasks, replica.ID)
}

func (s *defaultSpanSplitChecker) UpdateStatus(replica *SpanReplication) {
	status, ok := s.allTasks[replica.ID]
	if !ok {
		log.Warn("default span split checker: replica not found", zap.String("changefeed", s.changefeedID.Name()), zap.String("replica", replica.ID.String()))
		return
	}
	if status.GetStatus().ComponentStatus != heartbeatpb.ComponentState_Working {
		return
	}

	// check traffic first
	if status.GetStatus().EventSizePerSecond != 0 {
		if s.writeThreshold == 0 || status.GetStatus().EventSizePerSecond < float32(s.writeThreshold) {
			status.trafficScore = 0
		} else {
			status.trafficScore++
			status.latestTraffic = float64(status.GetStatus().EventSizePerSecond)
		}
	}

	// check region count, because the change of region count is not frequent, so we can check less frequently
	if time.Since(status.regionCheckTime) > regionCheckInterval {
		regions, err := s.regionCache.LoadRegionsInKeyRange(tikv.NewBackoffer(context.Background(), 500), status.Span.StartKey, status.Span.EndKey)
		if err != nil {
			log.Warn("list regions failed, skip check region count",
				zap.Stringer("changefeed", s.changefeedID),
				zap.String("span", common.FormatTableSpan(status.Span)), zap.Error(err))
		} else {
			status.regionCount = len(regions)
		}
		status.regionCheckTime = time.Now()
	}

	log.Debug("default span split checker: update status", zap.Stringer("changefeed", s.changefeedID), zap.String("replica", replica.ID.String()), zap.Int("trafficScore", status.trafficScore), zap.Int("regionCount", status.regionCount))

	if status.trafficScore >= trafficScoreThreshold || (s.regionThreshold > 0 && status.regionCount >= s.regionThreshold) {
		if _, ok := s.splitReadyTasks[status.ID]; !ok {
			s.splitReadyTasks[status.ID] = status
		}
	} else {
		delete(s.splitReadyTasks, status.ID)
	}
}

type DefaultSpanSplitCheckResult struct {
	Span     *SpanReplication
	SpanNum  int
	SpanType split.SplitType
}

func (s *defaultSpanSplitChecker) Check(batch int) replica.GroupCheckResult {
	results := make([]DefaultSpanSplitCheckResult, 0, batch)
	for _, status := range s.splitReadyTasks {
		// for default span to do split, we use splitByTraffic to make the split more balanced
		if status.trafficScore >= trafficScoreThreshold || status.regionCount >= s.regionThreshold {
			var spanNum int
			var spanType split.SplitType
			if status.trafficScore >= trafficScoreThreshold {
				spanNum = int(math.Ceil(status.latestTraffic / float64(s.writeThreshold)))
				if status.regionCount < split.MaxRegionCountForWriteBytesSplit {
					spanType = split.SplitTypeWriteBytes
				} else {
					spanType = split.SplitTypeRegionCount
				}
			} else {
				spanNum = int(math.Ceil(float64(status.regionCount) / float64(s.regionThreshold)))
				spanType = split.SplitTypeRegionCount
			}

			results = append(results, DefaultSpanSplitCheckResult{
				Span:     status.SpanReplication,
				SpanNum:  spanNum * 2,
				SpanType: spanType,
			})
		}
		if len(results) >= batch {
			break
		}
	}
	return results
}

// stat shows the split ready tasks's dispatcherID, traffic score and region count
func (s *defaultSpanSplitChecker) Stat() string {
	var res strings.Builder
	for _, status := range s.splitReadyTasks {
		res.WriteString("[dispatcherID: ")
		res.WriteString(status.ID.String())
		res.WriteString(" trafficScore: ")
		res.WriteString(strconv.Itoa(status.trafficScore))
		res.WriteString(" regionCount: ")
		res.WriteString(strconv.Itoa(status.regionCount))
		res.WriteString("];")
	}
	return res.String()
}
