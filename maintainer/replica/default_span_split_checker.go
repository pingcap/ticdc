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
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/split"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

var (
	trafficScoreThreshold = 3
	regionScoreThreshold  = 3
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

	mu sync.RWMutex

	// writeThreshold defines the traffic threshold for triggering split consideration
	writeThreshold int

	// regionThreshold defines the maximum number of regions allowed before split
	regionThreshold int

	refreshInterval time.Duration
	regionCache     split.RegionCache

	cancel context.CancelFunc
}

func NewDefaultSpanSplitChecker(changefeedID common.ChangeFeedID, schedulerCfg *config.ChangefeedSchedulerConfig) *defaultSpanSplitChecker {
	if schedulerCfg == nil {
		log.Panic("scheduler config is nil, please check the config", zap.String("changefeed", changefeedID.Name()))
	}
	checker := &defaultSpanSplitChecker{
		changefeedID:    changefeedID,
		allTasks:        make(map[common.DispatcherID]*spanSplitStatus),
		splitReadyTasks: make(map[common.DispatcherID]*spanSplitStatus),
		writeThreshold:  util.GetOrZero(schedulerCfg.WriteKeyThreshold),
		regionThreshold: util.GetOrZero(schedulerCfg.RegionThreshold),
		refreshInterval: util.GetOrZero(schedulerCfg.RegionCountRefreshInterval),
		regionCache:     appcontext.GetService[split.RegionCache](appcontext.RegionCache),
	}
	if checker.regionThreshold > 0 {
		ctx, cancel := context.WithCancel(context.Background())
		checker.cancel = cancel
		go checker.runRegionCountRefresh(ctx)
	}
	return checker
}

// spanSplitStatus tracks the split status of a span in the default group
type spanSplitStatus struct {
	*SpanReplication
	trafficScore  int
	latestTraffic float64
	regionCount   int
}

func (s *defaultSpanSplitChecker) Name() string {
	return "default span split checker"
}

func (s *defaultSpanSplitChecker) AddReplica(replica *SpanReplication) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	}
}

func (s *defaultSpanSplitChecker) RemoveReplica(replica *SpanReplication) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.allTasks, replica.ID)
	delete(s.splitReadyTasks, replica.ID)
}

func (s *defaultSpanSplitChecker) UpdateStatus(replica *SpanReplication) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	s.mu.RLock()
	defer s.mu.RUnlock()
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
	s.mu.RLock()
	defer s.mu.RUnlock()
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

func (s *defaultSpanSplitChecker) runRegionCountRefresh(ctx context.Context) {
	ticker := time.NewTicker(s.refreshInterval)
	defer ticker.Stop()

	s.refreshRegionCounts()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.refreshRegionCounts()
		}
	}
}

func (s *defaultSpanSplitChecker) refreshRegionCounts() {
	jobs := s.collectRegionCountJobs()
	runRegionCountJobs(s.regionCache, jobs, s.applyRegionCountResult)
}

func (s *defaultSpanSplitChecker) collectRegionCountJobs() []regionCountJob {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.allTasks) == 0 {
		return nil
	}
	jobs := make([]regionCountJob, 0, len(s.allTasks))
	for id, status := range s.allTasks {
		jobs = append(jobs, regionCountJob{
			id:   id,
			span: status.Span,
		})
	}
	return jobs
}

func (s *defaultSpanSplitChecker) applyRegionCountResult(job regionCountJob, count int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	status, ok := s.allTasks[job.id]
	if !ok {
		return
	}
	if err != nil {
		log.Warn("list regions failed, skip check region count",
			zap.Stringer("changefeed", s.changefeedID),
			zap.String("span", common.FormatTableSpan(status.Span)),
			zap.Error(err))
		return
	}
	status.regionCount = count
}

func (s *defaultSpanSplitChecker) Close() {
	if s.cancel != nil {
		s.cancel()
	}
}
