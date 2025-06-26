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
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/tikv/client-go/v2/tikv"
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

	regionCache RegionCache
}

func NewDefaultSpanSplitChecker(changefeedID common.ChangeFeedID, schedulerCfg *config.ChangefeedSchedulerConfig) *defaultSpanSplitChecker {
	if schedulerCfg == nil {
		log.Panic("scheduler config is nil, please check the config", zap.String("changefeed", changefeedID.Name()))
	}
	regionCache := appcontext.GetService[RegionCache](appcontext.RegionCache)
	return &defaultSpanSplitChecker{
		changefeedID:    changefeedID,
		writeThreshold:  schedulerCfg.WriteKeyThreshold,
		regionThreshold: schedulerCfg.RegionThreshold,
		regionCache:     regionCache,
	}
}

// spanSplitStatus tracks the split status of a span in the default group
type spanSplitStatus struct {
	*SpanReplication
	trafficScore int
	regionCount  int
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
		regionCount:     0,
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
		if status.GetStatus().EventSizePerSecond < float32(s.writeThreshold) {
			status.trafficScore = 0
		} else {
			status.trafficScore++
		}
	}

	// check region count, because the change of region count is not frequent, so we can check less frequently
	if time.Since(status.regionCheckTime) > regionCheckInterval {
		regions, err := s.regionCache.LoadRegionsInKeyRange(tikv.NewBackoffer(context.Background(), 500), status.Span.StartKey, status.Span.EndKey)
		if err != nil {
			log.Warn("list regions failed, skip check region count", zap.String("changefeed", s.changefeedID.Name()), zap.String("span", status.Span.String()), zap.Error(err))
		} else {
			status.regionCount = len(regions)
		}
		status.regionCheckTime = time.Now()
	}

	if status.trafficScore >= trafficScoreThreshold || status.regionCount >= s.regionThreshold {
		if _, ok := s.splitReadyTasks[status.ID]; !ok {
			s.splitReadyTasks[status.ID] = status
		}
	} else {
		if _, ok := s.splitReadyTasks[status.ID]; ok {
			delete(s.splitReadyTasks, status.ID)
		}
	}
}

const (
	SplitTypeTraffic = iota
	SplitTypeRegion
)

type DefaultSpanSplitCheckResult struct {
	SplitType int
	Span      *SpanReplication
}

func (s *defaultSpanSplitChecker) Check(batch int) replica.GroupCheckResult {
	results := make([]DefaultSpanSplitCheckResult, 0, batch)
	for _, status := range s.splitReadyTasks {
		// We prefer do traffic split when both traffic score and region count are high
		if status.trafficScore > trafficScoreThreshold {
			results = append(results, DefaultSpanSplitCheckResult{
				SplitType: SplitTypeTraffic,
				Span:      status.SpanReplication,
			})
		} else if status.regionCount > s.regionThreshold {
			results = append(results, DefaultSpanSplitCheckResult{
				SplitType: SplitTypeRegion,
				Span:      status.SpanReplication,
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

// TODO:it's a temp way to avoid import cycle, remove it after refactor the region cache
// RegionCache is a simplified interface of tikv.RegionCache.
// It is useful to restrict RegionCache usage and mocking in tests.
type RegionCache interface {
	// LoadRegionsInKeyRange loads regions in [startKey,endKey].
	LoadRegionsInKeyRange(
		bo *tikv.Backoffer, startKey, endKey []byte,
	) (regions []*tikv.Region, err error)
}
