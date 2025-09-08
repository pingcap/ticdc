// Copyright 2024 PingCAP, Inc.
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

package split

import (
	"bytes"
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

// maxSpanCount is the maximum number of spans that can be created when splitting a table
// based on regionCountPerSpan. If splitting by regionCountPerSpan would result in more
// than maxSpanCount spans, the splitting will be limited to maxSpanCount spans instead.
const maxSpanCount = 1000

// regionCountSplitter is a splitter that splits spans by region count.
// regionCountSplitter has two modes:
// 1. if spansNum > 0, means we split the span to spansNum spans
// 2. if spansNum == 0, means we split the span as each span contains at most regionCountPerSpan regions.
type regionCountSplitter struct {
	changefeedID       common.ChangeFeedID
	regionCache        RegionCache
	regionThreshold    int
	regionCountPerSpan int // the max number of regions in each span, which is set by configuration
}

func newRegionCountSplitter(
	changefeedID common.ChangeFeedID, regionCountPerSpan int, regionThreshold int,
) *regionCountSplitter {
	regionCache := appcontext.GetService[RegionCache](appcontext.RegionCache)
	return &regionCountSplitter{
		changefeedID:       changefeedID,
		regionCache:        regionCache,
		regionCountPerSpan: regionCountPerSpan,
		regionThreshold:    regionThreshold,
	}
}

// If spansNum > 0, means we split the span to spansNum spans
// In this split, we don't need to ensure the region count is larger than regionThreshold.
//
// If spansNum == 0, means we split the span to regionNum / regionCountPerSpan spans.
// In this split, we need to ensure the region count is larger than regionThreshold.
// Otherwise, we don't need to split the span.
func (m *regionCountSplitter) split(
	ctx context.Context, span *heartbeatpb.TableSpan, spansNum int,
) []*heartbeatpb.TableSpan {
	startTimestamp := time.Now()
	bo := tikv.NewBackoffer(ctx, 2000)
	regions, err := m.regionCache.LoadRegionsInKeyRange(bo, span.StartKey, span.EndKey)
	if err != nil {
		log.Warn("load regions failed, skip split span",
			zap.String("changefeed", m.changefeedID.Name()),
			zap.String("span", span.String()),
			zap.Error(err))
		return []*heartbeatpb.TableSpan{span}
	}

	if spansNum == 0 && (m.regionThreshold == 0 || len(regions) <= m.regionThreshold) {
		log.Info("skip split span because region count is less than region threshold or region threshold is 0",
			zap.String("changefeed", m.changefeedID.Name()),
			zap.String("span", span.String()),
			zap.Int("regionCount", len(regions)),
			zap.Int("regionThreshold", m.regionThreshold))
		return []*heartbeatpb.TableSpan{span}
	}
	if spansNum > 0 && len(regions) < spansNum {
		log.Info("skip split span because region count is less than target spans num",
			zap.String("changefeed", m.changefeedID.Name()),
			zap.String("span", span.String()),
			zap.Int("regionCount", len(regions)),
			zap.Int("targetSpansNum", spansNum))
		return []*heartbeatpb.TableSpan{span}
	}

	stepper := newEvenlySplitStepper(len(regions), m.regionCountPerSpan, spansNum)

	spans := make([]*heartbeatpb.TableSpan, 0, stepper.SpanCount())
	start, end := 0, stepper.Step()
	for {
		startKey := regions[start].StartKey()
		endKey := regions[end-1].EndKey()
		if len(spans) > 0 &&
			bytes.Compare(spans[len(spans)-1].EndKey, startKey) > 0 {
			log.Warn("schedulerv3: list region out of order detected",
				zap.String("keyspace", m.changefeedID.Keyspace()),
				zap.String("changefeed", m.changefeedID.Name()),
				zap.String("span", span.String()),
				zap.Stringer("lastSpan", spans[len(spans)-1]),
				zap.Any("startKey", startKey),
				zap.Any("endKey", endKey))
			return []*heartbeatpb.TableSpan{span}
		}
		spans = append(spans, &heartbeatpb.TableSpan{
			TableID:  span.TableID,
			StartKey: startKey,
			EndKey:   endKey,
		},
		)

		if end == len(regions) {
			break
		}
		start = end
		step := stepper.Step()
		if end+step <= len(regions) {
			end = end + step
		} else {
			// should not happen
			log.Panic("Unexpected stepper step", zap.Any("end", end), zap.Any("step", step), zap.Any("lenOfRegions", len(regions)))
		}
	}
	// Make sure spans does not exceed [startKey, endKey).
	spans[0].StartKey = span.StartKey
	spans[len(spans)-1].EndKey = span.EndKey
	log.Info("split span by region count",
		zap.String("changefeed", m.changefeedID.Name()),
		zap.String("span", span.String()),
		zap.Int("spans", len(spans)),
		zap.Int("regionCount", len(regions)),
		zap.Int("regionCountPerSpan", m.regionCountPerSpan),
		zap.Int("spansNum", spansNum),
		zap.Duration("splitTime", time.Since(startTimestamp)))
	return spans
}

type evenlySplitStepper struct {
	spanCount     int
	regionPerSpan int
	remain        int // the number of spans that have the regionPerSpan + 1 region count
}

func newEvenlySplitStepper(totalRegion int, maxRegionPerSpan int, spansNum int) evenlySplitStepper {
	// split based on the spansNum
	if spansNum > 0 {
		return evenlySplitStepper{
			regionPerSpan: totalRegion / spansNum,
			spanCount:     spansNum,
			remain:        totalRegion % spansNum,
		}
	}

	// if splitted by maxRegionPerSpan would result in more than maxSpanCount spans,
	// the splitting will be limited to maxSpanCount spans instead.
	if totalRegion/maxRegionPerSpan > maxSpanCount {
		return evenlySplitStepper{
			regionPerSpan: totalRegion / maxSpanCount,
			spanCount:     maxSpanCount,
			remain:        totalRegion % maxSpanCount,
		}
	}

	// split based on the maxRegionPerSpan
	if totalRegion%maxRegionPerSpan == 0 {
		return evenlySplitStepper{
			regionPerSpan: maxRegionPerSpan,
			spanCount:     totalRegion / maxRegionPerSpan,
			remain:        0,
		}
	}
	spanCount := totalRegion/maxRegionPerSpan + 1
	regionPerSpan := totalRegion / spanCount
	return evenlySplitStepper{
		regionPerSpan: regionPerSpan,
		spanCount:     spanCount,
		remain:        totalRegion - regionPerSpan*spanCount,
	}
}

func (e *evenlySplitStepper) SpanCount() int {
	return e.spanCount
}

func (e *evenlySplitStepper) Step() int {
	if e.remain <= 0 {
		return e.regionPerSpan
	}
	e.remain = e.remain - 1
	return e.regionPerSpan + 1
}
