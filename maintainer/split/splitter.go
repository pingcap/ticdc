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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/utils"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

const (
	// spanRegionLimit is the maximum number of regions a span can cover.
	spanRegionLimit = 50000
	// DefaultMaxSpanNumber is the maximum number of spans that can be split
	// in single batch.
	DefaultMaxSpanNumber = 100
)

// baseSpanNumberCoefficient is the base coefficient that use to
// multiply the number of captures to get the number of spans.
var baseSpanNumberCoefficient = replica.MinSpanNumberCoefficient + 1

// RegionCache is a simplified interface of tikv.RegionCache.
// It is useful to restrict RegionCache usage and mocking in tests.
type RegionCache interface {
	// LoadRegionsInKeyRange loads regions in [startKey,endKey].
	LoadRegionsInKeyRange(
		bo *tikv.Backoffer, startKey, endKey []byte,
	) (regions []*tikv.Region, err error)
}

type splitter interface {
	split(
		ctx context.Context, span *heartbeatpb.TableSpan, totalCaptures int,
	) []*heartbeatpb.TableSpan
}

type Splitter struct {
	regionCounterSplitter *regionCountSplitter
	writeKeySplitter      *writeSplitter
	changefeedID          common.ChangeFeedID
}

// NewSplitter returns a Splitter.
func NewSplitter(
	changefeedID common.ChangeFeedID,
	config *config.ChangefeedSchedulerConfig,
) *Splitter {
	baseSpanNumberCoefficient = config.SplitNumberPerNode
	log.Info("baseSpanNumberCoefficient", zap.Any("ChangefeedID", changefeedID.Name()), zap.Any("baseSpanNumberCoefficient", baseSpanNumberCoefficient))
	return &Splitter{
		changefeedID:          changefeedID,
		regionCounterSplitter: newRegionCountSplitter(changefeedID, config.RegionThreshold, config.RegionCountPerSpan),
		writeKeySplitter:      newWriteSplitter(changefeedID, config.WriteKeyThreshold),
	}
}

func (s *Splitter) SplitSpansByRegion(ctx context.Context,
	span *heartbeatpb.TableSpan,
) []*heartbeatpb.TableSpan {
	spans := []*heartbeatpb.TableSpan{span}
	spans = s.regionCounterSplitter.split(ctx, span)
	if len(spans) > 1 {
		return spans
	}
	return spans
}

func (s *Splitter) SplitSpansByWriteKey(ctx context.Context,
	span *heartbeatpb.TableSpan,
	totalCaptures int,
) []*heartbeatpb.TableSpan {
	spans := []*heartbeatpb.TableSpan{span}
	spans = s.writeKeySplitter.split(ctx, span, totalCaptures)
	if len(spans) > 1 {
		return spans
	}
	return spans
}

// FindHoles returns an array of Span that are not covered in the range
func FindHoles(currentSpan utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication], totalSpan *heartbeatpb.TableSpan) []*heartbeatpb.TableSpan {
	lastSpan := &heartbeatpb.TableSpan{
		TableID:  totalSpan.TableID,
		StartKey: totalSpan.StartKey,
		EndKey:   totalSpan.StartKey,
	}
	var holes []*heartbeatpb.TableSpan
	// table span is sorted
	currentSpan.Ascend(func(current *heartbeatpb.TableSpan, _ *replica.SpanReplication) bool {
		ord := bytes.Compare(lastSpan.EndKey, current.StartKey)
		if ord < 0 {
			// Find a hole.
			holes = append(holes, &heartbeatpb.TableSpan{
				TableID:  totalSpan.TableID,
				StartKey: lastSpan.EndKey,
				EndKey:   current.StartKey,
			})
		} else if ord > 0 {
			log.Panic("map is out of order",
				zap.String("lastSpan", lastSpan.String()),
				zap.String("current", current.String()))
		}
		lastSpan = current
		return true
	})
	// Check if there is a hole in the end.
	// the lastSpan not reach the totalSpan end
	if !bytes.Equal(lastSpan.EndKey, totalSpan.EndKey) {
		holes = append(holes, &heartbeatpb.TableSpan{
			TableID:  totalSpan.TableID,
			StartKey: lastSpan.EndKey,
			EndKey:   totalSpan.EndKey,
		})
	}
	return holes
}

func NextExpectedSpansNumber(oldNum int) int {
	if oldNum < 64 {
		return oldNum * 2
	}
	return min(DefaultMaxSpanNumber, oldNum*3/2)
}

// func getSpansNumber(regionNum, captureNum, expectedNum, maxSpanNum int) int {
// 	spanNum := 1
// 	if regionNum > 1 {
// 		// spanNum = max(expectedNum, captureNum*baseSpanNumberCoefficient, regionNum/spanRegionLimit)
// 		spanNum = captureNum * baseSpanNumberCoefficient
// 	}
// 	return min(spanNum, maxSpanNum)
// }

func getSpansNumber(regionNum, captureNum int) int {
	basicSpanNumber := 1
	var spanNum int
	if regionNum > 1 {
		// spanNum = max(expectedNum, captureNum*baseSpanNumberCoefficient, regionNum/spanRegionLimit)
		spanNum = captureNum * baseSpanNumberCoefficient
	}
	return max(spanNum, basicSpanNumber)
}
