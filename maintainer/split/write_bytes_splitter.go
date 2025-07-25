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
	"context"
	"encoding/hex"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"go.uber.org/zap"
)

const regionWrittenKeyBase = 1

type writeBytesSplitter struct {
	changefeedID common.ChangeFeedID
	pdAPIClient  pdutil.PDAPIClient
}

type splitRegionsInfo struct {
	RegionCounts []int
	Weights      []uint64
	WriteKeys    []uint64
	Spans        []*heartbeatpb.TableSpan
}

func newWriteBytesSplitter(
	changefeedID common.ChangeFeedID,
) *writeBytesSplitter {
	return &writeBytesSplitter{
		changefeedID: changefeedID,
		pdAPIClient:  appcontext.GetService[pdutil.PDAPIClient](appcontext.PDAPIClient),
	}
}

func (m *writeBytesSplitter) split(
	ctx context.Context,
	span *heartbeatpb.TableSpan,
	spansNum int,
) []*heartbeatpb.TableSpan {
	regions, err := m.pdAPIClient.ScanRegions(ctx, heartbeatpb.TableSpan{
		TableID:  span.TableID,
		StartKey: span.StartKey,
		EndKey:   span.EndKey,
	})
	if err != nil {
		// Skip split.
		log.Warn("scan regions failed, skip split span",
			zap.String("namespace", m.changefeedID.Namespace()),
			zap.String("changefeed", m.changefeedID.Name()),
			zap.String("span", span.String()),
			zap.Error(err))
		return []*heartbeatpb.TableSpan{span}
	}

	splitInfo := m.splitRegionsByWrittenBytesV1(span.TableID, regions, spansNum)
	log.Info("split span by written keys",
		zap.String("namespace", m.changefeedID.Namespace()),
		zap.String("changefeed", m.changefeedID.Name()),
		zap.String("span", span.String()),
		zap.Ints("perSpanRegionCounts", splitInfo.RegionCounts),
		zap.Uint64s("weights", splitInfo.Weights),
		zap.Int("spans", len(splitInfo.Spans)),
		zap.Uint64("spansNum", uint64(spansNum)))

	return splitInfo.Spans
}

// splitRegionsByWrittenBytesV1 tries to split the regions into at least `baseSpansNum` spans,
// each span has approximately the same write weight.
// The algorithm is:
//  1. Sum the written keys of all regions, and normalize the written keys of each region by
//     adding baseline weights (regionWrittenKeyBase) to each region's written keys. Which takes
//     the region number into account.
//  2. Calculate the writeLimitPerSpan.
//  3. Split the table into spans:
//     3.1 If the total write is less than writeKeyThreshold, don't need to split the regions.
//     3.2 If the restSpans count is one, and the restWeight is less than writeLimitPerSpan,
//     we will use the rest regions as the last span. If the restWeight is larger than writeLimitPerSpan,
//     then we need to add more restSpans (restWeight / writeLimitPerSpan) to split the rest regions.
//     3.3 If the restRegions is less than equal to restSpans, then every region will be a span.
//     3.4 If the spanWriteWeight is larger than writeLimitPerSpan then use the region range from spanStartIndex to i to as a span.
//  4. Return the split result.
func (m *writeBytesSplitter) splitRegionsByWrittenBytesV1(
	tableID int64,
	regions []pdutil.RegionInfo,
	spansNum int,
) *splitRegionsInfo {
	decodeKey := func(hexkey string) []byte {
		key, _ := hex.DecodeString(hexkey)
		return key
	}

	totalWrite, totalWriteNormalized := uint64(0), uint64(0)
	for i := range regions {
		totalWrite += regions[i].WrittenBytes
		regions[i].WrittenBytes += regionWrittenKeyBase
		totalWriteNormalized += regions[i].WrittenBytes
	}

	// calc the spansNum by totalWriteNormalized and writeKeyThreshold?

	// 2. Calculate the writeLimitPerSpan, if one span's write is larger that
	// this number, we should create a new span.
	writeLimitPerSpan := totalWriteNormalized / uint64(spansNum)

	// The result of this method
	var (
		regionCounts = make([]int, 0, spansNum)
		writeKeys    = make([]uint64, 0, spansNum)
		weights      = make([]uint64, 0, spansNum)
		spans        = make([]*heartbeatpb.TableSpan, 0, spansNum)
	)

	// Temp variables used in the loop
	var (
		spanWriteWeight = uint64(0)
		spanStartIndex  = 0
		restSpans       = spansNum
		regionCount     = 0
		restWeight      = int64(totalWriteNormalized)
	)

	// 3. Split the table into spans, each span has approximately
	// `writeWeightPerSpan` weight or `spanRegionLimit` regions.
	for i := 0; i < len(regions); i++ {
		restRegions := len(regions) - i
		regionCount++
		spanWriteWeight += regions[i].WrittenBytes
		// If the restSpans count is one, and the restWeight is less than writeLimitPerSpan,
		// we will use the rest regions as the last span. If the restWeight is larger than writeLimitPerSpan,
		// then we need to add more restSpans (restWeight / writeLimitPerSpan) to split the rest regions.
		if restSpans == 1 {
			if restWeight < int64(writeLimitPerSpan) {
				spans = append(spans, &heartbeatpb.TableSpan{
					TableID:  tableID,
					StartKey: decodeKey(regions[spanStartIndex].StartKey),
					EndKey:   decodeKey(regions[len(regions)-1].EndKey),
				})

				lastSpanRegionCount := len(regions) - spanStartIndex
				lastSpanWriteWeight := uint64(0)
				lastSpanWriteKey := uint64(0)
				for j := spanStartIndex; j < len(regions); j++ {
					lastSpanWriteKey += regions[j].WrittenBytes
					lastSpanWriteWeight += regions[j].WrittenBytes
				}
				regionCounts = append(regionCounts, lastSpanRegionCount)
				weights = append(weights, lastSpanWriteWeight)
				writeKeys = append(writeKeys, lastSpanWriteKey)
				break
			}
			// If the restWeight is larger than writeLimitPerSpan,
			// then we need to update the restSpans.
			restSpans = int(restWeight) / int(writeLimitPerSpan)
		}

		// If the restRegions is less than equal to restSpans,
		// then every region will be a span.
		if restRegions <= restSpans {
			spans = append(spans, &heartbeatpb.TableSpan{
				TableID:  tableID,
				StartKey: decodeKey(regions[spanStartIndex].StartKey),
				EndKey:   decodeKey(regions[i].EndKey),
			})
			regionCounts = append(regionCounts, regionCount)
			weights = append(weights, spanWriteWeight)

			// reset the temp variables to start a new span
			restSpans--
			restWeight -= int64(spanWriteWeight)
			spanWriteWeight = 0
			regionCount = 0
			spanStartIndex = i + 1
			continue
		}

		// If the spanWriteWeight is larger than writeLimitPerSpan or the regionCount
		// is larger than spanRegionLimit, then use the region range from
		// spanStartIndex to i to as a span.
		if spanWriteWeight > writeLimitPerSpan {
			spans = append(spans, &heartbeatpb.TableSpan{
				TableID:  tableID,
				StartKey: decodeKey(regions[spanStartIndex].StartKey),
				EndKey:   decodeKey(regions[i].EndKey),
			})
			regionCounts = append(regionCounts, regionCount)
			weights = append(weights, spanWriteWeight)
			// reset the temp variables to start a new span
			restSpans--
			restWeight -= int64(spanWriteWeight)
			spanWriteWeight = 0
			regionCount = 0
			spanStartIndex = i + 1
		}
	}
	return &splitRegionsInfo{
		RegionCounts: regionCounts,
		Weights:      weights,
		WriteKeys:    writeKeys,
		Spans:        spans,
	}
}
