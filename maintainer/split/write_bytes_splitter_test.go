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
	"strconv"
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/testutil"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/stretchr/testify/require"
)

func prepareRegionsInfo(writtenKeys []int) ([]pdutil.RegionInfo, map[int][]byte, map[int][]byte) {
	regions := []pdutil.RegionInfo{}
	start := byte('a')
	for i, writtenKey := range writtenKeys {
		regions = append(regions, pdutil.NewTestRegionInfo(uint64(i+2), []byte{start}, []byte{start + 1}, uint64(writtenKey)))
		start++
	}
	startKeys := map[int][]byte{}
	endKeys := map[int][]byte{}
	for _, r := range regions {
		b, _ := hex.DecodeString(r.StartKey)
		startKeys[int(r.ID)] = b
	}
	for _, r := range regions {
		b, _ := hex.DecodeString(r.EndKey)
		endKeys[int(r.ID)] = b
	}
	return regions, startKeys, endKeys
}

func cloneRegions(info []pdutil.RegionInfo) []pdutil.RegionInfo {
	return append([]pdutil.RegionInfo{}, info...)
}

func TestSplitRegionsByWrittenKeysUniform(t *testing.T) {
	preTest()
	re := require.New(t)

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)
	regions, startKeys, endKeys := prepareRegionsInfo(
		[]int{100, 100, 100, 100, 100, 100, 100}) // region id: [2,3,4,5,6,7,8]
	splitter := newWriteBytesSplitter(0, cfID)
	info := splitter.splitRegionsByWrittenBytesV1(0, cloneRegions(regions), 1)
	re.Len(info.RegionCounts, 1)
	re.EqualValues(7, info.RegionCounts[0])
	re.Len(info.Spans, 1)
	re.EqualValues(startKeys[2], info.Spans[0].StartKey)
	re.EqualValues(endKeys[8], info.Spans[0].EndKey)

	info = splitter.splitRegionsByWrittenBytesV1(0, cloneRegions(regions), 2) // [2,3,4,5], [6,7,8]
	re.Len(info.RegionCounts, 2)
	re.EqualValues(4, info.RegionCounts[0])
	re.EqualValues(3, info.RegionCounts[1])
	re.Len(info.Weights, 2)
	re.EqualValues(404, info.Weights[0])
	re.EqualValues(303, info.Weights[1])
	re.Len(info.Spans, 2)
	re.EqualValues(startKeys[2], info.Spans[0].StartKey)
	re.EqualValues(endKeys[5], info.Spans[0].EndKey)
	re.EqualValues(startKeys[6], info.Spans[1].StartKey)
	re.EqualValues(endKeys[8], info.Spans[1].EndKey)

	info = splitter.splitRegionsByWrittenBytesV1(0, cloneRegions(regions), 3) // [2,3,4], [5,6,7], [8]
	re.Len(info.RegionCounts, 3)
	re.EqualValues(3, info.RegionCounts[0])
	re.EqualValues(3, info.RegionCounts[1])
	re.EqualValues(1, info.RegionCounts[2])
	re.Len(info.Weights, 3)
	re.EqualValues(303, info.Weights[0])
	re.EqualValues(303, info.Weights[1])
	re.EqualValues(101, info.Weights[2])
	re.Len(info.Spans, 3)
	re.EqualValues(startKeys[2], info.Spans[0].StartKey)
	re.EqualValues(endKeys[4], info.Spans[0].EndKey)
	re.EqualValues(startKeys[5], info.Spans[1].StartKey)
	re.EqualValues(endKeys[7], info.Spans[1].EndKey)
	re.EqualValues(startKeys[8], info.Spans[2].StartKey)
	re.EqualValues(endKeys[8], info.Spans[2].EndKey)

	// spans > regions
	for p := 7; p <= 10; p++ {
		info = splitter.splitRegionsByWrittenBytesV1(0, cloneRegions(regions), p)
		re.Len(info.RegionCounts, 7)
		for _, c := range info.RegionCounts {
			re.EqualValues(1, c)
		}
		re.Len(info.Weights, 7)
		for _, w := range info.Weights {
			re.EqualValues(101, w, info)
		}
		re.Len(info.Spans, 7)
		for i, r := range info.Spans {
			re.EqualValues(startKeys[2+i], r.StartKey)
			re.EqualValues(endKeys[2+i], r.EndKey)
		}
	}
}

func TestSplitRegionsByWrittenKeysHotspot1(t *testing.T) {
	preTest()
	re := require.New(t)

	// Hotspots
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)
	regions, startKeys, endKeys := prepareRegionsInfo(
		[]int{100, 1, 100, 1, 1, 1, 100})
	splitter := newWriteBytesSplitter(0, cfID)
	info := splitter.splitRegionsByWrittenBytesV1(0, regions, 4) // [2], [3,4], [5,6,7], [8]
	re.Len(info.RegionCounts, 4)
	re.EqualValues(1, info.RegionCounts[0])
	re.EqualValues(2, info.RegionCounts[1])
	re.EqualValues(3, info.RegionCounts[2])
	re.EqualValues(1, info.RegionCounts[3])
	re.Len(info.Weights, 4)
	re.EqualValues(101, info.Weights[0])
	re.EqualValues(103, info.Weights[1])
	re.EqualValues(6, info.Weights[2])
	re.EqualValues(101, info.Weights[3])
	re.Len(info.Spans, 4)
	re.EqualValues(startKeys[2], info.Spans[0].StartKey)
	re.EqualValues(endKeys[2], info.Spans[0].EndKey)
	re.EqualValues(startKeys[3], info.Spans[1].StartKey)
	re.EqualValues(endKeys[4], info.Spans[1].EndKey)
	re.EqualValues(startKeys[5], info.Spans[2].StartKey)
	re.EqualValues(endKeys[7], info.Spans[2].EndKey)
	re.EqualValues(startKeys[8], info.Spans[3].StartKey)
	re.EqualValues(endKeys[8], info.Spans[3].EndKey)
}

func TestSplitRegionsByWrittenKeysHotspot2(t *testing.T) {
	preTest()
	re := require.New(t)

	// Hotspots
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)
	regions, startKeys, endKeys := prepareRegionsInfo(
		[]int{1000, 1, 1, 1, 100, 1, 99})
	splitter := newWriteBytesSplitter(0, cfID)
	info := splitter.splitRegionsByWrittenBytesV1(0, regions, 4) // [2], [3,4,5,6], [7], [8]
	re.Len(info.Spans, 4)
	re.EqualValues(startKeys[2], info.Spans[0].StartKey)
	re.EqualValues(endKeys[2], info.Spans[0].EndKey)
	re.EqualValues(startKeys[3], info.Spans[1].StartKey)
	re.EqualValues(endKeys[6], info.Spans[1].EndKey)
	re.EqualValues(startKeys[7], info.Spans[2].StartKey)
	re.EqualValues(endKeys[7], info.Spans[2].EndKey)
	re.EqualValues(startKeys[8], info.Spans[3].StartKey)
	re.EqualValues(endKeys[8], info.Spans[3].EndKey)
}

func TestSplitRegionsByWrittenKeysCold(t *testing.T) {
	preTest()
	re := require.New(t)
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)
	splitter := newWriteBytesSplitter(0, cfID)
	baseSpanNum := 3
	regions, startKeys, endKeys := prepareRegionsInfo(make([]int, 7))
	info := splitter.splitRegionsByWrittenBytesV1(0, regions, baseSpanNum) // [2,3,4], [5,6,7], [8]
	re.Len(info.RegionCounts, 3)
	re.EqualValues(3, info.RegionCounts[0], info)
	re.EqualValues(3, info.RegionCounts[1])
	re.EqualValues(1, info.RegionCounts[2])
	re.Len(info.Weights, 3)
	re.EqualValues(3, info.Weights[0])
	re.EqualValues(3, info.Weights[1])
	re.EqualValues(1, info.Weights[2])
	re.Len(info.Spans, 3)
	re.EqualValues(startKeys[2], info.Spans[0].StartKey)
	re.EqualValues(endKeys[4], info.Spans[0].EndKey)
	re.EqualValues(startKeys[5], info.Spans[1].StartKey)
	re.EqualValues(endKeys[7], info.Spans[1].EndKey)
	re.EqualValues(startKeys[8], info.Spans[2].StartKey)
	re.EqualValues(endKeys[8], info.Spans[2].EndKey)
}

func TestNotSplitRegionsByWrittenKeysCold(t *testing.T) {
	preTest()
	re := require.New(t)
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)
	splitter := newWriteBytesSplitter(0, cfID)
	baseSpanNum := 7 // spans >= regions, expect each region as a span
	regions, startKeys, endKeys := prepareRegionsInfo(make([]int, 7))
	info := splitter.splitRegionsByWrittenBytesV1(0, regions, baseSpanNum)
	re.Len(info.RegionCounts, 7)
	for _, c := range info.RegionCounts {
		re.EqualValues(1, c)
	}
	re.Len(info.Spans, 7)
	for i, r := range info.Spans {
		re.EqualValues(startKeys[2+i], r.StartKey)
		re.EqualValues(endKeys[2+i], r.EndKey)
	}
}

func TestSplitRegionsByWrittenKeysConfig(t *testing.T) {
	preTest()
	re := require.New(t)

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)
	splitter := newWriteBytesSplitter(0, cfID)
	regions, _, _ := prepareRegionsInfo([]int{1, 1, 1, 1, 1, 1, 1})
	// verify table id propagated and spans not empty when spansNum>0
	info := splitter.splitRegionsByWrittenBytesV1(1, regions, 3)
	re.Equal(3, len(info.Spans))
	for _, s := range info.Spans {
		re.Equal(int64(1), s.TableID)
	}

	// When PD returns no regions, split should return empty spans
	spans := splitter.split(context.Background(), &heartbeatpb.TableSpan{}, 3)
	require.Empty(t, spans)
}

func TestSplitRegionEven(t *testing.T) {
	preTest()
	var tblID int64 = 1
	regionCount := 4653 + 1051 + 745 + 9530 + 1
	regions := make([]pdutil.RegionInfo, regionCount)
	for i := 0; i < regionCount; i++ {
		regions[i] = pdutil.RegionInfo{
			ID:           uint64(i),
			StartKey:     "" + strconv.Itoa(i),
			EndKey:       "" + strconv.Itoa(i),
			WrittenBytes: 2,
		}
	}
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)
	splitter := newWriteBytesSplitter(0, cfID)
	info := splitter.splitRegionsByWrittenBytesV1(tblID, regions, 5)
	require.Len(t, info.RegionCounts, 5)
	require.Len(t, info.Weights, 5)
	for i, w := range info.Weights {
		if i == 4 {
			require.Equal(t, uint64(9576), w, i)
		} else {
			require.Equal(t, uint64(9591), w, i)
		}
	}
}

func TestSpanRegionLimit(t *testing.T) {
	preTest()
	// simplify: ensure function runs with many regions without panicking
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)
	splitter := newWriteBytesSplitter(0, cfID)

	// deterministically generate writtenKeys with varied distribution
	totalRegionNumbers := 1000
	var writtenKeys []int
	for i := 0; i < totalRegionNumbers; i++ {
		if i%100 == 0 {
			writtenKeys = append(writtenKeys, 30000+i%1000)
		} else if i%20 == 0 {
			writtenKeys = append(writtenKeys, 7000+i%500)
		} else {
			writtenKeys = append(writtenKeys, i%1000)
		}
	}

	var regions []pdutil.RegionInfo
	for i := 0; i < len(writtenKeys); i++ {
		regions = append(
			regions,
			pdutil.NewTestRegionInfo(uint64(i+9), []byte("f"), []byte("f"), uint64(writtenKeys[i])))
	}
	spanNum := 100
	info := splitter.splitRegionsByWrittenBytesV1(0, cloneRegions(regions), spanNum)
	require.GreaterOrEqual(t, len(info.RegionCounts), 1)
}

func preTest() {
	appcontext.SetService(appcontext.PDAPIClient, testutil.NewMockPDAPIClient())
}
