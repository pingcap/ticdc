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
	"fmt"
	"testing"
	"unsafe"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/spanz"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestRegionCountSplitSpan(t *testing.T) {
	cache := NewMockRegionCache(nil)
	appcontext.SetService(appcontext.RegionCache, cache)
	cache.regions.ReplaceOrInsert(heartbeatpb.TableSpan{StartKey: []byte("t1_0"), EndKey: []byte("t1_1")}, 1)
	cache.regions.ReplaceOrInsert(heartbeatpb.TableSpan{StartKey: []byte("t1_1"), EndKey: []byte("t1_2")}, 2)
	cache.regions.ReplaceOrInsert(heartbeatpb.TableSpan{StartKey: []byte("t1_2"), EndKey: []byte("t1_3")}, 3)
	cache.regions.ReplaceOrInsert(heartbeatpb.TableSpan{StartKey: []byte("t1_3"), EndKey: []byte("t1_4")}, 4)
	cache.regions.ReplaceOrInsert(heartbeatpb.TableSpan{StartKey: []byte("t1_4"), EndKey: []byte("t2_2")}, 5)
	cache.regions.ReplaceOrInsert(heartbeatpb.TableSpan{StartKey: []byte("t2_2"), EndKey: []byte("t2_3")}, 6)

	cases := []struct {
		span        *heartbeatpb.TableSpan
		cfg         *config.ChangefeedSchedulerConfig
		spansNum    int
		expectSpans []*heartbeatpb.TableSpan
	}{
		{
			span: &heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t2")},
			cfg: &config.ChangefeedSchedulerConfig{
				RegionThreshold:    1,
				RegionCountPerSpan: 1,
			},
			spansNum: 0,
			expectSpans: []*heartbeatpb.TableSpan{
				{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t1_1")},   // 1 region
				{TableID: 1, StartKey: []byte("t1_1"), EndKey: []byte("t1_2")}, // 1 region
				{TableID: 1, StartKey: []byte("t1_2"), EndKey: []byte("t1_3")}, // 1 region
				{TableID: 1, StartKey: []byte("t1_3"), EndKey: []byte("t1_4")}, // 1 region
				{TableID: 1, StartKey: []byte("t1_4"), EndKey: []byte("t2")},   // 1 region
			},
		},
		{
			span: &heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t2")},
			cfg: &config.ChangefeedSchedulerConfig{
				RegionThreshold:    1,
				RegionCountPerSpan: 2,
			},
			spansNum: 0,
			expectSpans: []*heartbeatpb.TableSpan{
				{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t1_2")},   // 2 region
				{TableID: 1, StartKey: []byte("t1_2"), EndKey: []byte("t1_4")}, // 2 region
				{TableID: 1, StartKey: []byte("t1_4"), EndKey: []byte("t2")},   // 1 region
			},
		},
		{
			span: &heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t2")},
			cfg: &config.ChangefeedSchedulerConfig{
				RegionThreshold:    1,
				RegionCountPerSpan: 3,
			},
			spansNum: 0,
			expectSpans: []*heartbeatpb.TableSpan{
				{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t1_3")}, // 3 region
				{TableID: 1, StartKey: []byte("t1_3"), EndKey: []byte("t2")}, // 2 region
			},
		},
		{
			span: &heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t2")},
			cfg: &config.ChangefeedSchedulerConfig{
				RegionThreshold:    1,
				RegionCountPerSpan: 4,
			},
			spansNum: 0,
			expectSpans: []*heartbeatpb.TableSpan{
				{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t1_3")}, // 3 region
				{TableID: 1, StartKey: []byte("t1_3"), EndKey: []byte("t2")}, // 2 region
			},
		},
		{
			span: &heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t2")},
			cfg: &config.ChangefeedSchedulerConfig{
				RegionThreshold:    10,
				RegionCountPerSpan: 2,
			},
			spansNum: 0,
			expectSpans: []*heartbeatpb.TableSpan{
				{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t2")}, // no split
			},
		},
		{
			span: &heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t2")},
			cfg: &config.ChangefeedSchedulerConfig{
				RegionThreshold:    1,
				RegionCountPerSpan: 1,
			},
			spansNum: 2,
			expectSpans: []*heartbeatpb.TableSpan{
				{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t1_3")}, // 3 region
				{TableID: 1, StartKey: []byte("t1_3"), EndKey: []byte("t2")}, // 2 region
			},
		},
	}

	cfID := common.NewChangeFeedIDWithName("test")
	for i, cs := range cases {
		splitter := newRegionCountSplitter(cfID, cs.cfg.RegionCountPerSpan, cs.cfg.RegionThreshold)
		spans := splitter.split(context.Background(), cs.span, cs.spansNum)
		require.Equalf(t, cs.expectSpans, spans, "%d %s", i, cs.span.String())
	}
}

func TestRegionCountEvenlySplitSpan(t *testing.T) {
	cache := NewMockRegionCache(nil)
	appcontext.SetService(appcontext.RegionCache, cache)
	totalRegion := 1000
	for i := 0; i < totalRegion; i++ {
		cache.regions.ReplaceOrInsert(heartbeatpb.TableSpan{
			StartKey: []byte(fmt.Sprintf("t1_%09d", i)),
			EndKey:   []byte(fmt.Sprintf("t1_%09d", i+1)),
		}, uint64(i+1))
	}

	cases := []struct {
		expectedSpans int
		cfg           *config.ChangefeedSchedulerConfig
		spansNum      int
	}{
		{
			expectedSpans: 1000,
			cfg: &config.ChangefeedSchedulerConfig{
				RegionThreshold:    1,
				RegionCountPerSpan: 1,
			},
			spansNum: 0,
		},
		{
			expectedSpans: 500,
			cfg: &config.ChangefeedSchedulerConfig{
				RegionThreshold:    1,
				RegionCountPerSpan: 2,
			},
			spansNum: 0,
		},
		{
			expectedSpans: 334,
			cfg: &config.ChangefeedSchedulerConfig{
				RegionThreshold:    1,
				RegionCountPerSpan: 3,
			},
			spansNum: 0,
		},
		{
			expectedSpans: 250,
			cfg: &config.ChangefeedSchedulerConfig{
				RegionThreshold:    1,
				RegionCountPerSpan: 4,
			},
			spansNum: 0,
		},
		{
			expectedSpans: 200,
			cfg: &config.ChangefeedSchedulerConfig{
				RegionThreshold:    1,
				RegionCountPerSpan: 5,
			},
			spansNum: 0,
		},
		{
			expectedSpans: 167,
			cfg: &config.ChangefeedSchedulerConfig{
				RegionThreshold:    1,
				RegionCountPerSpan: 6,
			},
			spansNum: 0,
		},
		{
			expectedSpans: 143,
			cfg: &config.ChangefeedSchedulerConfig{
				RegionThreshold:    1,
				RegionCountPerSpan: 7,
			},
			spansNum: 0,
		},
		{
			expectedSpans: 125,
			cfg: &config.ChangefeedSchedulerConfig{
				RegionThreshold:    1,
				RegionCountPerSpan: 8,
			},
			spansNum: 0,
		},
	}

	cfID := common.NewChangeFeedIDWithName("test")
	spans := &heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t2")}
	for i, cs := range cases {
		splitter := newRegionCountSplitter(cfID, cs.cfg.RegionCountPerSpan, cs.cfg.RegionThreshold)
		spans := splitter.split(context.Background(), spans, cs.spansNum)
		require.Equalf(t, cs.expectedSpans, len(spans), "%d %v", i, cs)
	}
}

func TestSplitSpanRegionOutOfOrder(t *testing.T) {
	cache := NewMockRegionCache(nil)
	appcontext.SetService(appcontext.RegionCache, cache)
	cache.regions.ReplaceOrInsert(heartbeatpb.TableSpan{StartKey: []byte("t1_0"), EndKey: []byte("t1_1")}, 1)
	cache.regions.ReplaceOrInsert(heartbeatpb.TableSpan{StartKey: []byte("t1_1"), EndKey: []byte("t1_4")}, 2)
	cache.regions.ReplaceOrInsert(heartbeatpb.TableSpan{StartKey: []byte("t1_2"), EndKey: []byte("t1_3")}, 3)

	cfg := &config.ChangefeedSchedulerConfig{
		RegionThreshold:    1,
		RegionCountPerSpan: 1,
	}
	cfID := common.NewChangeFeedIDWithName("test")
	splitter := newRegionCountSplitter(cfID, cfg.RegionCountPerSpan, cfg.RegionCountPerSpan)
	span := &heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t2")}
	spans := splitter.split(context.Background(), span, 0)
	require.Equal(
		t, []*heartbeatpb.TableSpan{{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t2")}}, spans)
}

// mockCache mocks tikv.RegionCache.
type mockCache struct {
	regions *spanz.BtreeMap[uint64]
}

// NewMockRegionCache returns a new MockCache.
func NewMockRegionCache(regions []heartbeatpb.TableSpan) *mockCache {
	return &mockCache{regions: spanz.NewBtreeMap[uint64]()}
}

func (m *mockCache) LoadRegionsInKeyRange(
	bo *tikv.Backoffer, startKey, endKey []byte,
) (regions []*tikv.Region, err error) {
	m.regions.Ascend(func(loc heartbeatpb.TableSpan, id uint64) bool {
		if bytes.Compare(loc.StartKey, endKey) >= 0 ||
			bytes.Compare(loc.EndKey, startKey) <= 0 {
			return true
		}
		region := &tikv.Region{}
		meta := &metapb.Region{
			Id:       id,
			StartKey: loc.StartKey,
			EndKey:   loc.EndKey,
		}

		// meta.id is not exported, so we use unsafe to access it more easier for test.
		regionPtr := (*struct {
			meta *metapb.Region
		})(unsafe.Pointer(region))
		regionPtr.meta = meta

		regions = append(regions, region)
		return true
	})
	return
}
