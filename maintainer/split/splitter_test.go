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
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/stretchr/testify/require"
)

// TestNewSplitter tests the NewSplitter constructor function
func TestNewSplitter(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	// Set up RegionCache service for testing
	cache := NewMockRegionCache(nil)
	appcontext.SetService(appcontext.RegionCache, cache)

	cfID := common.NewChangeFeedIDWithName("test")
	cfg := &config.ChangefeedSchedulerConfig{
		RegionThreshold:    100,
		RegionCountPerSpan: 10,
		WriteKeyThreshold:  1000,
	}
	mockPDClient := &mockPDAPIClient{}

	splitter := NewSplitter(cfID, mockPDClient, cfg)

	re.NotNil(splitter)
	re.Equal(cfID, splitter.changefeedID)
	re.NotNil(splitter.regionCounterSplitter)
	re.NotNil(splitter.writeKeySplitter)
}

// TestSplitter_Split_ByRegion tests splitting by region count
func TestSplitter_Split_ByRegion(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	// Set up RegionCache service for testing
	cache := NewMockRegionCache(nil)
	appcontext.SetService(appcontext.RegionCache, cache)
	cache.regions.ReplaceOrInsert(heartbeatpb.TableSpan{StartKey: []byte("t1_0"), EndKey: []byte("t1_1")}, 1)
	cache.regions.ReplaceOrInsert(heartbeatpb.TableSpan{StartKey: []byte("t1_1"), EndKey: []byte("t1_2")}, 2)
	cache.regions.ReplaceOrInsert(heartbeatpb.TableSpan{StartKey: []byte("t1_2"), EndKey: []byte("t1_3")}, 3)
	cache.regions.ReplaceOrInsert(heartbeatpb.TableSpan{StartKey: []byte("t1_3"), EndKey: []byte("t1_4")}, 4)
	cache.regions.ReplaceOrInsert(heartbeatpb.TableSpan{StartKey: []byte("t1_4"), EndKey: []byte("t2_2")}, 5)
	cache.regions.ReplaceOrInsert(heartbeatpb.TableSpan{StartKey: []byte("t2_2"), EndKey: []byte("t2_3")}, 6)

	cfID := common.NewChangeFeedIDWithName("test")
	cfg := &config.ChangefeedSchedulerConfig{
		RegionThreshold:    2,
		RegionCountPerSpan: 10,
		WriteKeyThreshold:  1000,
	}
	mockPDClient := &mockPDAPIClient{}

	splitter := NewSplitter(cfID, mockPDClient, cfg)

	span := &heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte("t1"),
		EndKey:   []byte("t2"),
	}

	// Test splitting by region count
	spans := splitter.Split(context.Background(), span, 2, SplitByRegion)
	re.Equal(2, len(spans))
	re.Equal(&heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t1_3")}, spans[0])
	re.Equal(&heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("t1_3"), EndKey: []byte("t2")}, spans[1])
}

// TestSplitter_Split_ByTraffic tests splitting by traffic/write keys
func TestSplitter_Split_ByTraffic(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	// Set up RegionCache service for testing
	cache := NewMockRegionCache(nil)
	appcontext.SetService(appcontext.RegionCache, cache)

	cfID := common.NewChangeFeedIDWithName("test")
	cfg := &config.ChangefeedSchedulerConfig{
		RegionThreshold:    2, // Set small threshold to trigger splitting
		RegionCountPerSpan: 2,
		WriteKeyThreshold:  50,
	}

	// Create a PDClient that returns mock region data
	mockPDClient := &testMockPDAPIClientWithData{
		regions: []pdutil.RegionInfo{
			{ID: 1, StartKey: "61", EndKey: "62", WrittenKeys: 100}, // 'a' to 'b'
			{ID: 2, StartKey: "62", EndKey: "63", WrittenKeys: 200}, // 'b' to 'c'
			{ID: 3, StartKey: "63", EndKey: "64", WrittenKeys: 300}, // 'c' to 'd'
			{ID: 4, StartKey: "64", EndKey: "65", WrittenKeys: 400}, // 'd' to 'e'
		},
	}

	splitter := NewSplitter(cfID, mockPDClient, cfg)

	span := &heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte("a"),
		EndKey:   []byte("e"),
	}

	// Test splitting by traffic with real data
	spans := splitter.Split(context.Background(), span, 2, SplitByTraffic)
	re.Equal(2, len(spans))
	re.Equal(&heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("a"), EndKey: []byte("d")}, spans[0])
	re.Equal(&heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("d"), EndKey: []byte("e")}, spans[1])
}

// TestSplitter_Split_UnknownSplitType tests handling of unknown split types
func TestSplitter_Split_UnknownSplitType(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	// Set up RegionCache service for testing
	cache := NewMockRegionCache(nil)
	appcontext.SetService(appcontext.RegionCache, cache)

	cfID := common.NewChangeFeedIDWithName("test")
	cfg := &config.ChangefeedSchedulerConfig{
		RegionThreshold:    100,
		RegionCountPerSpan: 10,
		WriteKeyThreshold:  500,
	}

	// Create a PDClient that returns mock region data
	mockPDClient := &testMockPDAPIClientWithData{
		regions: []pdutil.RegionInfo{
			{ID: 1, StartKey: "61", EndKey: "62", WrittenKeys: 100}, // 'a' to 'b'
			{ID: 2, StartKey: "62", EndKey: "63", WrittenKeys: 200}, // 'b' to 'c'
			{ID: 3, StartKey: "63", EndKey: "64", WrittenKeys: 300}, // 'c' to 'd'
			{ID: 4, StartKey: "64", EndKey: "65", WrittenKeys: 400}, // 'd' to 'e'
		},
	}

	splitter := NewSplitter(cfID, mockPDClient, cfg)

	span := &heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte("a"),
		EndKey:   []byte("e"),
	}

	// Test unknown split type
	spans := splitter.Split(context.Background(), span, 3, SplitType(999))
	re.NotNil(spans)
	re.Len(spans, 1)
	re.Equal(span, spans[0])
}

// testMockPDAPIClientWithData is a mock implementation that returns simulated data
type testMockPDAPIClientWithData struct {
	regions []pdutil.RegionInfo
}

func (m *testMockPDAPIClientWithData) ScanRegions(ctx context.Context, span heartbeatpb.TableSpan) ([]pdutil.RegionInfo, error) {
	return m.regions, nil
}

func (m *testMockPDAPIClientWithData) Close() {}

func (m *testMockPDAPIClientWithData) UpdateMetaLabel(ctx context.Context) error {
	return nil
}

func (m *testMockPDAPIClientWithData) ListGcServiceSafePoint(ctx context.Context) (*pdutil.ListServiceGCSafepoint, error) {
	return nil, nil
}

func (m *testMockPDAPIClientWithData) CollectMemberEndpoints(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (m *testMockPDAPIClientWithData) Healthy(ctx context.Context, endpoint string) error {
	return nil
}

// TestRegionCacheInterface tests the RegionCache interface
func TestRegionCacheInterface(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	// Test basic functionality of RegionCache interface
	var cache RegionCache
	re.Nil(cache) // Zero value of interface is nil

	// More tests for RegionCache interface can be added here
	// but since it's an interface, main testing should be done in concrete implementations
}
