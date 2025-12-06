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
	"github.com/pingcap/ticdc/maintainer/testutil"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

// TestNewSplitter tests the NewSplitter constructor function
func TestNewSplitter(t *testing.T) {
	re := require.New(t)

	// Set up RegionCache service for testing
	cache := NewMockRegionCache(nil)
	appcontext.SetService(appcontext.RegionCache, cache)

	// Set up PDAPIClient service for testing
	mockPDClient := testutil.NewMockPDAPIClient()
	appcontext.SetService(appcontext.PDAPIClient, mockPDClient)

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)
	cfg := &config.ChangefeedSchedulerConfig{
		RegionThreshold:    100,
		RegionCountPerSpan: 10,
		WriteKeyThreshold:  1000,
	}

	splitter := NewSplitter(common.DefaultKeyspaceID, cfID, cfg)

	re.NotNil(splitter)
	re.Equal(cfID, splitter.changefeedID)
	re.NotNil(splitter.regionCounterSplitter)
	re.NotNil(splitter.writeBytesSplitter)
}

// TestSplitter_Split_ByRegion tests splitting by region count
func TestSplitter_Split_ByRegion(t *testing.T) {
	re := require.New(t)

	// Set up RegionCache service for testing
	cache := NewMockRegionCache(nil)
	appcontext.SetService(appcontext.RegionCache, cache)
	cache.regions.ReplaceOrInsert(*heartbeatpb.NewTableSpan(0, []byte("t1_0"), []byte("t1_1"), 0), 1)
	cache.regions.ReplaceOrInsert(*heartbeatpb.NewTableSpan(0, []byte("t1_1"), []byte("t1_2"), 0), 2)
	cache.regions.ReplaceOrInsert(*heartbeatpb.NewTableSpan(0, []byte("t1_2"), []byte("t1_3"), 0), 3)
	cache.regions.ReplaceOrInsert(*heartbeatpb.NewTableSpan(0, []byte("t1_3"), []byte("t1_4"), 0), 4)
	cache.regions.ReplaceOrInsert(*heartbeatpb.NewTableSpan(0, []byte("t1_4"), []byte("t2_2"), 0), 5)
	cache.regions.ReplaceOrInsert(*heartbeatpb.NewTableSpan(0, []byte("t2_2"), []byte("t2_3"), 0), 6)

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)
	cfg := &config.ChangefeedSchedulerConfig{
		RegionThreshold:    2,
		RegionCountPerSpan: 10,
		WriteKeyThreshold:  1000,
	}

	splitter := NewSplitter(0, cfID, cfg)

	span := heartbeatpb.NewTableSpan(1, []byte("t1"), []byte("t2"), 0)

	// Test splitting by region count
	spans := splitter.Split(context.Background(), span, 2, SplitTypeRegionCount)
	re.Equal(2, len(spans))
	re.Equal(heartbeatpb.NewTableSpan(1, []byte("t1"), []byte("t1_3"), 0), spans[0])
	re.Equal(heartbeatpb.NewTableSpan(1, []byte("t1_3"), []byte("t2"), 0), spans[1])
}
