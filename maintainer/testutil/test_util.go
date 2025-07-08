// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package testutil

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/tikv/client-go/v2/tikv"
)

// GetTableSpanByID returns a mock TableSpan for testing
func GetTableSpanByID(id common.TableID) *heartbeatpb.TableSpan {
	totalSpan := common.TableIDToComparableSpan(id)
	return &heartbeatpb.TableSpan{
		TableID:  totalSpan.TableID,
		StartKey: totalSpan.StartKey,
		EndKey:   totalSpan.EndKey,
	}
}

// InitializeTestServices sets up the node manager and message center for testing
func SetUpTestServices() {
	n := node.NewInfo("", "")
	mockPDClock := pdutil.NewClock4Test()
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)

	mc := messaging.NewMessageCenter(context.Background(), n.ID, config.NewDefaultMessageCenterConfig(n.AdvertiseAddr), nil)
	mc.Run(context.Background())
	defer mc.Close()
	appcontext.SetService(appcontext.MessageCenter, mc)

	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	regionCache := NewMockRegionCache()
	appcontext.SetService(appcontext.RegionCache, regionCache)
}

type MockCache struct {
	regions map[string][]*tikv.Region
	err     error
}

// NewMockRegionCache returns a new MockCache.
func NewMockRegionCache() *MockCache {
	return &MockCache{
		regions: make(map[string][]*tikv.Region),
	}
}

func (m *MockCache) SetError(err error) {
	m.err = err
}

func (m *MockCache) SetRegions(key string, regions []*tikv.Region) {
	m.regions[key] = regions
}

func (m *MockCache) LoadRegionsInKeyRange(
	bo *tikv.Backoffer, startKey, endKey []byte,
) (regions []*tikv.Region, err error) {
	if m.err != nil {
		return nil, m.err
	}
	key := fmt.Sprintf("%s-%s", string(startKey), string(endKey))
	return m.regions[key], nil
}

func MockRegionWithKeyRange(id uint64, startKey, endKey []byte) *tikv.Region {
	region := &tikv.Region{}
	meta := &metapb.Region{Id: id, StartKey: startKey, EndKey: endKey}
	regionPtr := (*struct {
		meta *metapb.Region
	})(unsafe.Pointer(region))
	regionPtr.meta = meta
	return region
}

func MockRegionWithID(id uint64) *tikv.Region {
	region := &tikv.Region{}
	meta := &metapb.Region{Id: id}
	regionPtr := (*struct {
		meta *metapb.Region
	})(unsafe.Pointer(region))
	regionPtr.meta = meta
	return region
}
