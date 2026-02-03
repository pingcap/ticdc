// Copyright 2021 PingCAP, Inc.
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

package gc

import (
	"context"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/client-go/v2/oracle"
<<<<<<< HEAD
	pdclient "github.com/tikv/pd/client"
	pdopt "github.com/tikv/pd/client"
=======
	pd "github.com/tikv/pd/client"
	pdgc "github.com/tikv/pd/client/clients/gc"
	pdopt "github.com/tikv/pd/client/opt"
>>>>>>> 6a0ae936a (coordinator: make the gc manager always report error if meet (#4119))
)

// MockPDClient mocks pd.Client to facilitate unit testing.
type MockPDClient struct {
	pdclient.Client
	ClusterID        uint64
	GetAllStoresFunc func(ctx context.Context, opts ...pdopt.GetStoreOption) ([]*metapb.Store, error)

	UpdateServiceGCSafePointFunc func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error)
	GetGCStatesClientFunc        func(keyspaceID uint32) pdgc.GCStatesClient
}

// UpdateServiceGCSafePoint implements pd.Client.UpdateServiceGCSafePoint.
func (m *MockPDClient) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	return m.UpdateServiceGCSafePointFunc(ctx, serviceID, ttl, safePoint)
}

// GetTS implements pd.Client.GetTS.
func (m *MockPDClient) GetTS(ctx context.Context) (int64, int64, error) {
	return oracle.GetPhysical(time.Now()), 0, nil
}

// Close implements pd.Client.Close()
// This method is used in some unit test cases.
func (m *MockPDClient) Close() {}

// GetClusterID gets the cluster ID from PD.
func (m *MockPDClient) GetClusterID(ctx context.Context) uint64 {
	return m.ClusterID
}

// GetAllStores gets all stores from PD.
func (m *MockPDClient) GetAllStores(
	ctx context.Context, opts ...pdopt.GetStoreOption,
) ([]*metapb.Store, error) {
	return m.GetAllStoresFunc(ctx, opts...)
}

// LoadGlobalConfig loads global config from PD.
func (m *MockPDClient) LoadGlobalConfig(
	ctx context.Context,
	names []string, configPath string,
) ([]pdopt.GlobalConfigItem, int64, error) {
	return []pdopt.GlobalConfigItem{
		{
			Name:  "source_id",
			Value: "1",
		},
	}, 0, nil
}

// GetGCStatesClient implements pd.Client.GetGCStatesClient.
func (m *MockPDClient) GetGCStatesClient(keyspaceID uint32) pdgc.GCStatesClient {
	if m.GetGCStatesClientFunc != nil {
		return m.GetGCStatesClientFunc(keyspaceID)
	}
	if m.Client != nil {
		return m.Client.GetGCStatesClient(keyspaceID)
	}
	return nil
}
