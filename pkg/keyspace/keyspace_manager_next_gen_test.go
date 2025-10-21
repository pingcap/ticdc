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

//go:build nextgen

package keyspace

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
)

func Test_manager_update(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockClient := pdutil.NewMockPDAPIClient(ctrl)

	appcontext.SetService(appcontext.PDAPIClient, mockClient)

	const keyspace = "ks1"

	// step 1, load a metadata to make the map has a member
	meta1 := &keyspacepb.KeyspaceMeta{
		Id:             1,
		Name:           keyspace,
		State:          0,
		CreatedAt:      1,
		StateChangedAt: 1,
		Config:         map[string]string{},
	}
	mockClient.EXPECT().LoadKeyspace(gomock.Any(), gomock.Eq(keyspace)).Return(meta1, nil).Times(1)

	m := &manager{
		keyspaceMap:   make(map[string]*keyspacepb.KeyspaceMeta),
		keyspaceIDMap: make(map[uint32]*keyspacepb.KeyspaceMeta),
		storageMap:    make(map[string]kv.Storage),
	}
	m.ForceLoadKeyspace(context.Background(), keyspace)
	require.EqualValues(t, map[string]*keyspacepb.KeyspaceMeta{
		"ks1": meta1,
	}, m.keyspaceMap)
	require.EqualValues(t, map[uint32]*keyspacepb.KeyspaceMeta{
		uint32(1): meta1,
	}, m.keyspaceIDMap)

	// step 2, lock success
	meta2 := &keyspacepb.KeyspaceMeta{
		Id:             1,
		Name:           keyspace,
		State:          0,
		CreatedAt:      1,
		StateChangedAt: 2,
		Config:         map[string]string{},
	}
	mockClient.EXPECT().LoadKeyspace(gomock.Any(), gomock.Eq(keyspace)).Return(meta2, nil).Times(1)
	m.update()
	require.EqualValues(t, map[string]*keyspacepb.KeyspaceMeta{
		"ks1": meta2,
	}, m.keyspaceMap)
	require.EqualValues(t, map[uint32]*keyspacepb.KeyspaceMeta{
		uint32(1): meta2,
	}, m.keyspaceIDMap)

	// step 3, lock failed
	m.updateMu.Lock()
	m.update()
	m.updateMu.Unlock()
	require.EqualValues(t, map[string]*keyspacepb.KeyspaceMeta{
		"ks1": meta2,
	}, m.keyspaceMap)
	require.EqualValues(t, map[uint32]*keyspacepb.KeyspaceMeta{
		uint32(1): meta2,
	}, m.keyspaceIDMap)

	// step 4, lock success again
	meta3 := &keyspacepb.KeyspaceMeta{
		Id:             1,
		Name:           keyspace,
		State:          0,
		CreatedAt:      1,
		StateChangedAt: 3,
		Config:         map[string]string{},
	}
	mockClient.EXPECT().LoadKeyspace(gomock.Any(), gomock.Eq(keyspace)).Return(meta3, nil).Times(1)
	m.update()
	require.EqualValues(t, map[string]*keyspacepb.KeyspaceMeta{
		"ks1": meta3,
	}, m.keyspaceMap)
	require.EqualValues(t, map[uint32]*keyspacepb.KeyspaceMeta{
		uint32(1): meta3,
	}, m.keyspaceIDMap)
}

func Test_manager_GetKeyspaceByID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockClient := pdutil.NewMockPDAPIClient(ctrl)

	appcontext.SetService(appcontext.PDAPIClient, mockClient)

	m := &manager{
		keyspaceMap:   make(map[string]*keyspacepb.KeyspaceMeta),
		keyspaceIDMap: make(map[uint32]*keyspacepb.KeyspaceMeta),
		storageMap:    make(map[string]kv.Storage),
	}

	meta1 := &keyspacepb.KeyspaceMeta{
		Id:             1,
		Name:           "ks1",
		State:          0,
		CreatedAt:      1,
		StateChangedAt: 1,
		Config:         map[string]string{},
	}
	mockClient.EXPECT().GetKeyspaceMetaByID(gomock.Any(), gomock.Eq(uint32(1))).Return(meta1, nil).Times(1)
	actual1, err := m.GetKeyspaceByID(context.Background(), 1)
	require.NoError(t, err)
	require.EqualValues(t, meta1, actual1)
	require.Equal(t, map[string]*keyspacepb.KeyspaceMeta{
		"ks1": meta1,
	}, m.keyspaceMap)
	require.Equal(t, map[uint32]*keyspacepb.KeyspaceMeta{
		1: meta1,
	}, m.keyspaceIDMap)

	// GetKeyspaceByID again and it will load from local cache
	actual2, err := m.GetKeyspaceByID(context.Background(), 1)
	require.NoError(t, err)
	require.EqualValues(t, meta1, actual2)
	require.Equal(t, map[string]*keyspacepb.KeyspaceMeta{
		"ks1": meta1,
	}, m.keyspaceMap)
	require.Equal(t, map[uint32]*keyspacepb.KeyspaceMeta{
		1: meta1,
	}, m.keyspaceIDMap)
}

func Test_manager_LoadKeyspace(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockClient := pdutil.NewMockPDAPIClient(ctrl)

	appcontext.SetService(appcontext.PDAPIClient, mockClient)

	m := &manager{
		keyspaceMap:   make(map[string]*keyspacepb.KeyspaceMeta),
		keyspaceIDMap: make(map[uint32]*keyspacepb.KeyspaceMeta),
		storageMap:    make(map[string]kv.Storage),
	}

	meta1 := &keyspacepb.KeyspaceMeta{
		Id:             1,
		Name:           "ks1",
		State:          0,
		CreatedAt:      1,
		StateChangedAt: 1,
		Config:         map[string]string{},
	}
	mockClient.EXPECT().LoadKeyspace(gomock.Any(), gomock.Eq("ks1")).Return(meta1, nil).Times(1)
	actual1, err := m.LoadKeyspace(context.Background(), "ks1")
	require.NoError(t, err)
	require.EqualValues(t, meta1, actual1)
	require.Equal(t, map[string]*keyspacepb.KeyspaceMeta{
		"ks1": meta1,
	}, m.keyspaceMap)
	require.Equal(t, map[uint32]*keyspacepb.KeyspaceMeta{
		1: meta1,
	}, m.keyspaceIDMap)

	// GetKeyspaceByID again and it will load from local cache
	actual2, err := m.LoadKeyspace(context.Background(), "ks1")
	require.NoError(t, err)
	require.EqualValues(t, meta1, actual2)
	require.Equal(t, map[string]*keyspacepb.KeyspaceMeta{
		"ks1": meta1,
	}, m.keyspaceMap)
	require.Equal(t, map[uint32]*keyspacepb.KeyspaceMeta{
		1: meta1,
	}, m.keyspaceIDMap)
}
