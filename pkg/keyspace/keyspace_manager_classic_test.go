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

//go:build !nextgen

package keyspace

import (
	"context"
	"testing"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
)

func Test_manager_GetKeyspaceByID(t *testing.T) {
	m := &manager{
		keyspaceMap:   make(map[string]*keyspacepb.KeyspaceMeta),
		keyspaceIDMap: make(map[uint32]*keyspacepb.KeyspaceMeta),
		storageMap:    make(map[string]kv.Storage),
	}

	meta := &keyspacepb.KeyspaceMeta{
		Id:   0,
		Name: common.DefaultKeyspace,
	}
	actual1, err := m.GetKeyspaceByID(context.Background(), 1)
	require.NoError(t, err)
	require.EqualValues(t, meta, actual1)
	require.Equal(t, map[string]*keyspacepb.KeyspaceMeta{}, m.keyspaceMap)
	require.Equal(t, map[uint32]*keyspacepb.KeyspaceMeta{}, m.keyspaceIDMap)
}

func Test_manager_LoadKeyspace(t *testing.T) {
	m := &manager{
		keyspaceMap:   make(map[string]*keyspacepb.KeyspaceMeta),
		keyspaceIDMap: make(map[uint32]*keyspacepb.KeyspaceMeta),
		storageMap:    make(map[string]kv.Storage),
	}

	meta := &keyspacepb.KeyspaceMeta{
		Id:   0,
		Name: common.DefaultKeyspace,
	}
	actual1, err := m.LoadKeyspace(context.Background(), "ks1")
	require.NoError(t, err)
	require.EqualValues(t, meta, actual1)
	require.Equal(t, map[string]*keyspacepb.KeyspaceMeta{}, m.keyspaceMap)
	require.Equal(t, map[uint32]*keyspacepb.KeyspaceMeta{}, m.keyspaceIDMap)
}
