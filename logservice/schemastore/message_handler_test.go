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

package schemastore

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/schemastoreclient"
	"github.com/stretchr/testify/require"
)

func TestSchemaStoreRequests(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	id := node.NewID()
	previousID := appcontext.GetID()
	appcontext.SetID(id.String())
	defer appcontext.SetID(previousID)
	mc := messaging.NewMessageCenter(ctx, id, config.NewDefaultMessageCenterConfig("127.0.0.1:0"), nil)
	mc.Run(ctx)
	defer mc.Close()
	appcontext.SetService(appcontext.MessageCenter, mc)

	storage := newPersistentStorageForTest(t.TempDir(), nil)
	defer func() { require.NoError(t, storage.close()) }()
	require.NoError(t, storage.handleDDLJob(buildCreateSchemaJobForTest(100, "test", 1000)))
	require.NoError(t, storage.handleDDLJob(buildCreateTableJobForTest(100, 200, "t1", 1010)))
	require.NoError(t, storage.handleDDLJob(buildCreateTableJobForTest(100, 201, "t2", 1020)))
	ks := &keyspaceSchemaStore{dataStorage: storage}
	ks.resolvedTs.Store(1020)
	meta := common.KeyspaceMeta{ID: 7, Name: "ks"}
	store := &schemaStore{
		mc:                     mc,
		keyspaceSchemaStoreMap: map[uint32]*keyspaceSchemaStore{meta.ID: ks},
		tombstoneKeyspaces:     map[uint32]struct{}{8: {}},
	}
	mc.RegisterHandler(messaging.SchemaStoreTopic, store.handleMessage)
	client := schemastoreclient.GetSchemaStoreClient()
	require.NoError(t, client.RegisterKeyspace(ctx, meta))
	require.True(t, errors.ErrKeyspaceNotFound.Equal(client.RegisterKeyspace(ctx, common.KeyspaceMeta{ID: 8})))
	cfg := config.NewDefaultFilterConfig()
	cfg.Rules = []string{"TEST.T1"}
	tables, err := client.GetAllPhysicalTables(ctx, meta, 1020, cfg, false, false)
	require.NoError(t, err)
	require.Len(t, tables, 1)
	require.Equal(t, int64(200), tables[0].TableID)
	require.Equal(t, "test", tables[0].SchemaName)
	require.Equal(t, "t1", tables[0].TableName)
	tables, err = client.GetAllPhysicalTables(ctx, meta, 1020, cfg, true, false)
	require.NoError(t, err)
	require.Empty(t, tables)
	cfg.Rules = []string{"test.*"}
	tables, err = client.GetAllPhysicalTables(ctx, meta, 1010, cfg, true, false)
	require.NoError(t, err)
	require.Len(t, tables, 1)
	storage.mu.Lock()
	storage.gcTs = 100
	storage.mu.Unlock()
	_, err = client.GetAllPhysicalTables(ctx, meta, 0, cfg, true, false)
	require.True(t, errors.ErrSnapshotLostByGC.Equal(err))
	cfg.Rules = []string{"["}
	_, err = client.GetAllPhysicalTables(ctx, meta, 1020, cfg, true, false)
	require.Error(t, err)
}
