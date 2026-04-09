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

package schemastore

import (
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestIgnoreDDLByCommitTs(t *testing.T) {
	// 1. Setup a mock SchemaStore.
	// We don't need a real puller or kv storage for this test.
	mockPDClock := pdutil.NewClock4Test()
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)

	dir := t.TempDir()
	// Use newPersistentStorageForTest to bypass the dependency on PD for getting gc safe point.
	pstorage := newPersistentStorageForTest(dir, nil)
	defer func() {
		err := pstorage.close()
		require.NoError(t, err)
	}()

	store := &keyspaceSchemaStore{
		pdClock:       mockPDClock,
		unsortedCache: newDDLCache(),
		dataStorage:   pstorage,
		notifyCh:      make(chan any, 1),
	}
	store.resolvedTs.Store(pstorage.gcTs)
	store.pendingResolvedTs.Store(pstorage.gcTs)

	// 2. Set the config to ignore a specific commit ts.
	ignoreCommitTs := uint64(1020)

	originalConfig := config.GetGlobalServerConfig()
	cfg := originalConfig.Clone()
	cfg.Debug.SchemaStore.IgnoreDDLCommitTs = []uint64{ignoreCommitTs}
	config.StoreGlobalServerConfig(cfg)
	defer config.StoreGlobalServerConfig(originalConfig)

	// 3. Prepare DDL jobs.
	ddlJobs := []DDLJobWithCommitTs{
		{
			Job:      buildCreateSchemaJobForTest(100, "test", 1000),
			CommitTs: 1000,
		},
		{
			Job:      buildCreateTableJobForTest(100, 200, "t1", 1010),
			CommitTs: 1010,
		},
		{ // This DDL should be ignored.
			Job:      buildCreateTableJobForTest(100, 201, "t2", 1020),
			CommitTs: ignoreCommitTs,
		},
		{
			Job:      buildCreateTableJobForTest(100, 202, "t3", 1030),
			CommitTs: 1030,
		},
	}
	// set SchemaVersion to an non empty value to avoid it is filtered by SchemaStore
	for _, ddl := range ddlJobs {
		ddl.Job.BinlogInfo.SchemaVersion = 100
	}

	// 4. Feed DDL jobs and advance resolved ts.
	for _, ddl := range ddlJobs {
		store.writeDDLEvent(ddl)
	}
	store.advancePendingResolvedTs(1030)
	store.tryUpdateResolvedTs()

	// 5. Verify the result.
	// Wait for the resolved ts to be advanced.
	require.Eventually(t, func() bool {
		return store.resolvedTs.Load() >= 1030
	}, 5*time.Second, 10*time.Millisecond)

	tables, err := pstorage.getAllPhysicalTables(1030, nil)
	require.NoError(t, err)

	// Only table t1 and t3 should exist. t2 should be ignored.
	require.Len(t, tables, 2)
	tableNames := make(map[string]struct{})
	for _, tbl := range tables {
		log.Info("found table", zap.String("name", tbl.SchemaTableName.TableName))
		tableNames[tbl.SchemaTableName.TableName] = struct{}{}
	}
	require.Contains(t, tableNames, "t1")
	require.Contains(t, tableNames, "t3")
	require.NotContains(t, tableNames, "t2")
}

func TestCreateMaterializedViewWithEmptySchemaVersionIsHandled(t *testing.T) {
	mockPDClock := pdutil.NewClock4Test()
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)

	dir := t.TempDir()
	pstorage := newPersistentStorageForTest(dir, nil)
	defer func() {
		err := pstorage.close()
		require.NoError(t, err)
	}()

	store := &keyspaceSchemaStore{
		pdClock:       mockPDClock,
		unsortedCache: newDDLCache(),
		dataStorage:   pstorage,
		notifyCh:      make(chan any, 1),
	}
	store.resolvedTs.Store(pstorage.gcTs)
	store.pendingResolvedTs.Store(pstorage.gcTs)

	createSchemaDDL := DDLJobWithCommitTs{
		Job:      buildCreateSchemaJobForTest(100, "test", 1000),
		CommitTs: 1000,
	}
	createSchemaDDL.Job.BinlogInfo.SchemaVersion = 100

	createBaseTableDDL := DDLJobWithCommitTs{
		Job:      buildCreateTableJobForTest(100, 150, "t_base", 1005),
		CommitTs: 1005,
	}
	createBaseTableDDL.Job.BinlogInfo.SchemaVersion = 101

	createMVDDL := DDLJobWithCommitTs{
		Job:      buildCreateMaterializedViewJobForTest(100, 200, "mv1", []int64{150}, "select * from t_base", 1010),
		CommitTs: 1010,
	}
	// This is the real case on the cluster: create materialized view comes with SchemaVersion == 0.
	createMVDDL.Job.BinlogInfo.SchemaVersion = 0

	for _, ddl := range []DDLJobWithCommitTs{createSchemaDDL, createBaseTableDDL, createMVDDL} {
		store.writeDDLEvent(ddl)
	}
	store.advancePendingResolvedTs(1010)
	store.tryUpdateResolvedTs()

	require.Eventually(t, func() bool {
		return store.resolvedTs.Load() >= 1010
	}, 5*time.Second, 10*time.Millisecond)

	tables, err := pstorage.getAllPhysicalTables(1010, nil)
	require.NoError(t, err)
	require.Len(t, tables, 2)
	tableNames := make(map[string]struct{}, len(tables))
	for _, table := range tables {
		require.NotNil(t, table.SchemaTableName)
		require.Equal(t, "test", table.SchemaTableName.SchemaName)
		tableNames[table.SchemaTableName.TableName] = struct{}{}
	}
	require.Contains(t, tableNames, "t_base")
	require.Contains(t, tableNames, "mv1")

	events, err := pstorage.fetchTableDDLEvents(common.NewDispatcherID(), 200, nil, 1000, 2000)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, byte(model.ActionCreateMaterializedView), events[0].Type)
}
