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
	bf "github.com/pingcap/ticdc/pkg/binlog-filter"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	cdcfilter "github.com/pingcap/ticdc/pkg/filter"
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
		log.Info("found table", zap.String("name", tbl.TableName))
		tableNames[tbl.TableName] = struct{}{}
	}
	require.Contains(t, tableNames, "t1")
	require.Contains(t, tableNames, "t3")
	require.NotContains(t, tableNames, "t2")
}

func TestDebugSkipDDLByType(t *testing.T) {
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

	tableFilter, err := cdcfilter.NewFilter(&config.FilterConfig{
		Rules:             []string{"*.*"},
		DebugSkipDDLTypes: []bf.EventType{bf.RenameTable},
	}, "", false, false)
	require.NoError(t, err)

	ddlJobs := []DDLJobWithCommitTs{
		{
			Job:      buildCreateSchemaJobForTest(100, "test", 1000),
			CommitTs: 1000,
		},
		{
			Job:      buildCreateTableJobForTest(100, 200, "t1", 1010),
			CommitTs: 1010,
		},
		{
			Job: buildRenameTableJobForTest(100, 200, "t2", 1020, &model.InvolvingSchemaInfo{
				Database: "test",
				Table:    "t1",
			}),
			CommitTs: 1020,
		},
	}
	for _, ddl := range ddlJobs {
		ddl.Job.BinlogInfo.SchemaVersion = 100
	}

	for _, ddl := range ddlJobs {
		store.writeDDLEvent(ddl)
	}
	store.advancePendingResolvedTs(1020)
	store.tryUpdateResolvedTs()

	require.Eventually(t, func() bool {
		return store.resolvedTs.Load() >= 1020
	}, 5*time.Second, 10*time.Millisecond)

	tables, err := pstorage.getAllPhysicalTables(1020, nil)
	require.NoError(t, err)
	require.Len(t, tables, 1)
	require.Equal(t, "t2", tables[0].TableName)

	err = pstorage.registerTable(200, 1010)
	require.NoError(t, err)
	ss := &schemaStore{
		keyspaceSchemaStoreMap: map[uint32]*keyspaceSchemaStore{
			common.DefaultKeyspace.ID: store,
		},
	}
	tableEvents, err := ss.FetchTableDDLEvents(common.DefaultKeyspace, common.NewDispatcherID(), 200, tableFilter, 1010, 1020)
	require.NoError(t, err)
	require.Empty(t, tableEvents)

	triggerEvents, end, err := ss.FetchTableTriggerDDLEvents(common.DefaultKeyspace, common.NewDispatcherID(), tableFilter, 1010, 10)
	require.NoError(t, err)
	require.Empty(t, triggerEvents)
	require.Equal(t, uint64(1020), end)
}

func TestDebugSkipDDLEventByTypeCoversSupportedDDLTypes(t *testing.T) {
	for _, actionType := range cdcfilter.SupportedDDLActionTypes() {
		ddlEvent := commonEvent.DDLEvent{
			Type:  byte(actionType),
			Query: "ddl",
		}
		eventType := cdcfilter.DDLToEventType(actionType)
		require.NotEqual(t, bf.NullEvent, eventType, actionType.String())

		require.True(t,
			shouldSkipDDLEventInSchemaStore(t, ddlEvent, []bf.EventType{eventType}),
			actionType.String())
		require.True(t,
			shouldSkipDDLEventInSchemaStore(t, ddlEvent, []bf.EventType{bf.AllDDL}),
			actionType.String())

		if cdcfilter.IsAlterTableDDL(actionType) {
			require.True(t,
				shouldSkipDDLEventInSchemaStore(t, ddlEvent, []bf.EventType{bf.AlterTable}),
				actionType.String())
		}
	}
}

func shouldSkipDDLEventInSchemaStore(t *testing.T, ddlEvent commonEvent.DDLEvent, skipDDLTypes []bf.EventType) bool {
	t.Helper()
	tableFilter, err := cdcfilter.NewFilter(&config.FilterConfig{
		Rules:             []string{"*.*"},
		DebugSkipDDLTypes: skipDDLTypes,
	}, "", false, false)
	require.NoError(t, err)
	skipped, err := tableFilter.ShouldSkipDDLEventInSchemaStore(ddlEvent.GetDDLType(), ddlEvent.Query)
	require.NoError(t, err)
	return skipped
}

func TestGetAllPhysicalTablesReturnsSnapshotLostByGCError(t *testing.T) {
	dir := t.TempDir()
	pstorage := newPersistentStorageForTest(dir, nil)
	defer func() {
		err := pstorage.close()
		require.NoError(t, err)
	}()

	pstorage.mu.Lock()
	pstorage.gcTs = 100
	pstorage.mu.Unlock()

	_, err := pstorage.getAllPhysicalTables(99, nil)
	require.Error(t, err)
	require.True(t, cerror.ErrSnapshotLostByGC.Equal(err))
	require.Contains(t, err.Error(), "checkpoint-ts 99 is earlier than or equal to GC safepoint at 100")
	require.NotContains(t, err.Error(), "%!d")
}
