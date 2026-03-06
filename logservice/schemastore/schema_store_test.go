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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pingcap/log"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type flakyEncryptionManagerForTest struct {
	failTimes int
	calls     int
}

func (m *flakyEncryptionManagerForTest) EncryptData(ctx context.Context, keyspaceID uint32, data []byte) ([]byte, error) {
	m.calls++
	if m.calls <= m.failTimes {
		return nil, errors.New("inject encryption failure")
	}
	return data, nil
}

func (m *flakyEncryptionManagerForTest) DecryptData(ctx context.Context, keyspaceID uint32, encryptedData []byte) ([]byte, error) {
	return encryptedData, nil
}

type prefixEncryptionManagerForTest struct{}

var encryptedPrefixForTest = []byte("enc:")

func (m *prefixEncryptionManagerForTest) EncryptData(ctx context.Context, keyspaceID uint32, data []byte) ([]byte, error) {
	encrypted := make([]byte, 0, len(encryptedPrefixForTest)+len(data))
	encrypted = append(encrypted, encryptedPrefixForTest...)
	encrypted = append(encrypted, data...)
	return encrypted, nil
}

func (m *prefixEncryptionManagerForTest) DecryptData(ctx context.Context, keyspaceID uint32, encryptedData []byte) ([]byte, error) {
	if len(encryptedData) < len(encryptedPrefixForTest) {
		return nil, errors.New("encrypted data too short")
	}
	if string(encryptedData[:len(encryptedPrefixForTest)]) != string(encryptedPrefixForTest) {
		return nil, errors.New("invalid encrypted prefix")
	}
	plaintext := make([]byte, len(encryptedData)-len(encryptedPrefixForTest))
	copy(plaintext, encryptedData[len(encryptedPrefixForTest):])
	return plaintext, nil
}

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

func TestTryUpdateResolvedTsRetryAfterDDLHandleFailure(t *testing.T) {
	mockPDClock := pdutil.NewClock4Test()
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)

	dir := t.TempDir()
	pstorage := newPersistentStorageForTest(dir, nil)
	defer func() {
		err := pstorage.close()
		require.NoError(t, err)
	}()
	pstorage.encryptionManager = &flakyEncryptionManagerForTest{failTimes: 1}

	store := &keyspaceSchemaStore{
		pdClock:       mockPDClock,
		unsortedCache: newDDLCache(),
		dataStorage:   pstorage,
		notifyCh:      make(chan any, 1),
	}
	store.resolvedTs.Store(pstorage.gcTs)
	store.pendingResolvedTs.Store(pstorage.gcTs)

	createSchema := buildCreateSchemaJobForTest(100, "test", 1000)
	createSchema.BinlogInfo.SchemaVersion = 1
	createTable := buildCreateTableJobForTest(100, 200, "t1", 1010)
	createTable.BinlogInfo.SchemaVersion = 2

	store.writeDDLEvent(DDLJobWithCommitTs{
		Job:      createSchema,
		CommitTs: 1000,
	})
	store.writeDDLEvent(DDLJobWithCommitTs{
		Job:      createTable,
		CommitTs: 1010,
	})
	store.advancePendingResolvedTs(1010)

	// First attempt fails when writing create schema; dedup watermarks should not advance.
	store.tryUpdateResolvedTs()
	require.Equal(t, int64(0), store.schemaVersion)
	require.Equal(t, uint64(0), store.finishedDDLTs)
	require.Equal(t, uint64(0), store.resolvedTs.Load())

	// Second attempt retries failed DDLs and succeeds.
	store.tryUpdateResolvedTs()
	require.Equal(t, int64(2), store.schemaVersion)
	require.Equal(t, uint64(1010), store.finishedDDLTs)
	require.Equal(t, uint64(1010), store.resolvedTs.Load())

	tables, err := pstorage.getAllPhysicalTables(1010, nil)
	require.NoError(t, err)
	require.Len(t, tables, 1)
	require.Equal(t, "t1", tables[0].SchemaTableName.TableName)
}

func TestGetAllPhysicalTablesDecryptsEncryptedDDLEvents(t *testing.T) {
	dir := t.TempDir()
	pstorage := newPersistentStorageForTest(dir, nil)
	defer func() {
		err := pstorage.close()
		require.NoError(t, err)
	}()
	pstorage.encryptionManager = &prefixEncryptionManagerForTest{}

	createSchema := buildCreateSchemaJobForTest(100, "test", 1000)
	createSchema.BinlogInfo.SchemaVersion = 1
	createTable := buildCreateTableJobForTest(100, 200, "t1", 1010)
	createTable.BinlogInfo.SchemaVersion = 2

	err := pstorage.handleDDLJob(createSchema)
	require.NoError(t, err)
	err = pstorage.handleDDLJob(createTable)
	require.NoError(t, err)

	tables, err := pstorage.getAllPhysicalTables(1010, nil)
	require.NoError(t, err)
	require.Len(t, tables, 1)
	require.Equal(t, int64(200), tables[0].TableID)
	require.Equal(t, int64(100), tables[0].SchemaID)
	require.Equal(t, "t1", tables[0].SchemaTableName.TableName)
}

func TestRegisterTableDecryptsEncryptedDDLEvents(t *testing.T) {
	dir := t.TempDir()
	pstorage := newPersistentStorageForTest(dir, nil)
	defer func() {
		err := pstorage.close()
		require.NoError(t, err)
	}()
	pstorage.encryptionManager = &prefixEncryptionManagerForTest{}

	createSchema := buildCreateSchemaJobForTest(100, "test", 1000)
	createSchema.BinlogInfo.SchemaVersion = 1
	createTable := buildCreateTableJobForTest(100, 200, "t1", 1010)
	createTable.BinlogInfo.SchemaVersion = 2

	err := pstorage.handleDDLJob(createSchema)
	require.NoError(t, err)
	err = pstorage.handleDDLJob(createTable)
	require.NoError(t, err)

	err = pstorage.registerTable(200, 1010)
	require.NoError(t, err)

	tableInfo, err := pstorage.getTableInfo(200, 1010)
	require.NoError(t, err)
	require.NotNil(t, tableInfo)
}
