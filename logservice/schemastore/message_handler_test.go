// Copyright 2026 PingCAP, Inc.
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
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/messaging/mock"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/schemastore"
	"github.com/pingcap/ticdc/pkg/schemastore/client"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func newRequestTestStore(t *testing.T, mc messaging.MessageCenter, workers int) *schemaStore {
	t.Helper()
	store := &schemaStore{
		keyspaceSchemaStoreMap: make(map[uint32]*keyspaceSchemaStore), tombstoneKeyspaces: map[uint32]struct{}{7: {}},
	}
	store.messageHandler = newSchemaStoreMessageHandler(t.Context(), store, mc, workers)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	return store
}

func testRequest(id uint64, keyspace uint32) *schemastore.GetTableInfosRequest {
	return &schemastore.GetTableInfosRequest{
		RequestID: id, Keyspace: schemastore.KeyspaceMeta{ID: keyspace},
		TableIDs: []int64{1}, Ts: 100,
	}
}

func waitRequestInStore(t *testing.T, ks *keyspaceSchemaStore) {
	t.Helper()
	require.Eventually(t, func() bool {
		if ks.lifecycleMu.TryLock() {
			ks.lifecycleMu.Unlock()
			return false
		}
		return true
	}, time.Second, time.Millisecond)
}

func TestSchemaStoreRequestPool(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mc := mock.NewMockMessageCenter(gomock.NewController(t))
		mc.EXPECT().RegisterHandler(messaging.SchemaStoreTopic, gomock.Any())
		mc.EXPECT().DeRegisterHandler(messaging.SchemaStoreTopic).AnyTimes()
		store := newRequestTestStore(t, mc, 1)
		responses := make(chan uint64, 3)
		release := make(chan struct{})
		releaseRequest := sync.OnceFunc(func() { close(release) })
		t.Cleanup(releaseRequest)
		mc.EXPECT().SendCommand(gomock.Any()).Times(3).DoAndReturn(func(msg *messaging.TargetMessage) error {
			id := msg.Message[0].(*schemastore.GetTableInfosResponse).RequestID
			responses <- id
			if id == 1 {
				<-release
			}
			return nil
		})
		require.NoError(t, store.messageHandler.handleMessage(t.Context(), messaging.NewSingleTargetMessage("store", messaging.SchemaStoreTopic,
			testRequest(1, 7))))
		select {
		case id := <-responses:
			require.Equal(t, uint64(1), id)
		case <-time.After(time.Second):
			t.Fatal("first request did not run")
		}
		require.NoError(t, store.messageHandler.handleMessage(t.Context(), messaging.NewSingleTargetMessage("store", messaging.SchemaStoreTopic,
			testRequest(2, 7))))
		// Queued requests do not start their processing timeout until a worker
		// takes them. Advance fake time only after confirming admission.
		synctest.Wait()
		require.Empty(t, responses)
		require.NoError(t, store.messageHandler.handleMessage(t.Context(), messaging.NewSingleTargetMessage("store", messaging.SchemaStoreTopic,
			testRequest(3, 7))))
		time.Sleep(schemastore.RequestTimeout)
		synctest.Wait()
		require.Empty(t, responses)
		releaseRequest()
		for _, expectedID := range []uint64{2, 3} {
			select {
			case id := <-responses:
				require.Equal(t, expectedID, id, "queued requests must execute in FIFO order")
			case <-time.After(time.Second):
				t.Fatal("queued request did not run")
			}
		}
	})
}

func TestSchemaStoreCloseReleasesQueuedMessages(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mc := mock.NewMockMessageCenter(gomock.NewController(t))
		mc.EXPECT().RegisterHandler(messaging.SchemaStoreTopic, gomock.Any())
		mc.EXPECT().DeRegisterHandler(messaging.SchemaStoreTopic).AnyTimes()
		store := newRequestTestStore(t, mc, 1)
		started := make(chan struct{})
		release := make(chan struct{})
		releaseWorker := sync.OnceFunc(func() { close(release) })
		t.Cleanup(releaseWorker)
		mc.EXPECT().SendCommand(gomock.Any()).DoAndReturn(func(msg *messaging.TargetMessage) error {
			require.Equal(t, uint64(1), msg.Message[0].(*schemastore.GetTableInfosResponse).RequestID)
			close(started)
			<-release
			return nil
		})
		require.NoError(t, store.messageHandler.submit(schemaRequest{from: "client", message: testRequest(1, 7)}))
		<-started
		require.NoError(t, store.messageHandler.submit(schemaRequest{from: "client", message: testRequest(2, 7)}))
		// Submissions racing with shutdown must finish without depending on the
		// blocked worker. Accepted requests are discarded on shutdown.
		const concurrentRequests = 16
		results := make(chan error, concurrentRequests)
		var submitters sync.WaitGroup
		for i := range concurrentRequests {
			submitters.Go(func() {
				results <- store.messageHandler.submit(schemaRequest{from: "client", message: testRequest(uint64(i+3), 7)})
			})
		}
		closed := make(chan error, 1)
		go func() { closed <- store.Close(t.Context()) }()
		synctest.Wait()
		require.Empty(t, store.messageHandler.requests, "shutdown must release queued requests while the worker is busy")
		submitters.Wait()
		for range concurrentRequests {
			if err := <-results; err != nil {
				require.ErrorIs(t, err, context.Canceled)
			}
		}
		require.Empty(t, closed, "close must wait for the running worker")
		require.ErrorIs(t, store.messageHandler.submit(schemaRequest{from: "client", message: testRequest(1000, 7)}), context.Canceled)
		releaseWorker()
		require.NoError(t, <-closed)
	})
}

func TestSchemaStoreCallerCancellation(t *testing.T) {
	for _, test := range []struct {
		name       string
		tableInfos bool
	}{
		{"table query", true},
		{"table discovery", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				id := node.NewID()
				mc := messaging.NewMessageCenter(t.Context(), id, config.NewDefaultMessageCenterConfig("127.0.0.1:0"), nil)
				mc.Run(t.Context())
				t.Cleanup(mc.Close)
				store := newRequestTestStore(t, mc, 1)
				ksCtx, ksCancel := context.WithCancel(t.Context())
				ks := &keyspaceSchemaStore{ctx: ksCtx, cancel: ksCancel}
				store.keyspaceSchemaStoreMap[1] = ks
				schemaClient := client.New(mc, id)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				finished := make(chan error, 1)
				go func() {
					var err error
					if test.tableInfos {
						_, err = schemaClient.GetTableInfos(ctx, common.KeyspaceMeta{ID: 1}, []int64{1}, 100)
					} else {
						_, err = schemaClient.GetAllPhysicalTables(ctx, common.KeyspaceMeta{ID: 1}, 100, config.NewDefaultFilterConfig(), true, false)
					}
					finished <- err
				}()
				waitRequestInStore(t, ks)
				cancel()
				select {
				case err := <-finished:
					require.ErrorIs(t, err, context.Canceled)
				case <-time.After(time.Second):
					t.Fatal("caller did not finish")
				}
				// Caller cancellation only ends the local wait. The existing schema
				// store wait continues until resolved ts advances or the keyspace closes.
				waitRequestInStore(t, ks)
				require.NoError(t, ksCtx.Err(), "canceling a query must not close the keyspace")
				ksCancel()
				synctest.Wait()
				followCtx, followCancel := context.WithTimeout(t.Context(), time.Second)
				defer followCancel()
				_, err := schemaClient.GetTableInfos(followCtx, common.KeyspaceMeta{ID: 7}, []int64{1}, 100)
				require.True(t, errors.ErrKeyspaceNotFound.Equal(err))
			})
		})
	}
}

func TestSchemaStoreDiscoveryDiscardsCanceledResponse(t *testing.T) {
	mc := mock.NewMockMessageCenter(gomock.NewController(t))
	mc.EXPECT().RegisterHandler(messaging.SchemaStoreTopic, gomock.Any())
	mc.EXPECT().DeRegisterHandler(messaging.SchemaStoreTopic).AnyTimes()
	store := newRequestTestStore(t, mc, 1)
	storage := newPersistentStorageForTest(t.TempDir(), []mockDBInfo{{
		dbInfo: &model.DBInfo{ID: 1, Name: ast.NewCIStr("test")},
		tables: []*model.TableInfo{newEligibleTableInfoForTest(100, "t1")},
	}})
	ks := &keyspaceSchemaStore{dataStorage: storage}
	ks.resolvedTs.Store(100)
	store.keyspaceSchemaStoreMap[1] = ks
	// Pause the existing synchronous read after it has opened its snapshot.
	storage.mu.Lock()
	releaseRead := sync.OnceFunc(storage.mu.Unlock)
	t.Cleanup(releaseRead)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	req := &schemastore.GetAllPhysicalTablesRequest{RequestID: 1, Keyspace: schemastore.KeyspaceMeta{ID: 1}, Ts: 100, Filter: schemastore.NewFilterConfig(config.NewDefaultFilterConfig())}
	finished := make(chan struct{})
	go func() {
		store.messageHandler.handleRequest(ctx, "client", req)
		close(finished)
	}()
	require.Eventually(t, func() bool { return storage.db.Metrics().Snapshots.Count > 0 }, time.Second, time.Millisecond)
	cancel()
	// Canceling the handler context does not interrupt storage. Once the read
	// finishes, the handler returns without sending the response.
	releaseRead()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("request handler did not return after the storage read finished")
	}
}

func TestSchemaStoreRequestQueueLimitAndClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mc := mock.NewMockMessageCenter(gomock.NewController(t))
		mc.EXPECT().RegisterHandler(messaging.SchemaStoreTopic, gomock.Any())
		mc.EXPECT().DeRegisterHandler(messaging.SchemaStoreTopic).AnyTimes()
		store := newRequestTestStore(t, mc, 1)
		ctx, cancel := context.WithCancel(t.Context())
		ks := &keyspaceSchemaStore{ctx: ctx, cancel: cancel}
		store.keyspaceSchemaStoreMap[1] = ks
		require.NoError(t, store.messageHandler.handleMessage(t.Context(), messaging.NewSingleTargetMessage("store", messaging.SchemaStoreTopic,
			testRequest(1, 1))))
		waitRequestInStore(t, ks)
		for id := 2; id <= schemaStoreRequestQueueSize+1; id++ {
			require.NoError(t, store.messageHandler.handleMessage(t.Context(), messaging.NewSingleTargetMessage("store", messaging.SchemaStoreTopic,
				testRequest(uint64(id), 1))))
		}
		mc.EXPECT().SendCommand(gomock.Any()).DoAndReturn(func(msg *messaging.TargetMessage) error {
			require.Contains(t, msg.Message[0].(*schemastore.GetTableInfosResponse).Error.Message, "queue is full")
			return nil
		})
		require.NoError(t, store.messageHandler.handleMessage(t.Context(), messaging.NewSingleTargetMessage("store", messaging.SchemaStoreTopic,
			testRequest(1000, 1))))
		mc.EXPECT().SendCommand(gomock.Any()).DoAndReturn(func(msg *messaging.TargetMessage) error {
			resp := msg.Message[0].(*schemastore.GetAllPhysicalTablesResponse)
			require.Equal(t, uint64(1001), resp.RequestID)
			require.Contains(t, resp.Error.Message, "queue is full")
			return nil
		})
		require.NoError(t, store.messageHandler.handleMessage(t.Context(), messaging.NewSingleTargetMessage("store", messaging.SchemaStoreTopic,
			&schemastore.GetAllPhysicalTablesRequest{RequestID: 1001, Keyspace: schemastore.KeyspaceMeta{ID: 1}})))
		require.NoError(t, ctx.Err())
		closed := make(chan error, 1)
		go func() { closed <- store.Close(context.Background()) }()
		select {
		case err := <-closed:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("closing the pool did not release schema requests")
		}
		require.Empty(t, store.messageHandler.requests)
		require.ErrorIs(t, store.messageHandler.submit(schemaRequest{from: "client", message: testRequest(1001, 2)}), context.Canceled)
	})
}

func TestSchemaStoreResponseDelivery(t *testing.T) {
	for _, tt := range []struct {
		name     string
		failures int
		err      error
		attempts int
	}{
		{"congestion", 1, errors.AppError{Type: errors.ErrorTypeMessageCongested}, 2},
		{"temporary connection", 1, errors.AppError{Type: errors.ErrorTypeConnectionNotFound}, 2},
		{"permanent failure", 1, errors.AppError{Type: errors.ErrorTypeTargetMismatch}, 1},
		{"exhausted retries", schemaStoreResponseMaxTries, errors.AppError{Type: errors.ErrorTypeMessageCongested}, schemaStoreResponseMaxTries},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mc := mock.NewMockMessageCenter(gomock.NewController(t))
			handler := &schemaStoreMessageHandler{mc: mc}
			attempts := 0
			mc.EXPECT().SendCommand(gomock.Any()).Times(tt.attempts).DoAndReturn(func(msg *messaging.TargetMessage) error {
				attempts++
				if attempts <= tt.failures {
					return tt.err
				}
				return nil
			})
			err := handler.sendResponse(t.Context(), "client", &schemastore.GetTableInfosResponse{RequestID: 1})
			if tt.failures >= tt.attempts {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSchemaStoreTableBatchSize(t *testing.T) {
	mc := mock.NewMockMessageCenter(gomock.NewController(t))
	mc.EXPECT().RegisterHandler(messaging.SchemaStoreTopic, gomock.Any())
	mc.EXPECT().DeRegisterHandler(messaging.SchemaStoreTopic).AnyTimes()
	store := newRequestTestStore(t, mc, 1)
	storage := newPersistentStorageForTest(t.TempDir(), nil)
	ks := &keyspaceSchemaStore{dataStorage: storage}
	ks.resolvedTs.Store(100)
	store.keyspaceSchemaStoreMap[1] = ks
	for _, id := range []int64{1, 2} {
		versioned := newEmptyVersionedTableInfoStore(id)
		versioned.addInitialTableInfo(common.WrapTableInfo("test", &model.TableInfo{ID: id, Name: ast.NewCIStr("t"), Comment: strings.Repeat("x", 2<<20)}), 1)
		versioned.setTableInfoInitialized()
		storage.tableInfoStoreMap[id] = versioned
	}
	req := testRequest(1, 1)
	req.TableIDs = []int64{1, 2}
	infos, more, err := store.messageHandler.getTableInfosBatch(t.Context(), req)
	require.NoError(t, err)
	require.True(t, more)
	require.Len(t, infos, 1)
	encoded, err := (&schemastore.GetTableInfosResponse{RequestID: 1, TableInfos: infos, More: more}).Marshal()
	require.NoError(t, err)
	require.LessOrEqual(t, len(encoded), schemastore.TableBatchBytes)
	req.TableIDs = []int64{2}
	infos, more, err = store.messageHandler.getTableInfosBatch(t.Context(), req)
	require.NoError(t, err)
	require.False(t, more)
	require.Len(t, infos, 1)
	require.Equal(t, int64(2), infos[0].TableID)
	oversized := newEmptyVersionedTableInfoStore(3)
	oversized.addInitialTableInfo(common.WrapTableInfo("test", &model.TableInfo{ID: 3, Name: ast.NewCIStr("t"), Comment: strings.Repeat("x", 4<<20)}), 1)
	oversized.setTableInfoInitialized()
	storage.tableInfoStoreMap[3] = oversized
	req.TableIDs = []int64{3}
	_, _, err = store.messageHandler.getTableInfosBatch(t.Context(), req)
	require.ErrorContains(t, err, "exceeds the response size limit")
}

func TestSchemaStoreRequests(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	id := node.NewID()
	mc := messaging.NewMessageCenter(ctx, id, config.NewDefaultMessageCenterConfig("127.0.0.1:0"), nil)
	mc.Run(ctx)
	defer mc.Close()

	storage := newPersistentStorageForTest(t.TempDir(), nil)
	require.NoError(t, storage.handleDDLJob(buildCreateSchemaJobForTest(100, "test", 1000)))
	require.NoError(t, storage.handleDDLJob(buildCreateTableJobForTest(100, 200, "t1", 1010)))
	require.NoError(t, storage.handleDDLJob(buildCreateTableJobForTest(100, 201, "t2", 1020)))
	ks := &keyspaceSchemaStore{dataStorage: storage}
	ks.resolvedTs.Store(1020)
	meta := common.KeyspaceMeta{ID: 7, Name: "ks"}
	store := newRequestTestStore(t, mc, schemaStoreRequestWorkers)
	store.keyspaceSchemaStoreMap[meta.ID] = ks
	store.tombstoneKeyspaces = map[uint32]struct{}{8: {}}
	schemaClient := client.New(mc, id)
	_, err := schemaClient.GetTableInfos(ctx, common.KeyspaceMeta{ID: 8}, []int64{200}, 1020)
	require.True(t, errors.ErrKeyspaceNotFound.Equal(err))
	infos, err := schemaClient.GetTableInfos(ctx, meta, []int64{200, 201}, 1020)
	require.NoError(t, err)
	require.Len(t, infos, 2)
	require.Equal(t, int64(200), infos[0].TableName.TableID)
	require.Empty(t, storage.tableRegisteredCount, "bootstrap reads must not pin table registrations")
	cfg := config.NewDefaultFilterConfig()
	cfg.Rules = []string{"TEST.T1"}
	tables, err := schemaClient.GetAllPhysicalTables(ctx, meta, 1020, cfg, false, false)
	require.NoError(t, err)
	require.Len(t, tables, 1)
	require.Equal(t, int64(200), tables[0].TableID)
	require.Equal(t, "test", tables[0].SchemaName)
	require.Equal(t, "t1", tables[0].TableName)
	tables, err = schemaClient.GetAllPhysicalTables(ctx, meta, 1020, cfg, true, false)
	require.NoError(t, err)
	require.Empty(t, tables)
	cfg.Rules = []string{"test.*"}
	tables, err = schemaClient.GetAllPhysicalTables(ctx, meta, 1010, cfg, true, false)
	require.NoError(t, err)
	require.Len(t, tables, 1)
	storage.mu.Lock()
	storage.gcTs = 100
	storage.mu.Unlock()
	_, err = schemaClient.GetAllPhysicalTables(ctx, meta, 0, cfg, true, false)
	require.True(t, errors.ErrSnapshotLostByGC.Equal(err))
	cfg.Rules = []string{"["}
	_, err = schemaClient.GetAllPhysicalTables(ctx, meta, 1020, cfg, true, false)
	require.Error(t, err)
}
