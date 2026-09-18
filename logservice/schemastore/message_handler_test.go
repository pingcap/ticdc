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
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/keyspace"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/messaging/mock"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/schemastore/client"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/store/mockstore"
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

func testRequest(id uint64, operation messaging.SchemaStoreOperation, keyspace uint32) *messaging.SchemaStoreRequest {
	return &messaging.SchemaStoreRequest{
		RequestID: id, Operation: operation, Keyspace: common.KeyspaceMeta{ID: keyspace},
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
			id := msg.Message[0].(*messaging.SchemaStoreResponse).RequestID
			responses <- id
			if id == 1 {
				<-release
			}
			return nil
		})
		require.NoError(t, store.messageHandler.handleMessage(t.Context(), messaging.NewSingleTargetMessage("store", messaging.SchemaStoreTopic,
			testRequest(1, messaging.SchemaStoreRegisterKeyspace, 7))))
		select {
		case id := <-responses:
			require.Equal(t, uint64(1), id)
		case <-time.After(time.Second):
			t.Fatal("first request did not run")
		}
		require.NoError(t, store.messageHandler.handleMessage(t.Context(), messaging.NewSingleTargetMessage("store", messaging.SchemaStoreTopic,
			testRequest(2, messaging.SchemaStoreGetTableInfos, 7))))
		// Expire only the older queued request while the sole worker is busy.
		// Fake time advances after admission, so submission cannot race expiry.
		synctest.Wait()
		require.Empty(t, responses)
		time.Sleep(messaging.SchemaStoreRequestTimeout / 2)
		require.NoError(t, store.messageHandler.handleMessage(t.Context(), messaging.NewSingleTargetMessage("store", messaging.SchemaStoreTopic,
			testRequest(3, messaging.SchemaStoreGetTableInfos, 7))))
		time.Sleep(messaging.SchemaStoreRequestTimeout / 2)
		synctest.Wait()
		require.Empty(t, responses)
		require.NoError(t, store.messageHandler.handleMessage(t.Context(), messaging.NewSingleTargetMessage("store", messaging.SchemaStoreTopic,
			testRequest(4, messaging.SchemaStoreGetTableInfos, 7))))
		releaseRequest()
		for _, expectedID := range []uint64{3, 4} {
			select {
			case id := <-responses:
				require.Equal(t, expectedID, id, "unexpired requests must execute in FIFO order")
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
			require.Equal(t, uint64(1), msg.Message[0].(*messaging.SchemaStoreResponse).RequestID)
			close(started)
			<-release
			return nil
		})
		require.NoError(t, store.messageHandler.submit(t.Context(), "client",
			testRequest(1, messaging.SchemaStoreRegisterKeyspace, 7)))
		<-started
		require.NoError(t, store.messageHandler.submit(t.Context(), "client",
			testRequest(2, messaging.SchemaStoreGetTableInfos, 7)))
		// Submissions racing with shutdown must finish without depending on the
		// blocked worker. Accepted requests are discarded when dispatch exits.
		const concurrentRequests = 16
		results := make(chan error, concurrentRequests)
		var submitters sync.WaitGroup
		for i := range concurrentRequests {
			submitters.Go(func() {
				results <- store.messageHandler.submit(t.Context(), "client",
					testRequest(uint64(i+3), messaging.SchemaStoreGetTableInfos, 7))
			})
		}
		closed := make(chan error, 1)
		go func() { closed <- store.Close(t.Context()) }()
		synctest.Wait()
		select {
		case <-store.messageHandler.stopped:
		default:
			t.Fatal("queued requests were not released while the worker was busy")
		}
		submitters.Wait()
		for range concurrentRequests {
			if err := <-results; err != nil {
				require.ErrorIs(t, err, context.Canceled)
			}
		}
		require.Empty(t, closed, "close must wait for the running worker")
		require.ErrorIs(t, store.messageHandler.submit(t.Context(), "client",
			testRequest(1000, messaging.SchemaStoreGetTableInfos, 7)), context.Canceled)
		releaseWorker()
		require.NoError(t, <-closed)
	})
}

func TestSchemaStoreCallerCancellation(t *testing.T) {
	for _, test := range []struct {
		name      string
		operation messaging.SchemaStoreOperation
	}{
		{"table query", messaging.SchemaStoreGetTableInfos},
		{"table discovery", messaging.SchemaStoreGetAllPhysicalTables},
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
					if test.operation == messaging.SchemaStoreGetTableInfos {
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
				require.True(t, errors.ErrKeyspaceNotFound.Equal(schemaClient.RegisterKeyspace(followCtx, common.KeyspaceMeta{ID: 7})))
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
	req := testRequest(1, messaging.SchemaStoreGetAllPhysicalTables, 1)
	req.Filter = config.NewDefaultFilterConfig()
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
			testRequest(1, messaging.SchemaStoreGetTableInfos, 1))))
		waitRequestInStore(t, ks)
		for id := 2; id <= schemaStoreMaxPendingRequests; id++ {
			require.NoError(t, store.messageHandler.handleMessage(t.Context(), messaging.NewSingleTargetMessage("store", messaging.SchemaStoreTopic,
				testRequest(uint64(id), messaging.SchemaStoreGetTableInfos, 1))))
		}
		mc.EXPECT().SendCommand(gomock.Any()).DoAndReturn(func(msg *messaging.TargetMessage) error {
			require.Contains(t, msg.Message[0].(*messaging.SchemaStoreResponse).Error, "queue is full")
			return nil
		})
		require.NoError(t, store.messageHandler.handleMessage(t.Context(), messaging.NewSingleTargetMessage("store", messaging.SchemaStoreTopic,
			testRequest(1000, messaging.SchemaStoreGetTableInfos, 1))))
		// Expiration frees queue capacity even while the running storage call
		// remains blocked. All expired requests must be removed from the queue.
		time.Sleep(messaging.SchemaStoreRequestTimeout)
		synctest.Wait()
		for id := 2; id <= schemaStoreMaxPendingRequests; id++ {
			require.NoError(t, store.messageHandler.submit(t.Context(), "client",
				testRequest(uint64(id), messaging.SchemaStoreGetTableInfos, 1)))
		}
		require.ErrorContains(t, store.messageHandler.submit(t.Context(), "client",
			testRequest(1000, messaging.SchemaStoreGetTableInfos, 1)), "queue is full")
		require.NoError(t, ctx.Err())
		closed := make(chan error, 1)
		go func() { closed <- store.Close(context.Background()) }()
		select {
		case err := <-closed:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("closing the pool did not release schema requests")
		}
		require.ErrorIs(t, store.messageHandler.submit(t.Context(), "client", testRequest(1001, messaging.SchemaStoreRegisterKeyspace, 2)), context.Canceled)
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
			err := handler.sendResponse(t.Context(), "client", &messaging.SchemaStoreResponse{RequestID: 1})
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
	req := testRequest(1, messaging.SchemaStoreGetTableInfos, 1)
	req.TableIDs = []int64{1, 2}
	infos, more, err := store.messageHandler.getTableInfosBatch(t.Context(), req)
	require.NoError(t, err)
	require.True(t, more)
	require.Len(t, infos, 1)
	encoded, err := (&messaging.SchemaStoreResponse{RequestID: 1, TableInfos: infos, More: more}).Marshal()
	require.NoError(t, err)
	require.LessOrEqual(t, len(encoded), messaging.SchemaStoreTableBatchBytes)
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	require.NoError(t, schemaClient.RegisterKeyspace(ctx, meta))
	require.True(t, errors.ErrKeyspaceNotFound.Equal(schemaClient.RegisterKeyspace(ctx, common.KeyspaceMeta{ID: 8})))
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

// Request registration always uses the service lifetime. Canceling a request
// does not change storage initialization or directly registered keyspaces.
func TestSchemaStoreRegistrationLifetime(t *testing.T) {
	for _, test := range []struct {
		name       string
		cancelInit bool
		direct     bool
	}{
		{name: "successful request registration"},
		{name: "request canceled during registration", cancelInit: true},
		{name: "direct registration retains caller lifetime", direct: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mc := mock.NewMockMessageCenter(ctrl)
			mc.EXPECT().RegisterHandler(messaging.SchemaStoreTopic, gomock.Any())
			mc.EXPECT().DeRegisterHandler(messaging.SchemaStoreTopic).AnyTimes()
			store := newRequestTestStore(t, mc, 1)
			store.root = t.TempDir()
			meta := common.KeyspaceMeta{ID: 42, Name: "test"}
			store.pdClock = pdutil.NewClock4Test()
			pdCli, _ := newMockGCServiceClientForSchemaStoreGC(t)
			store.pdCli = pdCli
			kvStore, err := mockstore.NewMockStore()
			require.NoError(t, err)
			defer func() { require.NoError(t, kvStore.Close()) }()
			manager := keyspace.NewMockManager(ctrl)
			appcontext.SetService(appcontext.KeyspaceManager, manager)
			appcontext.SetService(appcontext.SubscriptionClient, newTrackingSubscriptionClient())
			requestCtx, cancelRequest := context.WithCancel(t.Context())
			defer cancelRequest()
			manager.EXPECT().GetStorage(gomock.Any(), meta.Name).DoAndReturn(func(context.Context, string) (kv.Storage, error) {
				if test.cancelInit {
					cancelRequest()
				}
				return kvStore, nil
			})
			pdCli.UpdateServiceGCSafePointFunc = func(ctx context.Context, _ string, _ int64, _ uint64) (uint64, error) {
				if err := ctx.Err(); err != nil {
					return 0, err
				}
				return 100, nil
			}
			if test.direct {
				require.NoError(t, store.RegisterKeyspace(requestCtx, meta))
			} else {
				if !test.cancelInit {
					mc.EXPECT().SendCommand(gomock.Any()).DoAndReturn(func(msg *messaging.TargetMessage) error {
						resp := msg.Message[0].(*messaging.SchemaStoreResponse)
						require.Equal(t, uint64(1), resp.RequestID)
						require.Empty(t, resp.Error)
						return nil
					})
				}
				store.messageHandler.handleRequest(requestCtx, "client", &messaging.SchemaStoreRequest{
					RequestID: 1, Operation: messaging.SchemaStoreRegisterKeyspace, Keyspace: meta,
				})
			}
			require.Contains(t, store.keyspaceSchemaStoreMap, meta.ID)
			ks := store.keyspaceSchemaStoreMap[meta.ID]
			if test.direct {
				store.messageHandler.cancel()
				require.NoError(t, ks.ctx.Err(), "the request service must not own a directly registered keyspace")
				cancelRequest()
				require.ErrorIs(t, ks.ctx.Err(), context.Canceled)
				require.ErrorIs(t, ks.dataStorage.ctx.Err(), context.Canceled)
				require.NoError(t, store.Close(context.Background()))
				return
			}
			cancelRequest()
			require.NoError(t, ks.ctx.Err())
			require.NoError(t, ks.dataStorage.ctx.Err())
			acquired, err := store.acquireKeyspaceSchemaStore(meta)
			require.NoError(t, err)
			require.Same(t, ks, acquired)
			acquired.release()
			store.messageHandler.cancel()
			require.ErrorIs(t, ks.ctx.Err(), context.Canceled)
			require.NoError(t, store.Close(context.Background()))
			require.ErrorIs(t, ks.ctx.Err(), context.Canceled)
		})
	}
}
