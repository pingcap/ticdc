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

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/messaging/mock"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/schemastoreclient"
	"github.com/stretchr/testify/require"
)

func TestSchemaStoreTableInfosResponseDelivery(t *testing.T) {
	congested := errors.AppError{Type: errors.ErrorTypeMessageCongested, Reason: "queue full"}
	tests := []struct {
		name               string
		tableFailures      int
		completionFailures int
		sendError          error
		wantTableAttempts  int
		wantCompleteError  bool
	}{
		{
			name: "retry congested table response", tableFailures: 1, sendError: congested,
			wantTableAttempts: 3,
		},
		{
			name: "retry temporary connection failure", tableFailures: 1,
			sendError:         errors.NewAppError(errors.ErrorTypeConnectionNotFound, "connection not ready"),
			wantTableAttempts: 3,
		},
		{
			name: "retry congested completion", completionFailures: 1, sendError: congested,
			wantTableAttempts: 2,
		},
		{
			name: "report permanent table delivery failure", tableFailures: 1,
			sendError:         errors.NewAppError(errors.ErrorTypeTargetMismatch, "target mismatch"),
			wantTableAttempts: 1, wantCompleteError: true,
		},
		{
			name: "report exhausted table delivery retries", tableFailures: schemaStoreResponseMaxTries,
			sendError: congested, wantTableAttempts: schemaStoreResponseMaxTries, wantCompleteError: true,
		},
		{
			name: "bound completion retries", completionFailures: schemaStoreResponseMaxTries,
			sendError: congested, wantTableAttempts: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := mock.NewMockMessageCenter(gomock.NewController(t))
			// A dropped keyspace produces explicit per-table errors without requiring storage.
			store := &schemaStore{mc: mc, tombstoneKeyspaces: map[uint32]struct{}{7: {}}}
			req := &messaging.SchemaStoreTableInfosRequest{RequestID: 123, KeyspaceID: 7, TableIDs: []int64{1, 2}}
			var completion *messaging.SchemaStoreTableInfosResponse
			var deliveredIDs []int64
			tableAttempts, completionAttempts := 0, 0
			wantCompletionAttempts := min(tt.completionFailures+1, schemaStoreResponseMaxTries)
			mc.EXPECT().SendCommand(gomock.Any()).Times(tt.wantTableAttempts + wantCompletionAttempts).
				DoAndReturn(func(msg *messaging.TargetMessage) error {
					require.Equal(t, node.ID("client"), msg.To)
					require.Equal(t, messaging.SchemaStoreClientTopic, msg.Topic)
					resp := msg.Message[0].(*messaging.SchemaStoreTableInfosResponse)
					require.Equal(t, req.RequestID, resp.RequestID)
					if resp.Done {
						completionAttempts++
						if completionAttempts <= tt.completionFailures {
							return tt.sendError
						}
						completion = resp
						return nil
					}
					tableAttempts++
					if tableAttempts <= tt.tableFailures {
						require.Equal(t, int64(1), resp.TableID)
						return tt.sendError
					}
					require.NotEmpty(t, resp.Error)
					deliveredIDs = append(deliveredIDs, resp.TableID)
					return nil
				})
			store.handleTableInfosRequest(context.Background(), "client", req)
			require.Equal(t, tt.wantTableAttempts, tableAttempts)
			require.Equal(t, wantCompletionAttempts, completionAttempts)
			if tt.completionFailures == schemaStoreResponseMaxTries {
				require.Nil(t, completion)
				return
			}
			require.NotNil(t, completion)
			if tt.wantCompleteError {
				require.Contains(t, completion.Error, tt.sendError.Error())
				require.Empty(t, deliveredIDs)
			} else {
				require.Empty(t, completion.Error)
				require.Equal(t, req.TableIDs, deliveredIDs)
			}
		})
	}
}

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
