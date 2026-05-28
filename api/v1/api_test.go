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

package v1

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/server"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

func TestDrainCaptureRouteRequiresAuthentication(t *testing.T) {
	gin.SetMode(gin.TestMode)

	originalConfig := config.GetGlobalServerConfig()
	defer config.StoreGlobalServerConfig(originalConfig)

	cfg := originalConfig.Clone()
	cfg.Security.ClientUserRequired = true
	cfg.Security.ClientAllowedUser = []string{"alice"}
	config.StoreGlobalServerConfig(cfg)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockEtcdClient := etcd.NewMockCDCEtcdClient(ctrl)
	mockEtcdClient.EXPECT().GetEtcdClient().Return(nil)

	coordinator := &mockCoordinator{remaining: 3}
	apiServer := &mockAPIServer{
		isCoordinator: true,
		selfInfo:      &node.Info{ID: node.ID("owner")},
		coordinator:   coordinator,
		etcdClient:    mockEtcdClient,
	}

	router := gin.New()
	RegisterOpenAPIV1Routes(router, NewOpenAPIV1(apiServer))

	req := httptest.NewRequest(
		http.MethodPut,
		"/api/v1/captures/drain",
		bytes.NewBufferString(`{"capture_id":"target"}`),
	)
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	require.Equal(t, http.StatusUnauthorized, resp.Code)
	require.False(t, coordinator.drainCalled)
}

func TestDrainCaptureRouteReachesHandlerWhenAuthenticationDisabled(t *testing.T) {
	gin.SetMode(gin.TestMode)

	originalConfig := config.GetGlobalServerConfig()
	defer config.StoreGlobalServerConfig(originalConfig)

	cfg := originalConfig.Clone()
	cfg.Security.ClientUserRequired = false
	cfg.Security.ClientAllowedUser = nil
	config.StoreGlobalServerConfig(cfg)

	coordinator := &mockCoordinator{remaining: 7}
	apiServer := &mockAPIServer{
		isCoordinator: true,
		selfInfo:      &node.Info{ID: node.ID("owner")},
		coordinator:   coordinator,
	}

	router := gin.New()
	RegisterOpenAPIV1Routes(router, NewOpenAPIV1(apiServer))

	req := httptest.NewRequest(
		http.MethodPut,
		"/api/v1/captures/drain",
		bytes.NewBufferString(`{"capture_id":"target"}`),
	)
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	require.Equal(t, http.StatusAccepted, resp.Code)
	require.JSONEq(t, `{"current_table_count":7}`, resp.Body.String())
	require.True(t, coordinator.drainCalled)
	require.Equal(t, node.ID("target"), coordinator.target)
}

type mockAPIServer struct {
	server.Server

	isCoordinator bool
	selfInfo      *node.Info
	coordinator   server.Coordinator
	etcdClient    etcd.CDCEtcdClient
}

func (m *mockAPIServer) IsCoordinator() bool {
	return m.isCoordinator
}

func (m *mockAPIServer) SelfInfo() (*node.Info, error) {
	return m.selfInfo, nil
}

func (m *mockAPIServer) GetCoordinator() (server.Coordinator, error) {
	return m.coordinator, nil
}

func (m *mockAPIServer) GetEtcdClient() etcd.CDCEtcdClient {
	return m.etcdClient
}

func (m *mockAPIServer) GetPdClient() pd.Client {
	return nil
}

type mockCoordinator struct {
	server.Coordinator

	remaining   int
	drainCalled bool
	target      node.ID
}

func (m *mockCoordinator) DrainNode(_ context.Context, target node.ID) (int, error) {
	m.drainCalled = true
	m.target = target
	return m.remaining, nil
}
