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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/server"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

func TestChangefeedConfigMasksKafkaCredentials(t *testing.T) {
	cfg := changefeedConfig{
		SinkURI: "kafka://user:sink-password-sentinel@127.0.0.1:9092/topic" +
			"?sasl-password=uri-password-sentinel&secret-access-key=uri-secret-sentinel",
		SinkConfig: &config.SinkConfig{
			SchemaRegistry: util.AddressOf("https://registry.example.com?access-key=registry-secret-sentinel"),
			KafkaConfig: &config.KafkaConfig{
				SASLPassword:          util.AddressOf("sasl-password-sentinel"),
				SASLGssAPIPassword:    util.AddressOf("gssapi-password-sentinel"),
				SASLOAuthClientSecret: util.AddressOf("oauth-secret-sentinel"),
				Key:                   util.AddressOf("private-key-sentinel"),
			},
		},
	}

	cfg.SinkURI = util.MaskSensitiveDataInURI(cfg.SinkURI)
	cfg.SinkConfig.MaskSensitiveData()
	encoded, err := json.Marshal(cfg)
	require.NoError(t, err)
	output := string(encoded)
	for _, secret := range []string{
		"sink-password-sentinel",
		"uri-password-sentinel",
		"uri-secret-sentinel",
		"registry-secret-sentinel",
		"sasl-password-sentinel",
		"gssapi-password-sentinel",
		"oauth-secret-sentinel",
		"private-key-sentinel",
	} {
		require.NotContains(t, output, secret)
	}
	require.Contains(t, output, "xxxxx")
	require.Contains(t, output, "******")
}

func TestDrainCaptureRouteRequiresAuthentication(t *testing.T) {
	configureDrainCaptureAuth(t, true)

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

	resp := serveDrainCaptureRequest(newDrainCaptureTestRouter(apiServer))

	require.Equal(t, http.StatusUnauthorized, resp.Code)
	require.False(t, coordinator.drainCalled)
}

func TestDrainCaptureRouteReachesHandlerWhenAuthenticationDisabled(t *testing.T) {
	configureDrainCaptureAuth(t, false)

	coordinator := &mockCoordinator{remaining: 7}
	apiServer := &mockAPIServer{
		isCoordinator: true,
		selfInfo:      &node.Info{ID: node.ID("owner")},
		coordinator:   coordinator,
	}

	resp := serveDrainCaptureRequest(newDrainCaptureTestRouter(apiServer))

	require.Equal(t, http.StatusAccepted, resp.Code)
	require.JSONEq(t, `{"current_table_count":7}`, resp.Body.String())
	require.True(t, coordinator.drainCalled)
	require.Equal(t, node.ID("target"), coordinator.target)
}

func configureDrainCaptureAuth(t *testing.T, required bool) {
	t.Helper()

	gin.SetMode(gin.TestMode)

	originalConfig := config.GetGlobalServerConfig()
	t.Cleanup(func() {
		config.StoreGlobalServerConfig(originalConfig)
	})

	cfg := originalConfig.Clone()
	cfg.Security.ClientUserRequired = required
	if required {
		cfg.Security.ClientAllowedUser = []string{"alice"}
	} else {
		cfg.Security.ClientAllowedUser = nil
	}
	config.StoreGlobalServerConfig(cfg)
}

func newDrainCaptureTestRouter(apiServer server.Server) *gin.Engine {
	router := gin.New()
	RegisterOpenAPIV1Routes(router, NewOpenAPIV1(apiServer))
	return router
}

func serveDrainCaptureRequest(router http.Handler) *httptest.ResponseRecorder {
	req := httptest.NewRequest(
		http.MethodPut,
		"/api/v1/captures/drain",
		bytes.NewBufferString(`{"capture_id":"target"}`),
	)
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)
	return resp
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
