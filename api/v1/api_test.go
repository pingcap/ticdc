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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/keyspace"
	"github.com/pingcap/ticdc/pkg/server"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestLogLevelRouteRequiresAuthentication(t *testing.T) {
	configureAuth(t, true)

	originalLogLevel := log.GetLevel()
	t.Cleanup(func() { log.SetLevel(originalLogLevel) })
	log.SetLevel(zapcore.InfoLevel)

	ctrl := gomock.NewController(t)
	mockEtcdClient := etcd.NewMockCDCEtcdClient(ctrl)
	mockEtcdClient.EXPECT().GetEtcdClient().Return(nil)
	apiServer := &mockAPIServer{etcdClient: mockEtcdClient}
	path := "/api/v1/log"
	if kerneltype.IsNextGen() {
		previous, _ := appcontext.TryGetService[keyspace.Manager](appcontext.KeyspaceManager)
		t.Cleanup(func() { appcontext.SetService(appcontext.KeyspaceManager, previous) })
		keyspaceManager := keyspace.NewMockManager(ctrl)
		keyspaceManager.EXPECT().LoadKeyspace(gomock.Any(), "auth-keyspace").Return(
			&keyspacepb.KeyspaceMeta{
				Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 42},
				Name:     "auth-keyspace",
			}, nil,
		)
		appcontext.SetService[keyspace.Manager](appcontext.KeyspaceManager, keyspaceManager)
		path += "?keyspace=auth-keyspace"
	}

	router := gin.New()
	RegisterOpenAPIV1Routes(router, NewOpenAPIV1(apiServer))
	request := httptest.NewRequest(
		http.MethodPost,
		path,
		bytes.NewBufferString(`{"log_level":"error"}`),
	)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	require.Equal(t, http.StatusUnauthorized, response.Code, response.Body.String())
	require.Equal(t, zapcore.InfoLevel, log.GetLevel())
}

func configureAuth(t *testing.T, required bool) {
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

type mockAPIServer struct {
	server.Server
	etcdClient etcd.CDCEtcdClient
}

func (m *mockAPIServer) GetEtcdClient() etcd.CDCEtcdClient {
	return m.etcdClient
}
