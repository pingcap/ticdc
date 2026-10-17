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

package v2

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/keyspace"
	"github.com/pingcap/ticdc/pkg/server/mock"
	"github.com/stretchr/testify/require"
)

func TestTableInspectionRoutesRequireAuthentication(t *testing.T) {
	gin.SetMode(gin.TestMode)

	originalConfig := config.GetGlobalServerConfig()
	t.Cleanup(func() { config.StoreGlobalServerConfig(originalConfig) })
	cfg := originalConfig.Clone()
	cfg.Security.ClientUserRequired = true
	cfg.Security.ClientAllowedUser = []string{"alice"}
	config.StoreGlobalServerConfig(cfg)

	paths := []string{
		"/api/v2/verify_table?keyspace=" + common.DefaultKeyspaceName,
		"/api/v2/get_all_tables?keyspace=" + common.DefaultKeyspaceName,
	}

	ctrl := gomock.NewController(t)
	apiServer := mock.NewMockServer(ctrl)
	etcdClient := etcd.NewMockCDCEtcdClient(ctrl)
	apiServer.EXPECT().GetEtcdClient().Return(etcdClient).Times(len(paths))
	etcdClient.EXPECT().GetEtcdClient().Return(nil).Times(len(paths))

	if kerneltype.IsNextGen() {
		previous, _ := appcontext.TryGetService[keyspace.Manager](appcontext.KeyspaceManager)
		t.Cleanup(func() { appcontext.SetService(appcontext.KeyspaceManager, previous) })
		keyspaceManager := keyspace.NewMockManager(ctrl)
		keyspaceManager.EXPECT().LoadKeyspace(gomock.Any(), common.DefaultKeyspaceName).Return(
			&keyspacepb.KeyspaceMeta{
				Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: common.DefaultKeyspaceID},
				Name:     common.DefaultKeyspaceName,
			}, nil,
		).Times(len(paths))
		appcontext.SetService[keyspace.Manager](appcontext.KeyspaceManager, keyspaceManager)
	}

	router := gin.New()
	RegisterOpenAPIV2Routes(router, NewOpenAPIV2(apiServer))

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()

			router.ServeHTTP(response, request)

			require.Equal(t, http.StatusUnauthorized, response.Code, response.Body.String())
		})
	}
}
