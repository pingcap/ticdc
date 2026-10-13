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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v2

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/api/middleware"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/server/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetChangeFeedCoordinatorError(t *testing.T) {
	ctrl := gomock.NewController(t)
	srv := mock.NewMockServer(ctrl)
	expectedErr := errors.ErrOwnerNotFound.GenWithStackByArgs()
	srv.EXPECT().GetCoordinatorInfo(gomock.Any()).Return(nil, expectedErr)
	h := NewOpenAPIV2(srv)

	_, err := h.getChangeFeed(context.Background(), "default", "test")
	require.ErrorIs(t, err, expectedErr)
}

func TestChangefeedAPIsIgnoreRequestHost(t *testing.T) {
	gin.SetMode(gin.TestMode)
	originalConfig := config.GetGlobalServerConfig()
	config.StoreGlobalServerConfig(config.GetDefaultServerConfig())
	t.Cleanup(func() { config.StoreGlobalServerConfig(originalConfig) })

	for _, tc := range []struct {
		path    string
		method  string
		handler func(*OpenAPIV2, *gin.Context)
	}{
		{"tables", http.MethodGet, (*OpenAPIV2).ListTables},
		{"get_dispatcher_count", http.MethodGet, (*OpenAPIV2).getDispatcherCount},
		{"move_table", http.MethodPost, (*OpenAPIV2).MoveTable},
		{"move_split_table", http.MethodPost, (*OpenAPIV2).MoveSplitTable},
		{"split_table_by_region_count", http.MethodPost, (*OpenAPIV2).SplitTableByRegionCount},
		{"merge_table", http.MethodPost, (*OpenAPIV2).MergeTable},
	} {
		t.Run(tc.path, func(t *testing.T) {
			const query = "keyspace=default&tableID=1&targetNodeID=target"
			const response = `{"forwarded":true}`
			path := "/api/v2/changefeeds/test/" + tc.path
			maintainer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, tc.method, r.Method)
				assert.Equal(t, path, r.URL.Path)
				assert.Equal(t, query, r.URL.RawQuery)
				assert.Equal(t, "capture", r.Header.Get("TiCDC-ForwardFrom"))
				_, _ = w.Write([]byte(response))
			}))
			t.Cleanup(maintainer.Close)

			coordinator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodGet, r.Method)
				assert.Equal(t, "/api/v2/changefeeds/test", r.URL.Path)
				assert.Equal(t, "default", r.URL.Query().Get("keyspace"))
				cfg := GetDefaultReplicaConfig()
				*cfg.Scheduler.EnableTableAcrossNodes = true
				assert.NoError(t, json.NewEncoder(w).Encode(ChangeFeedInfo{
					ID:             "test",
					Keyspace:       "default",
					MaintainerAddr: strings.TrimPrefix(maintainer.URL, "http://"),
					Config:         cfg,
				}))
			}))
			t.Cleanup(coordinator.Close)

			ctrl := gomock.NewController(t)
			srv := mock.NewMockServer(ctrl)
			srv.EXPECT().GetCoordinatorInfo(gomock.Any()).Return(&node.Info{
				AdvertiseAddr: strings.TrimPrefix(coordinator.URL, "http://"),
			}, nil)
			srv.EXPECT().SelfInfo().Return(&node.Info{
				ID:            "capture",
				AdvertiseAddr: "capture:8300",
			}, nil)
			h := NewOpenAPIV2(srv)
			router := gin.New()
			router.Use(middleware.ErrorHandleMiddleware())
			router.Handle(tc.method, "/api/v2/changefeeds/:changefeed_id/"+tc.path, func(c *gin.Context) {
				tc.handler(&h, c)
			})

			req := httptest.NewRequest(tc.method, path+"?"+query, nil)
			req.Host = "example.invalid"
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.JSONEq(t, response, w.Body.String())
		})
	}
}
