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

//go:build nextgen

package v2

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/server"
	"github.com/stretchr/testify/require"
)

func TestChangefeedMetadataRoutesAllowDeletedKeyspace(t *testing.T) {
	gin.SetMode(gin.TestMode)
	originalConfig := config.GetGlobalServerConfig()
	t.Cleanup(func() {
		config.StoreGlobalServerConfig(originalConfig)
	})
	cfg := originalConfig.Clone()
	cfg.Security.ClientUserRequired = false
	config.StoreGlobalServerConfig(cfg)

	tests := []struct {
		name   string
		method string
		path   string
		check  func(t *testing.T, coordinator *deletedKeyspaceCoordinator)
	}{
		{
			name:   "get",
			method: http.MethodGet,
			path:   "/api/v2/changefeeds/test?keyspace=deleted-keyspace",
		},
		{
			name:   "list",
			method: http.MethodGet,
			path:   "/api/v2/changefeeds?keyspace=deleted-keyspace",
		},
		{
			name:   "status",
			method: http.MethodGet,
			path:   "/api/v2/changefeeds/test/status?keyspace=deleted-keyspace",
		},
		{
			name:   "pause",
			method: http.MethodPost,
			path:   "/api/v2/changefeeds/test/pause?keyspace=deleted-keyspace",
			check: func(t *testing.T, coordinator *deletedKeyspaceCoordinator) {
				require.True(t, coordinator.pauseCalled)
			},
		},
		{
			name:   "delete",
			method: http.MethodDelete,
			path:   "/api/v2/changefeeds/test?keyspace=deleted-keyspace",
			check: func(t *testing.T, coordinator *deletedKeyspaceCoordinator) {
				require.True(t, coordinator.removeCalled)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coordinator := newDeletedKeyspaceCoordinator()
			apiServer := &deletedKeyspaceServer{coordinator: coordinator}
			router := gin.New()
			RegisterOpenAPIV2Routes(router, NewOpenAPIV2(apiServer))

			req := httptest.NewRequest(tt.method, tt.path, nil)
			resp := httptest.NewRecorder()
			router.ServeHTTP(resp, req)

			require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
			if tt.check != nil {
				tt.check(t, coordinator)
			}
		})
	}
}

type deletedKeyspaceServer struct {
	server.Server
	coordinator server.Coordinator
}

func (s *deletedKeyspaceServer) IsCoordinator() bool {
	return true
}

func (s *deletedKeyspaceServer) GetCoordinator() (server.Coordinator, error) {
	return s.coordinator, nil
}

type deletedKeyspaceCoordinator struct {
	server.Coordinator
	info         *config.ChangeFeedInfo
	status       *config.ChangeFeedStatus
	pauseCalled  bool
	removeCalled bool
}

func newDeletedKeyspaceCoordinator() *deletedKeyspaceCoordinator {
	id := common.NewChangeFeedIDWithName("test", "deleted-keyspace")
	return &deletedKeyspaceCoordinator{
		info: &config.ChangeFeedInfo{
			ChangefeedID: id,
			KeyspaceID:   42,
			Config:       config.GetDefaultReplicaConfig(),
			State:        config.StateStopped,
		},
		status: &config.ChangeFeedStatus{CheckpointTs: 100},
	}
}

func (c *deletedKeyspaceCoordinator) Initialized() bool {
	return true
}

func (c *deletedKeyspaceCoordinator) GetChangefeed(
	_ context.Context,
	_ common.ChangeFeedDisplayName,
) (*config.ChangeFeedInfo, *config.ChangeFeedStatus, error) {
	return c.info, c.status, nil
}

func (c *deletedKeyspaceCoordinator) ListChangefeeds(
	_ context.Context,
	_ string,
) ([]*config.ChangeFeedInfo, []*config.ChangeFeedStatus, error) {
	return []*config.ChangeFeedInfo{c.info}, []*config.ChangeFeedStatus{c.status}, nil
}

func (c *deletedKeyspaceCoordinator) PauseChangefeed(_ context.Context, _ common.ChangeFeedID) error {
	c.pauseCalled = true
	return nil
}

func (c *deletedKeyspaceCoordinator) RemoveChangefeed(_ context.Context, _ common.ChangeFeedID) (uint64, error) {
	c.removeCalled = true
	return c.status.CheckpointTs, nil
}
