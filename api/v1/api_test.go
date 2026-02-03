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
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/ticdc/maintainer"
	"github.com/pingcap/ticdc/pkg/api"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/server"
	pd "github.com/tikv/pd/client"
)

type fakeServer struct {
	coordinator server.Coordinator
}

func (s *fakeServer) Run(context.Context) error { return nil }
func (s *fakeServer) Close(context.Context)     {}

func (s *fakeServer) SelfInfo() (*node.Info, error) { return node.NewInfo("127.0.0.1:0", ""), nil }
func (s *fakeServer) Liveness() api.Liveness        { return api.LivenessCaptureAlive }

func (s *fakeServer) GetCoordinator() (server.Coordinator, error) { return s.coordinator, nil }
func (s *fakeServer) IsCoordinator() bool                         { return true }

func (s *fakeServer) GetCoordinatorInfo(context.Context) (*node.Info, error) {
	return node.NewInfo("127.0.0.1:0", ""), nil
}

func (s *fakeServer) GetPdClient() pd.Client            { return nil }
func (s *fakeServer) GetEtcdClient() etcd.CDCEtcdClient { return nil }

func (s *fakeServer) GetMaintainerManager() *maintainer.Manager { return nil }

type fakeCoordinator struct {
	remaining int
}

func (c *fakeCoordinator) Stop()                     {}
func (c *fakeCoordinator) Run(context.Context) error { return nil }
func (c *fakeCoordinator) ListChangefeeds(context.Context, string) ([]*config.ChangeFeedInfo, []*config.ChangeFeedStatus, error) {
	return nil, nil, nil
}

func (c *fakeCoordinator) GetChangefeed(context.Context, common.ChangeFeedDisplayName) (*config.ChangeFeedInfo, *config.ChangeFeedStatus, error) {
	return nil, nil, nil
}
func (c *fakeCoordinator) CreateChangefeed(context.Context, *config.ChangeFeedInfo) error { return nil }
func (c *fakeCoordinator) RemoveChangefeed(context.Context, common.ChangeFeedID) (uint64, error) {
	return 0, nil
}
func (c *fakeCoordinator) PauseChangefeed(context.Context, common.ChangeFeedID) error { return nil }
func (c *fakeCoordinator) ResumeChangefeed(context.Context, common.ChangeFeedID, uint64, bool) error {
	return nil
}
func (c *fakeCoordinator) UpdateChangefeed(context.Context, *config.ChangeFeedInfo) error { return nil }
func (c *fakeCoordinator) RequestResolvedTsFromLogCoordinator(context.Context, common.ChangeFeedDisplayName) {
}
func (c *fakeCoordinator) Initialized() bool { return true }

func (c *fakeCoordinator) DrainNode(node.ID) int { return c.remaining }

type fakeCoordinatorWithoutDrain struct{}

func (c *fakeCoordinatorWithoutDrain) Stop()                     {}
func (c *fakeCoordinatorWithoutDrain) Run(context.Context) error { return nil }
func (c *fakeCoordinatorWithoutDrain) ListChangefeeds(context.Context, string) ([]*config.ChangeFeedInfo, []*config.ChangeFeedStatus, error) {
	return nil, nil, nil
}

func (c *fakeCoordinatorWithoutDrain) GetChangefeed(context.Context, common.ChangeFeedDisplayName) (*config.ChangeFeedInfo, *config.ChangeFeedStatus, error) {
	return nil, nil, nil
}

func (c *fakeCoordinatorWithoutDrain) CreateChangefeed(context.Context, *config.ChangeFeedInfo) error {
	return nil
}

func (c *fakeCoordinatorWithoutDrain) RemoveChangefeed(context.Context, common.ChangeFeedID) (uint64, error) {
	return 0, nil
}

func (c *fakeCoordinatorWithoutDrain) PauseChangefeed(context.Context, common.ChangeFeedID) error {
	return nil
}

func (c *fakeCoordinatorWithoutDrain) ResumeChangefeed(context.Context, common.ChangeFeedID, uint64, bool) error {
	return nil
}

func (c *fakeCoordinatorWithoutDrain) UpdateChangefeed(context.Context, *config.ChangeFeedInfo) error {
	return nil
}

func (c *fakeCoordinatorWithoutDrain) RequestResolvedTsFromLogCoordinator(context.Context, common.ChangeFeedDisplayName) {
}
func (c *fakeCoordinatorWithoutDrain) Initialized() bool { return true }

func TestDrainCaptureReturnsRemaining(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	s := &fakeServer{coordinator: &fakeCoordinator{remaining: 7}}
	RegisterOpenAPIV1Routes(r, NewOpenAPIV1(s))

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/api/v1/captures/drain", strings.NewReader(`{"capture_id":"node-1"}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d, body=%s", w.Code, http.StatusAccepted, w.Body.String())
	}
	var resp drainCaptureResp
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response failed: %v, body=%s", err, w.Body.String())
	}
	if resp.CurrentTableCount != 7 {
		t.Fatalf("current_table_count = %d, want %d", resp.CurrentTableCount, 7)
	}
}

func TestDrainCaptureUnsupportedCoordinator(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	s := &fakeServer{coordinator: &fakeCoordinatorWithoutDrain{}}
	RegisterOpenAPIV1Routes(r, NewOpenAPIV1(s))

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/api/v1/captures/drain", strings.NewReader(`{"capture_id":"node-1"}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d, body=%s", w.Code, http.StatusInternalServerError, w.Body.String())
	}
}
