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
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/ticdc/maintainer"
	"github.com/pingcap/ticdc/pkg/api"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/liveness"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/server"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

// TestValidateResumeChangefeedState covers the API-side guard that runs before
// resume GC safepoint/barrier setup. Running states must fail fast, while states
// that are actually stopped can proceed to the remaining resume validation.
func TestValidateResumeChangefeedState(t *testing.T) {
	for _, state := range []config.FeedState{config.StateStopped, config.StateFailed, config.StateFinished} {
		require.NoError(t, validateResumeChangefeedState(state))
	}

	for _, state := range []config.FeedState{config.StateNormal, config.StateWarning, config.StatePending} {
		err := validateResumeChangefeedState(state)
		require.True(t, cerror.ErrChangefeedUpdateRefused.Equal(err))
		require.Contains(t, err.Error(), string(state))
	}
}

// TestResumeChangefeedRejectsNormalBeforeGC covers the HTTP resume regression:
// a normal changefeed must fail before the handler requests PD/etcd clients for
// GC safepoint/barrier setup or calls the coordinator resume path.
func TestResumeChangefeedRejectsNormalBeforeGC(t *testing.T) {
	gin.SetMode(gin.TestMode)

	co := &resumeNormalCoordinator{}
	srv := &resumeNormalServer{coordinator: co}
	h := &OpenAPIV2{server: srv}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v2/changefeeds/test/resume?keyspace=default", nil)
	c.Params = gin.Params{{Key: api.APIOpVarChangefeedID, Value: "test"}}
	c.Set("ctx-keyspace", &keyspacepb.KeyspaceMeta{
		Id:    common.DefaultKeyspaceID,
		State: keyspacepb.KeyspaceState_ENABLED,
	})

	h.ResumeChangefeed(c)

	require.Len(t, c.Errors, 1)
	require.True(t, cerror.ErrChangefeedUpdateRefused.Equal(c.Errors.Last().Err))
	require.False(t, srv.pdClientRequested)
	require.False(t, srv.etcdClientRequested)
	require.False(t, co.resumeCalled)
}

type resumeNormalServer struct {
	coordinator         server.Coordinator
	pdClientRequested   bool
	etcdClientRequested bool
}

func (s *resumeNormalServer) Run(ctx context.Context) error { return nil }

func (s *resumeNormalServer) Close() {}

func (s *resumeNormalServer) SelfInfo() (*node.Info, error) { return nil, nil }

func (s *resumeNormalServer) Liveness() liveness.Liveness { return liveness.CaptureAlive }

func (s *resumeNormalServer) GetCoordinator() (server.Coordinator, error) {
	return s.coordinator, nil
}

func (s *resumeNormalServer) IsCoordinator() bool { return true }

func (s *resumeNormalServer) GetCoordinatorInfo(ctx context.Context) (*node.Info, error) {
	return nil, nil
}

func (s *resumeNormalServer) GetPdClient() pd.Client {
	s.pdClientRequested = true
	return nil
}

func (s *resumeNormalServer) GetEtcdClient() etcd.CDCEtcdClient {
	s.etcdClientRequested = true
	return nil
}

func (s *resumeNormalServer) GetMaintainerManager() *maintainer.Manager { return nil }

type resumeNormalCoordinator struct {
	resumeCalled bool
}

func (c *resumeNormalCoordinator) Stop() {}

func (c *resumeNormalCoordinator) Run(ctx context.Context) error { return nil }

func (c *resumeNormalCoordinator) ListChangefeeds(ctx context.Context, keyspace string) ([]*config.ChangeFeedInfo, []*config.ChangeFeedStatus, error) {
	return nil, nil, nil
}

func (c *resumeNormalCoordinator) GetChangefeed(ctx context.Context, changefeedDisplayName common.ChangeFeedDisplayName) (*config.ChangeFeedInfo, *config.ChangeFeedStatus, error) {
	changefeedID := common.NewChangeFeedIDWithName(changefeedDisplayName.Name, changefeedDisplayName.Keyspace)
	return &config.ChangeFeedInfo{
			ChangefeedID: changefeedID,
			State:        config.StateNormal,
		}, &config.ChangeFeedStatus{
			CheckpointTs: 123,
		}, nil
}

func (c *resumeNormalCoordinator) CreateChangefeed(ctx context.Context, info *config.ChangeFeedInfo) error {
	return nil
}

func (c *resumeNormalCoordinator) RemoveChangefeed(ctx context.Context, id common.ChangeFeedID) (uint64, error) {
	return 0, nil
}

func (c *resumeNormalCoordinator) PauseChangefeed(ctx context.Context, id common.ChangeFeedID) error {
	return nil
}

func (c *resumeNormalCoordinator) ResumeChangefeed(ctx context.Context, id common.ChangeFeedID, newCheckpointTs uint64, overwriteCheckpointTs bool) error {
	c.resumeCalled = true
	return nil
}

func (c *resumeNormalCoordinator) UpdateChangefeed(ctx context.Context, change *config.ChangeFeedInfo) error {
	return nil
}

func (c *resumeNormalCoordinator) RequestResolvedTsFromLogCoordinator(ctx context.Context, changefeedDisplayName common.ChangeFeedDisplayName) {
}

func (c *resumeNormalCoordinator) Initialized() bool { return true }
