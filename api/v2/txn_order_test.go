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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/ticdc/api/middleware"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/keyspace"
	"github.com/pingcap/ticdc/pkg/server/mock"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestTxnOrderUpdateRejectsPartitionedTables(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	helper.DDL2Job("CREATE TABLE test.regular (id INT PRIMARY KEY)")
	helper.DDL2Job("CREATE TABLE test.partitioned (id INT PRIMARY KEY) PARTITION BY HASH(id) PARTITIONS 2")
	version, err := helper.Storage().CurrentVersion(oracle.GlobalTxnScope)
	require.NoError(t, err)
	previous, _ := appcontext.TryGetService[keyspace.Manager](appcontext.KeyspaceManager)
	defer appcontext.SetService(appcontext.KeyspaceManager, previous)
	manager := keyspace.NewMockManager(gomock.NewController(t))
	manager.EXPECT().GetStorage(gomock.Any(), common.DefaultKeyspaceName).Return(helper.Storage(), nil).AnyTimes()
	appcontext.SetService[keyspace.Manager](appcontext.KeyspaceManager, manager)
	for _, expandFilter := range []bool{false, true} {
		t.Run(map[bool]string{false: "enable ordering", true: "expand table filter"}[expandFilter], func(t *testing.T) {
			cfg := config.GetDefaultReplicaConfig()
			if expandFilter {
				cfg.Filter.Rules = []string{"test.regular"}
				cfg.Sink.MySQLConfig = &config.MySQLConfig{TxnOrderTables: []string{"test.partitioned"}}
			}
			stored := &config.ChangeFeedInfo{
				ChangefeedID: common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName),
				SinkURI:      "mysql://127.0.0.1:1/", State: config.StateStopped, Config: cfg,
			}
			original, err := stored.Marshal()
			require.NoError(t, err)
			co := &resumeNormalCoordinator{info: stored, status: &config.ChangeFeedStatus{CheckpointTs: version.Ver}}
			srv := mock.NewMockServer(gomock.NewController(t))
			srv.EXPECT().GetCoordinator().Return(co, nil)
			h := OpenAPIV2{server: srv}
			request := &ChangefeedConfig{ReplicaConfig: ToAPIReplicaConfig(cfg)}
			request.ReplicaConfig.Filter.Rules = []string{"test.*"}
			request.ReplicaConfig.Sink.MySQLConfig = &MySQLConfig{TxnOrderTables: []string{"test.partitioned"}}
			request.ReplicaConfig.IgnoreIneligibleTable = util.AddressOf(true)
			body, err := json.Marshal(request)
			require.NoError(t, err)
			router := gin.New()
			router.Use(middleware.ErrorHandleMiddleware())
			router.PUT("/changefeeds/:changefeed_id", func(c *gin.Context) {
				c.Set("ctx-keyspace", &keyspacepb.KeyspaceMeta{State: keyspacepb.KeyspaceState_ENABLED})
				h.UpdateChangefeed(c)
			})
			w := httptest.NewRecorder()
			router.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/changefeeds/test", strings.NewReader(string(body))))
			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			require.Contains(t, w.Body.String(), "txn-order-tables does not support partitioned table")
			require.Nil(t, co.updated)
			after, err := stored.Marshal()
			require.NoError(t, err)
			require.Equal(t, original, after)
		})
	}
}
