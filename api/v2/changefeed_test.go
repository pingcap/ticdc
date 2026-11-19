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
	"github.com/golang/mock/gomock"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/ticdc/api/middleware"
	"github.com/pingcap/ticdc/pkg/api"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/keyspace"
	"github.com/pingcap/ticdc/pkg/server"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestDeleteMissingChangefeedRequiresAuthentication(t *testing.T) {
	gin.SetMode(gin.TestMode)

	originalConfig := config.GetGlobalServerConfig()
	t.Cleanup(func() {
		config.StoreGlobalServerConfig(originalConfig)
	})
	cfg := originalConfig.Clone()
	cfg.Security.ClientUserRequired = true
	cfg.Security.ClientAllowedUser = []string{"alice"}
	config.StoreGlobalServerConfig(cfg)

	ctrl := gomock.NewController(t)
	etcdClient := etcd.NewMockCDCEtcdClient(ctrl)
	etcdClient.EXPECT().GetEtcdClient().Return(nil)

	coordinator := &deleteMissingCoordinator{}
	handler := &OpenAPIV2{server: &deleteMissingServer{
		coordinator: coordinator,
		etcdClient:  etcdClient,
	}}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(
		http.MethodDelete,
		"/api/v2/changefeeds/missing?keyspace=test",
		nil,
	)
	c.Params = gin.Params{{Key: api.APIOpVarChangefeedID, Value: "missing"}}

	handler.DeleteChangefeed(c)

	require.Equal(t, http.StatusUnauthorized, w.Code)
	require.False(t, coordinator.removeCalled)
}

func TestLoadKeyspaceInContext(t *testing.T) {
	if !kerneltype.IsNextGen() {
		t.Skip("keyspace authentication context is only needed in next-gen")
	}

	ctrl := gomock.NewController(t)
	keyspaceManager := keyspace.NewMockManager(ctrl)
	keyspaceManager.EXPECT().LoadKeyspace(gomock.Any(), "test").Return(&keyspacepb.KeyspaceMeta{
		Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 1},
		Name:     "test",
	}, nil)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodDelete, "/api/v2/changefeeds/missing?keyspace=test", nil)

	loadKeyspaceInContext(c, keyspaceManager)

	require.Equal(t, uint32(1), middleware.GetKeyspaceFromContext(c).GetId())
}

type deleteMissingServer struct {
	server.Server
	coordinator server.Coordinator
	etcdClient  etcd.CDCEtcdClient
}

func (s *deleteMissingServer) GetCoordinator() (server.Coordinator, error) {
	return s.coordinator, nil
}

func (s *deleteMissingServer) GetEtcdClient() etcd.CDCEtcdClient {
	return s.etcdClient
}

type deleteMissingCoordinator struct {
	server.Coordinator
	removeCalled bool
}

func (c *deleteMissingCoordinator) Initialized() bool {
	return true
}

func (c *deleteMissingCoordinator) GetChangefeed(
	_ context.Context,
	changefeedDisplayName common.ChangeFeedDisplayName,
) (*config.ChangeFeedInfo, *config.ChangeFeedStatus, error) {
	return nil, nil, errors.ErrChangeFeedNotExists.GenWithStackByArgs(changefeedDisplayName.String())
}

func (c *deleteMissingCoordinator) RemoveChangefeed(
	_ context.Context,
	_ common.ChangeFeedID,
) (uint64, error) {
	c.removeCalled = true
	return 0, nil
}

func TestVerifyRouteConflict(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangeFeedIDWithName("test-changefeed", common.DefaultKeyspaceName)
	replicaCfg := config.GetDefaultReplicaConfig()
	replicaCfg.Sink.DispatchRules = []*config.DispatchRule{
		{Matcher: []string{"db1.*"}, TargetSchema: "archive", TargetTable: "{table}"},
		{Matcher: []string{"db2.*"}, TargetSchema: "archive", TargetTable: "{table}"},
	}

	eligibleTables := []common.TableName{{Schema: "db1", Table: "orders"}}
	ineligibleTables := []common.TableName{{Schema: "db2", Table: "orders"}}

	replicaCfg.ForceReplicate = util.AddressOf(false)
	replicaCfg.IgnoreIneligibleTable = util.AddressOf(true)
	require.NoError(t, verifyRouteConflict(changefeedID, eligibleTables, ineligibleTables, replicaCfg))

	replicaCfg.IgnoreIneligibleTable = util.AddressOf(false)
	require.NoError(t, verifyRouteConflict(changefeedID, eligibleTables, ineligibleTables, replicaCfg))

	err := verifyRouteConflict(
		changefeedID,
		[]common.TableName{{Schema: "db1", Table: "orders"}, {Schema: "db2", Table: "orders"}},
		ineligibleTables,
		replicaCfg,
	)
	require.Error(t, err)
	require.True(t, errors.ErrTableRouteConflict.Equal(err))

	replicaCfg.ForceReplicate = util.AddressOf(true)
	err = verifyRouteConflict(changefeedID, eligibleTables, ineligibleTables, replicaCfg)
	require.Error(t, err)
	require.True(t, errors.ErrTableRouteConflict.Equal(err))
	require.Contains(t, err.Error(), "target `archive`.`orders`")
	require.Contains(t, err.Error(), "source `db1`.`orders`")
	require.Contains(t, err.Error(), "source `db2`.`orders`")

	replicaCfg.ForceReplicate = util.AddressOf(false)
	replicaCfg.Sink.DispatchRules = []*config.DispatchRule{
		{Matcher: []string{"db2.*"}, TargetSchema: "db1", TargetTable: "{table}"},
	}
	err = verifyRouteConflict(
		changefeedID,
		[]common.TableName{{Schema: "db1", Table: "orders"}, {Schema: "db2", Table: "orders"}},
		nil,
		replicaCfg,
	)
	require.Error(t, err)
	require.True(t, errors.ErrTableRouteConflict.Equal(err))
	require.Contains(t, err.Error(), "target `db1`.`orders`")
	require.Contains(t, err.Error(), "source `db1`.`orders`")
	require.Contains(t, err.Error(), "source `db2`.`orders`")
}

func TestVerifyRouteConflictCaseSensitive(t *testing.T) {
	for _, tc := range []struct {
		name          string
		caseSensitive *bool
	}{
		{name: "unset"},
		{name: "insensitive", caseSensitive: util.AddressOf(false)},
		{name: "sensitive", caseSensitive: util.AddressOf(true)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.GetDefaultReplicaConfig()
			cfg.CaseSensitive = tc.caseSensitive
			cfg.Sink.DispatchRules = []*config.DispatchRule{{Matcher: []string{"Sales.*"}, TargetSchema: "archive"}}
			cfg = ToAPIReplicaConfig(cfg).ToInternalReplicaConfig()
			runtimeCfg := (&config.ChangeFeedInfo{Config: cfg}).ToChangefeedConfig()
			require.Equal(t, util.GetOrZero(tc.caseSensitive), runtimeCfg.CaseSensitive)
			err := verifyRouteConflict(common.NewChangefeedID4Test("test", tc.name),
				[]common.TableName{{Schema: "sales", Table: "orders"}, {Schema: "archive", Table: "orders"}}, nil, cfg)
			if util.GetOrZero(tc.caseSensitive) {
				require.NoError(t, err)
			} else {
				require.True(t, errors.ErrTableRouteConflict.Equal(err), "%v", err)
			}
		})
	}
}

func TestVerifyTablesForSinkCaseSensitive(t *testing.T) {
	idType := types.NewFieldType(mysql.TypeLong)
	idType.AddFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
	table := common.WrapTableInfo("sales", &model.TableInfo{
		ID: 1, Name: ast.NewCIStr("orders"), PKIsHandle: true,
		Columns: []*model.ColumnInfo{{ID: 1, Name: ast.NewCIStr("id"), State: model.StatePublic, FieldType: *idType}},
	})
	for _, tc := range []struct {
		name          string
		caseSensitive *bool
	}{
		{name: "unset"},
		{name: "insensitive", caseSensitive: util.AddressOf(false)},
		{name: "sensitive", caseSensitive: util.AddressOf(true)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, scheme := range []string{config.FileScheme, config.KafkaScheme, config.PulsarScheme} {
				t.Run(scheme, func(t *testing.T) {
					cfg := config.GetDefaultReplicaConfig()
					cfg.CaseSensitive = tc.caseSensitive
					cfg.Sink.ColumnSelectors = []*config.ColumnSelector{{Matcher: []string{"Sales.*"}, Columns: []string{"*", "!id"}}}
					cfg = ToAPIReplicaConfig(cfg).ToInternalReplicaConfig()
					err := verifyTablesForSink(cfg, scheme, "default-topic", config.ProtocolCanalJSON, []*common.TableInfo{table})
					if util.GetOrZero(tc.caseSensitive) {
						require.NoError(t, err)
					} else {
						require.True(t, errors.ErrColumnSelectorFailed.Equal(err), "%v", err)
					}
					if !config.IsMQScheme(scheme) {
						return
					}
					cfg.Sink.ColumnSelectors = nil
					cfg.Sink.DispatchRules = []*config.DispatchRule{{Matcher: []string{"Sales.*"}, PartitionRule: "index-value", IndexName: "missing_index"}}
					err = verifyTablesForSink(cfg, scheme, "default-topic", config.ProtocolCanalJSON, []*common.TableInfo{table})
					if util.GetOrZero(tc.caseSensitive) {
						require.NoError(t, err)
					} else {
						require.True(t, errors.ErrDispatcherFailed.Equal(err), "%v", err)
					}
				})
			}
		})
	}
}

func TestRouteMatcherValidation(t *testing.T) {
	changefeedID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	for _, tc := range []struct {
		name       string
		eligible   []common.TableName
		ineligible []common.TableName
	}{
		{name: "empty"},
		{name: "eligible", eligible: []common.TableName{{Schema: "sales", Table: "orders"}}},
		{name: "ineligible", ineligible: []common.TableName{{Schema: "sales", Table: "orders"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, forceReplicate := range []bool{false, true} {
				cfg := config.GetDefaultReplicaConfig()
				cfg.ForceReplicate = util.AddressOf(forceReplicate)
				cfg.Sink.DispatchRules = []*config.DispatchRule{{
					Matcher: []string{"["}, TargetSchema: "archive",
				}}
				err := verifyRouteConflict(changefeedID, tc.eligible, tc.ineligible, cfg)
				code, ok := errors.RFCCode(err)
				require.True(t, ok)
				require.Equal(t, errors.ErrInvalidTableRoutingRule.RFCCode(), code)

				cfg.Sink.DispatchRules[0].Matcher = []string{"sales.*"}
				require.NoError(t, verifyRouteConflict(changefeedID, tc.eligible, tc.ineligible, cfg))
			}
		})
	}
}
