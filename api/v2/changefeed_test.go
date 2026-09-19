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
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/server"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

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

// Return the same info pointer as the real coordinator, rather than a copy.
type updateTestCoordinator struct {
	server.Coordinator
	info *config.ChangeFeedInfo
}

func (c *updateTestCoordinator) Initialized() bool { return true }

func (c *updateTestCoordinator) GetChangefeed(context.Context, common.ChangeFeedDisplayName) (*config.ChangeFeedInfo, *config.ChangeFeedStatus, error) {
	return c.info, &config.ChangeFeedStatus{}, nil
}

type updateTestServer struct {
	server.Server
	co server.Coordinator
}

func (s *updateTestServer) GetCoordinator() (server.Coordinator, error) { return s.co, nil }

func TestRejectedUpdatePreservesChangefeedInfo(t *testing.T) {
	info := &config.ChangeFeedInfo{
		ChangefeedID: common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName),
		State:        config.StateStopped, StartTs: 10, TargetTs: 20,
		SinkURI: "mysql://root@127.0.0.1:4000/", Config: config.GetDefaultReplicaConfig(),
	}
	before, err := info.MarshalWithTruncation(false)
	require.NoError(t, err)
	h := NewOpenAPIV2(&updateTestServer{co: &updateTestCoordinator{info: info}})
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Set("ctx-keyspace", &keyspacepb.KeyspaceMeta{State: keyspacepb.KeyspaceState_ENABLED})
	c.Params = gin.Params{{Key: "changefeed_id", Value: "test"}}
	// These fields are applied before the forbidden start_ts is checked.
	c.Request = httptest.NewRequest(http.MethodPut, "/api/v2/changefeeds/test", strings.NewReader(
		`{"start_ts":1,"target_ts":30,"sink_uri":"mysql://root@127.0.0.1:5000/","replica_config":{"allow_same_cluster":true}}`))
	c.Request.Header.Set("Content-Type", "application/json")
	h.UpdateChangefeed(c)
	require.NotEmpty(t, c.Errors)
	require.ErrorContains(t, c.Errors.Last().Err, "start_ts can not be updated")
	after, err := info.MarshalWithTruncation(false)
	require.NoError(t, err)
	require.JSONEq(t, before, after)
}
