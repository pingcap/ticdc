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
	"net/url"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/ticdc/maintainer"
	"github.com/pingcap/ticdc/pkg/api"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/liveness"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/server"
	"github.com/pingcap/ticdc/pkg/util"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
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
		require.True(t, errors.ErrChangefeedUpdateRefused.Equal(err))
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
		Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: common.DefaultKeyspaceID},
		State:    keyspacepb.KeyspaceState_ENABLED,
	})

	h.ResumeChangefeed(c)

	require.Len(t, c.Errors, 1)
	require.True(t, errors.ErrChangefeedUpdateRefused.Equal(c.Errors.Last().Err))
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

func (c *resumeNormalCoordinator) GetPersistedChangefeedInfo(ctx context.Context, id common.ChangeFeedID) (*config.ChangeFeedInfo, error) {
	return &config.ChangeFeedInfo{
		ChangefeedID: id,
		State:        config.StateNormal,
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

func (c *resumeNormalCoordinator) DrainNode(ctx context.Context, target node.ID) (int, error) {
	return 0, nil
}

func (c *resumeNormalCoordinator) Initialized() bool { return true }

// TestMaskSinkURIForError verifies that error messages mask sensitive sink URI
// fields. It checks both a valid URI with secret query parameters and an invalid
// URI parse error that previously exposed raw credentials.
func TestMaskSinkURIForError(t *testing.T) {
	sinkURI := "kafka://127.0.0.1:9092/topic?protocol=canal-json" +
		"&sasl-user=ticdc&sasl-password=verysecure&secret-access-key=rawsecret"

	maskedURI := maskSinkURIForError(sinkURI)
	require.NotContains(t, maskedURI, "verysecure")
	require.NotContains(t, maskedURI, "rawsecret")
	require.Contains(t, maskedURI, "sasl-password=xxxxx")
	require.Contains(t, maskedURI, "secret-access-key=xxxxx")
	require.Contains(t, maskedURI, "sasl-user=ticdc")

	invalidURI := "mysql://root:verysecure@127.0.0.1/%zz"
	require.Equal(t, "<invalid uri>", maskSinkURIForError(invalidURI))

	err := genSinkURIInvalidError(invalidURI, mustParseURLError(t, invalidURI))
	require.NotContains(t, err.Error(), "verysecure")
	require.Contains(t, err.Error(), "<invalid uri>")
	require.Contains(t, err.Error(), `parse "<invalid uri>"`)
	require.Contains(t, err.Error(), "invalid URL escape")
}

func TestCfInfoToAPIModelMasksSensitiveData(t *testing.T) {
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.KafkaConfig = &config.KafkaConfig{
		SASLUser:              util.AddressOf("ticdc-user"),
		SASLPassword:          util.AddressOf("plain-password-sentinel"),
		SASLGssAPIPassword:    util.AddressOf("gssapi-password-sentinel"),
		SASLOAuthClientID:     util.AddressOf("oauth-client-id"),
		SASLOAuthClientSecret: util.AddressOf("oauth-secret-sentinel"),
		SASLOAuthTokenURL: util.AddressOf(
			"https://oauth.example.com/token?client_secret=token-url-secret-sentinel&audience=ticdc"),
	}
	info := &config.ChangeFeedInfo{
		ChangefeedID: common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName),
		SinkURI: "kafka://sink-user:sink-password-sentinel@127.0.0.1:9092/topic" +
			"?protocol=canal-json&secret-access-key=uri-secret-sentinel",
		Config: replicaConfig,
	}
	status := &config.ChangeFeedStatus{CheckpointTs: 123}

	apiInfo := CfInfoToAPIModel(info, status, nil)
	response, err := apiInfo.Marshal()
	require.NoError(t, err)

	for _, secret := range []string{
		"sink-password-sentinel",
		"uri-secret-sentinel",
		"plain-password-sentinel",
		"gssapi-password-sentinel",
		"oauth-secret-sentinel",
		"token-url-secret-sentinel",
	} {
		require.NotContains(t, response, secret)
	}
	require.Contains(t, apiInfo.SinkURI, "sink-user:xxxxx@")
	require.Contains(t, apiInfo.SinkURI, "secret-access-key=xxxxx")
	require.Equal(t, "******", *apiInfo.Config.Sink.KafkaConfig.SASLPassword)
	require.Equal(t, "******", *apiInfo.Config.Sink.KafkaConfig.SASLGssAPIPassword)
	require.Equal(t, "******", *apiInfo.Config.Sink.KafkaConfig.SASLOAuthClientSecret)
	require.Equal(t, "ticdc-user", *apiInfo.Config.Sink.KafkaConfig.SASLUser)
	require.Equal(t, "oauth-client-id", *apiInfo.Config.Sink.KafkaConfig.SASLOAuthClientID)

	// Building an API response must not modify the in-memory changefeed config.
	require.Equal(t, "plain-password-sentinel", *info.Config.Sink.KafkaConfig.SASLPassword)
	require.Equal(t, "oauth-secret-sentinel", *info.Config.Sink.KafkaConfig.SASLOAuthClientSecret)
	require.Contains(t, info.SinkURI, "sink-password-sentinel")
}

func TestRestoreMaskedChangefeedCredentials(t *testing.T) {
	original := config.GetDefaultReplicaConfig()
	original.Sink.SchemaRegistry = util.AddressOf(
		"https://registry.example.com?access-key=registry-access-sentinel")
	original.Sink.KafkaConfig = &config.KafkaConfig{
		SASLPassword:          util.AddressOf("plain-password-sentinel"),
		SASLGssAPIPassword:    util.AddressOf("gssapi-password-sentinel"),
		SASLOAuthClientSecret: util.AddressOf("oauth-secret-sentinel"),
		SASLOAuthTokenURL: util.AddressOf(
			"https://oauth.example.com/token?client_secret=token-url-secret-sentinel"),
		Key: util.AddressOf("/path/to/private-key"),
		GlueSchemaRegistryConfig: &config.GlueSchemaRegistryConfig{
			AccessKey:       "glue-access-sentinel",
			SecretAccessKey: "glue-secret-sentinel",
			Token:           "glue-token-sentinel",
		},
	}
	original.Consistent.Storage = util.AddressOf(
		"s3://bucket/prefix?access-key=storage-access-sentinel")

	masked := original.Clone()
	masked.MaskSensitiveData()
	updated := ToAPIReplicaConfig(masked).ToInternalReplicaConfig()
	restoreMaskedSensitiveData(updated, original)

	require.Equal(t, original.Sink.SchemaRegistry, updated.Sink.SchemaRegistry)
	require.Equal(t, original.Sink.KafkaConfig, updated.Sink.KafkaConfig)
	require.Equal(t, original.Consistent.Storage, updated.Consistent.Storage)

	updatedWithURIChange := ToAPIReplicaConfig(masked).ToInternalReplicaConfig()
	updatedWithURIChange.Sink.KafkaConfig.SASLOAuthTokenURL = util.AddressOf(
		*updatedWithURIChange.Sink.KafkaConfig.SASLOAuthTokenURL + "&audience=new-audience")
	restoreMaskedSensitiveData(updatedWithURIChange, original)
	require.Contains(t, *updatedWithURIChange.Sink.KafkaConfig.SASLOAuthTokenURL,
		"client_secret=token-url-secret-sentinel")
	require.Contains(t, *updatedWithURIChange.Sink.KafkaConfig.SASLOAuthTokenURL,
		"audience=new-audience")

	updated.Sink.KafkaConfig.SASLPassword = util.AddressOf("replacement-password")
	restoreMaskedSensitiveData(updated, original)
	require.Equal(t, "replacement-password", *updated.Sink.KafkaConfig.SASLPassword)

	originalSinkURI := "kafka://user:sink-password-sentinel@127.0.0.1:9092/topic?secret=uri-secret-sentinel"
	maskedSinkURI := util.MaskSensitiveDataInURI(originalSinkURI)
	restoredSinkURI, changed := restoreMaskedSinkURI(maskedSinkURI, originalSinkURI)
	require.False(t, changed)
	require.Equal(t, originalSinkURI, restoredSinkURI)
	maskedSinkURI = strings.Replace(maskedSinkURI, "/topic?", "/new-topic?", 1) + "&protocol=avro"
	restoredSinkURI, changed = restoreMaskedSinkURI(maskedSinkURI, originalSinkURI)
	require.True(t, changed)
	require.Contains(t, restoredSinkURI, "sink-password-sentinel")
	require.Contains(t, restoredSinkURI, "secret=uri-secret-sentinel")
	require.Contains(t, restoredSinkURI, "/new-topic")
	require.Contains(t, restoredSinkURI, "protocol=avro")

	newEndpointURI := strings.Replace(maskedSinkURI, "127.0.0.1:9092", "new-broker:9092", 1)
	restoredSinkURI, changed = restoreMaskedSinkURI(newEndpointURI, originalSinkURI)
	require.True(t, changed)
	require.NotContains(t, restoredSinkURI, "sink-password-sentinel")
	require.NotContains(t, restoredSinkURI, "uri-secret-sentinel")

	replacementSinkURI := "kafka://127.0.0.1:9092/new-topic"
	restoredSinkURI, changed = restoreMaskedSinkURI(replacementSinkURI, originalSinkURI)
	require.True(t, changed)
	require.Equal(t, replacementSinkURI, restoredSinkURI)
}

func mustParseURLError(t *testing.T, rawURL string) error {
	t.Helper()

	_, err := url.Parse(rawURL)
	require.Error(t, err)
	return err
}

// TestVerifyRouteConflict covers route conflict detection for eligible and
// ineligible source tables. It exercises the safe cases first, then verifies
// that conflicts report both the shared target table and conflicting sources.
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

func TestVerifyTablesForSinkValidatesStorageColumnSelectors(t *testing.T) {
	t.Parallel()

	replicaCfg := config.GetDefaultReplicaConfig()
	replicaCfg.Sink.ColumnSelectors = []*config.ColumnSelector{
		{Matcher: []string{"test.t"}, Columns: []string{"name"}},
	}
	tableInfos := []*common.TableInfo{newTableInfoWithPrimaryKeyForTest()}

	err := verifyTablesForSink(replicaCfg, config.FileScheme, "", config.ProtocolCanalJSON, tableInfos)
	require.Error(t, err)
	require.True(t, errors.ErrColumnSelectorFailed.Equal(err))

	replicaCfg.Sink.ColumnSelectors[0].Columns = []string{"id", "name"}
	require.NoError(t, verifyTablesForSink(replicaCfg, config.FileScheme, "", config.ProtocolCanalJSON, tableInfos))
}

func newTableInfoWithPrimaryKeyForTest() *common.TableInfo {
	idFieldType := types.NewFieldType(mysql.TypeLong)
	idFieldType.AddFlag(mysql.PriKeyFlag | mysql.NotNullFlag)

	return common.WrapTableInfo("test", &timodel.TableInfo{
		ID:         1,
		Name:       ast.NewCIStr("t"),
		PKIsHandle: true,
		Columns: []*timodel.ColumnInfo{
			{
				ID:        1,
				Name:      ast.NewCIStr("id"),
				FieldType: *idFieldType,
				State:     timodel.StatePublic,
			},
			{
				ID:        2,
				Name:      ast.NewCIStr("name"),
				FieldType: *types.NewFieldType(mysql.TypeVarchar),
				State:     timodel.StatePublic,
			},
		},
	})
}
