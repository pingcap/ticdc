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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/ticdc/maintainer"
	"github.com/pingcap/ticdc/pkg/api"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/liveness"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/server"
	"github.com/pingcap/ticdc/pkg/server/mock"
	"github.com/pingcap/ticdc/pkg/txnutil/gc"
	"github.com/pingcap/ticdc/pkg/util"
<<<<<<< HEAD
=======
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/types"
>>>>>>> ce44c4dde (api,cli: keep credential redaction at display boundaries (#6464))
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"github.com/twmb/franz-go/pkg/kfake"
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
		Id:    common.DefaultKeyspaceID,
		State: keyspacepb.KeyspaceState_ENABLED,
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
	info         *config.ChangeFeedInfo
	status       *config.ChangeFeedStatus
	updated      *config.ChangeFeedInfo
}

func (c *resumeNormalCoordinator) Stop() {}

func (c *resumeNormalCoordinator) Run(ctx context.Context) error { return nil }

func (c *resumeNormalCoordinator) ListChangefeeds(ctx context.Context, keyspace string) ([]*config.ChangeFeedInfo, []*config.ChangeFeedStatus, error) {
	return nil, nil, nil
}

func (c *resumeNormalCoordinator) GetChangefeed(ctx context.Context, changefeedDisplayName common.ChangeFeedDisplayName) (*config.ChangeFeedInfo, *config.ChangeFeedStatus, error) {
	if c.info != nil {
		info, err := c.info.Clone()
		return info, c.status, err
	}
	changefeedID := common.NewChangeFeedIDWithName(changefeedDisplayName.Name, changefeedDisplayName.Keyspace)
	return &config.ChangeFeedInfo{
			ChangefeedID: changefeedID,
			State:        config.StateNormal,
		}, &config.ChangeFeedStatus{
			CheckpointTs: 123,
		}, nil
}

// GetPersistedChangefeedInfo keeps the persisted fake state normal so the
// handler exercises the state guard after reloading backend metadata.
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
	c.updated = change
	return nil
}

func (c *resumeNormalCoordinator) RequestResolvedTsFromLogCoordinator(ctx context.Context, changefeedDisplayName common.ChangeFeedDisplayName) {
}

func (c *resumeNormalCoordinator) DrainNode(ctx context.Context, target node.ID) (int, error) {
	return 0, nil
}

func (c *resumeNormalCoordinator) Initialized() bool { return true }

<<<<<<< HEAD
=======
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

// TestMaskSinkURIForError verifies that error messages mask sensitive sink URI
// fields. It checks both a valid URI with secret query parameters and an invalid
// URI parse error that previously exposed raw credentials.
func TestMaskSinkURIForError(t *testing.T) {
	sinkURI := "kafka://127.0.0.1:9092/topic?protocol=canal-json" +
		"&sasl-user=ticdc&sasl-password=verysecure&secret-access-key=rawsecret"

	maskedURI := util.MaskSensitiveDataInURIForError(sinkURI)
	require.NotContains(t, maskedURI, "verysecure")
	require.NotContains(t, maskedURI, "rawsecret")
	require.Contains(t, maskedURI, "sasl-password=xxxxx")
	require.Contains(t, maskedURI, "secret-access-key=xxxxx")
	require.Contains(t, maskedURI, "sasl-user=ticdc")

	invalidURI := "mysql://root:verysecure@127.0.0.1/%zz"
	require.Equal(t, "<invalid uri>", util.MaskSensitiveDataInURIForError(invalidURI))

	err := genSinkURIInvalidError(invalidURI, mustParseURLError(t, invalidURI))
	require.NotContains(t, err.Error(), "verysecure")
	require.Contains(t, err.Error(), "<invalid uri>")
	require.Contains(t, err.Error(), `parse "<invalid uri>"`)
	require.Contains(t, err.Error(), "invalid URL escape")
}

func TestCfInfoToAPIModelOmitsReplicaCredentials(t *testing.T) {
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Consistent.Storage = new("s3://bucket/redo?secret-access-key=redo-secret-sentinel")
	replicaConfig.Sink.PulsarConfig = &config.PulsarConfig{
		AuthenticationToken: new("pulsar-token-sentinel"), BasicPassword: new("pulsar-password-sentinel"),
		OAuth2: &config.OAuth2{
			OAuth2PrivateKey: "pulsar-key-sentinel",
			OAuth2IssuerURL:  "https://user:pulsar-issuer-secret-sentinel@oauth.example.com",
		},
	}
	replicaConfig.Sink.SchemaRegistry = util.AddressOf(
		"https://registry-user:registry-password-sentinel@registry.example.com?access-key=registry-access-sentinel")
	replicaConfig.Sink.KafkaConfig = &config.KafkaConfig{
		SASLUser:              util.AddressOf("ticdc-user"),
		SASLPassword:          util.AddressOf("plain-password-sentinel"),
		SASLGssAPIPassword:    util.AddressOf("gssapi-password-sentinel"),
		SASLOAuthClientID:     util.AddressOf("oauth-client-id"),
		SASLOAuthClientSecret: util.AddressOf("oauth-secret-sentinel"),
		SASLOAuthTokenURL: util.AddressOf(
			"https://oauth.example.com/token?client_secret=token-url-secret-sentinel&audience=ticdc"),
		Key: util.AddressOf("private-key-sentinel"),
		LargeMessageHandle: &config.LargeMessageHandleConfig{
			ClaimCheckStorageURI: "s3://bucket/prefix?access-key=claim-check-secret-sentinel",
		},
		GlueSchemaRegistryConfig: &config.GlueSchemaRegistryConfig{
			AccessKey:       "glue-access-sentinel",
			SecretAccessKey: "glue-secret-sentinel",
			Token:           "glue-token-sentinel",
		},
	}
	info := &config.ChangeFeedInfo{
		ChangefeedID: common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName),
		SinkURI: "kafka://sink-user:sink-password-sentinel@127.0.0.1:9092/topic" +
			"?protocol=canal-json&sasl-password=uri-sasl-password-sentinel&secret-access-key=uri-secret-sentinel",
		Config: replicaConfig,
	}
	status := &config.ChangeFeedStatus{CheckpointTs: 123}
	original, err := info.Marshal()
	require.NoError(t, err)

	apiInfo := CfInfoToAPIModel(info, status, nil)
	response, err := apiInfo.Marshal()
	require.NoError(t, err)

	require.NotContains(t, response, "sentinel")
	require.Contains(t, apiInfo.SinkURI, "sink-user:xxxxx@")
	require.Contains(t, apiInfo.SinkURI, "sasl-password=xxxxx")
	require.Contains(t, apiInfo.SinkURI, "secret-access-key=xxxxx")
	for _, accept := range []string{"application/json", "application/toml"} {
		t.Run(accept, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(http.MethodGet, "/", nil)
			ctx.Request.Header.Set("Accept", accept)
			respondWithFormat(ctx, http.StatusOK, apiInfo)
			require.Equal(t, http.StatusOK, recorder.Code)
			var decoded ChangeFeedInfo
			if accept == "application/toml" {
				_, err = toml.Decode(recorder.Body.String(), &decoded)
			} else {
				err = json.Unmarshal(recorder.Body.Bytes(), &decoded)
			}
			require.NoError(t, err)
			require.Equal(t, apiInfo.Config.Sink, decoded.Config.Sink)
		})
	}
	// Omitted credentials can be retained by an update without exposing them.
	require.Nil(t, apiInfo.Config.Sink.KafkaConfig.SASLPassword)
	require.Equal(t, "ticdc-user", *apiInfo.Config.Sink.KafkaConfig.SASLUser)

	// The display copy masks credentials without modifying the source config.
	masked, err := apiInfo.CloneWithMaskedSensitiveData()
	require.NoError(t, err)
	output, err := masked.Marshal()
	require.NoError(t, err)
	require.NotContains(t, output, "sentinel")
	require.Nil(t, apiInfo.Config.Sink.KafkaConfig.SASLPassword)
	after, err := info.Marshal()
	require.NoError(t, err)
	require.Equal(t, original, after)
}

func TestUpdateChangefeedValidatesCredentialsBeforePersistence(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	previous, _ := appcontext.TryGetService[keyspace.Manager](appcontext.KeyspaceManager)
	t.Cleanup(func() { appcontext.SetService(appcontext.KeyspaceManager, previous) })
	manager := keyspace.NewMockManager(gomock.NewController(t))
	manager.EXPECT().GetStorage(gomock.Any(), common.DefaultKeyspaceName).Return(store, nil).AnyTimes()
	appcontext.SetService[keyspace.Manager](appcontext.KeyspaceManager, manager)

	for _, tc := range []struct {
		name        string
		password    *string
		brokerPass  string
		useURI      bool
		wantFailure bool
	}{
		{name: "read modify update", brokerPass: "stored-password-sentinel"},
		{name: "new password", password: new("new-password-sentinel"), brokerPass: "new-password-sentinel"},
		{name: "invalid password", password: new("bad-password-sentinel"), brokerPass: "stored-password-sentinel", wantFailure: true},
		{name: "empty password", password: new(""), brokerPass: "stored-password-sentinel", wantFailure: true},
		{name: "literal scalar mask", password: new("******"), brokerPass: "******"},
		{name: "literal URI mask", password: new("xxxxx"), brokerPass: "xxxxx", useURI: true},
		{name: "invalid URI password", password: new("bad-password-sentinel"), brokerPass: "stored-password-sentinel", useURI: true, wantFailure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "test"),
				kfake.EnableSASL(), kfake.Superuser("PLAIN", "alice", tc.brokerPass))
			t.Cleanup(cluster.Close)
			cfg := config.GetDefaultReplicaConfig()
			cfg.Sink.Protocol = new("open-protocol")
			cfg.Sink.KafkaConfig = &config.KafkaConfig{
				SASLUser: new("alice"), SASLPassword: new("stored-password-sentinel"), SASLMechanism: new("PLAIN"),
			}
			cfg.Consistent.Storage = new("s3://bucket/redo?secret-access-key=redo-secret-sentinel")
			stored := &config.ChangeFeedInfo{
				ChangefeedID: common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName),
				SinkURI:      "kafka://" + cluster.ListenAddrs()[0] + "/test?kafka-client=franz&required-acks=1",
				State:        config.StateStopped, Config: cfg,
			}
			original, err := stored.Marshal()
			require.NoError(t, err)
			co := &resumeNormalCoordinator{info: stored, status: &config.ChangeFeedStatus{CheckpointTs: 1}}
			srv := mock.NewMockServer(gomock.NewController(t))
			srv.EXPECT().GetCoordinator().Return(co, nil)
			srv.EXPECT().GetPdClient().Return(&gc.MockPDClient{ClusterID: 1}).AnyTimes()
			h := OpenAPIV2{server: srv}
			request := &ChangefeedConfig{}
			if tc.useURI {
				request.SinkURI = stored.SinkURI + "&sasl-user=alice&sasl-mechanism=PLAIN&sasl-password=" + *tc.password
			} else {
				request.ReplicaConfig = CfInfoToAPIModel(stored, co.status, nil).Config
				request.ReplicaConfig.MemoryQuota = new(uint64(2097152))
				if tc.password != nil {
					request.ReplicaConfig.Sink.KafkaConfig.SASLPassword = tc.password
				}
			}
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
			if tc.wantFailure {
				require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
				require.Nil(t, co.updated)
			} else {
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
				require.NotNil(t, co.updated)
				require.Equal(t, cfg.Consistent.Storage, co.updated.Config.Consistent.Storage)
				if tc.useURI {
					require.Equal(t, request.SinkURI, co.updated.SinkURI)
				} else {
					require.Equal(t, tc.brokerPass, *co.updated.Config.Sink.KafkaConfig.SASLPassword)
				}
			}
			require.NotContains(t, w.Body.String(), "sentinel")
			after, err := stored.Marshal()
			require.NoError(t, err)
			require.Equal(t, original, after)
		})
	}
}

func mustParseURLError(t *testing.T, rawURL string) error {
	t.Helper()

	_, err := url.Parse(rawURL)
	require.Error(t, err)
	return err
}

>>>>>>> ce44c4dde (api,cli: keep credential redaction at display boundaries (#6464))
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

func mustParseURLError(t *testing.T, rawURL string) error {
	t.Helper()

	_, err := url.Parse(rawURL)
	require.Error(t, err)
	return err
}
