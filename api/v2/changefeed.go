// Copyright 2022 PingCAP, Inc.
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
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/api/middleware"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/pkg/api"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/keyspace"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/txnutil/gc"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/version"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// CreateChangefeed handles create changefeed request,
// it returns the changefeed's changefeedInfo that it just created
// CreateChangefeed creates a changefeed
// @Summary Create changefeed
// @Description create a new changefeed
// @Tags changefeed,v2
// @Accept json
// @Produce json
// @Param changefeed body ChangefeedConfig true "changefeed config"
// @Param keyspace query string false "default"
// @Success 200 {object} ChangeFeedInfo
// @Failure 500,400 {object} common.HTTPError
// @Router	/api/v2/changefeeds [post]
func (h *OpenAPIV2) CreateChangefeed(c *gin.Context) {
	ctx := c.Request.Context()
	cfg := &ChangefeedConfig{ReplicaConfig: GetDefaultReplicaConfig()}

	if err := c.BindJSON(&cfg); err != nil {
		_ = c.Error(errors.WrapError(errors.ErrAPIInvalidParam, err))
		return
	}

	// verify sinkURI
	if cfg.SinkURI == "" {
		_ = c.Error(errors.ErrSinkURIInvalid.GenWithStackByArgs(
			"sink_uri is empty, cannot create a changefeed without sink_uri"))
		return
	}

	keyspaceName := GetKeyspaceValueWithDefault(c)

	var changefeedID common.ChangeFeedID
	if cfg.ID == "" {
		changefeedID = common.NewChangefeedID(keyspaceName)
	} else {
		changefeedID = common.NewChangeFeedIDWithName(cfg.ID, keyspaceName)
	}
	// verify changefeedID
	if err := common.ValidateChangefeedID(changefeedID.Name()); err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack(
			"invalid changefeed_id: %s", cfg.ID))
		return
	}

	// We use the keyspace in the query parameter
	cfg.Keyspace = keyspaceName

	// verify changefeed keyspace
	if err := common.ValidateKeyspace(changefeedID.Keyspace()); err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack(
			"invalid keyspace: %s", cfg.ID))
		return
	}

	keyspaceMeta := middleware.GetKeyspaceFromContext(c)

	if keyspaceMeta.State != keyspacepb.KeyspaceState_ENABLED {
		c.IndentedJSON(http.StatusBadRequest, errors.ErrAPIInvalidParam)
		c.Abort()
		return
	}

	co, err := h.server.GetCoordinator()
	if err != nil {
		_ = c.Error(err)
		return
	}

	ok, err := isBootstrapped(co)
	if err != nil || !ok {
		_ = c.Error(err)
		return
	}

	_, status, err := co.GetChangefeed(ctx, common.NewChangeFeedDisplayName(cfg.ID, cfg.Keyspace))
	if err != nil && errors.ErrChangeFeedNotExists.NotEqual(err) {
		_ = c.Error(err)
		return
	}
	if status != nil {
		err = errors.ErrChangeFeedAlreadyExists.GenWithStackByArgs(cfg.ID)
		_ = c.Error(err)
		return
	}

	ts, logical, err := h.server.GetPdClient().GetTS(ctx)
	if err != nil {
		_ = c.Error(errors.ErrPDEtcdAPIError.GenWithStackByArgs("fail to get ts from pd client"))
		return
	}
	currentTSO := oracle.ComposeTS(ts, logical)
	// verify start ts
	if cfg.StartTs == 0 {
		cfg.StartTs = currentTSO
	} else if cfg.StartTs > currentTSO {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack(
			"invalid start-ts %v, larger than current tso %v", cfg.StartTs, currentTSO))
		return
	}
	// Ensure the start ts is valid in the next 3600 seconds, aka 1 hour
	const ensureTTL = 60 * 60
	createGcServiceID := h.server.GetEtcdClient().GetEnsureGCServiceID(gc.EnsureGCServiceCreating)
	if err = gc.EnsureChangefeedStartTsSafety(
		ctx,
		h.server.GetPdClient(),
		createGcServiceID,
		keyspaceMeta.Id,
		changefeedID,
		ensureTTL, cfg.StartTs); err != nil {
		if !errors.ErrStartTsBeforeGC.Equal(err) {
			_ = c.Error(errors.ErrPDEtcdAPIError.Wrap(err))
			return
		}
		_ = c.Error(err)
		return
	}

	// verify target ts
	if cfg.TargetTs > 0 && cfg.TargetTs <= cfg.StartTs {
		_ = c.Error(errors.ErrTargetTsBeforeStartTs.GenWithStackByArgs(
			cfg.TargetTs, cfg.StartTs))
		return
	}

	// fill replicaConfig
	replicaCfg := cfg.ReplicaConfig.ToInternalReplicaConfig()

	// verify replicaConfig
	sinkURIParsed, err := url.Parse(cfg.SinkURI)
	if err != nil {
		_ = c.Error(errors.WrapError(errors.ErrSinkURIInvalid, err, cfg.SinkURI))
		return
	}
	err = replicaCfg.ValidateAndAdjust(sinkURIParsed)
	if err != nil {
		_ = c.Error(errors.WrapError(errors.ErrInvalidReplicaConfig, err))
		return
	}

	scheme := sinkURIParsed.Scheme
	topic := ""
	if config.IsMQScheme(scheme) {
		topic, err = helper.GetTopic(sinkURIParsed)
		if err != nil {
			_ = c.Error(errors.WrapError(errors.ErrSinkURIInvalid, err, cfg.SinkURI))
			return
		}
	}
	protocol, _ := config.ParseSinkProtocolFromString(util.GetOrZero(replicaCfg.Sink.Protocol))

	keyspaceManager := appcontext.GetService[keyspace.Manager](appcontext.KeyspaceManager)
	kvStorage, err := keyspaceManager.GetStorage(ctx, keyspaceName)
	if err != nil {
		_ = c.Error(err)
		return
	}

	schemaStore := appcontext.GetService[schemastore.SchemaStore](appcontext.SchemaStore)
	// The ctx's lifecycle is the same as the HTTP request.
	// The schema store may use the context to fetch database information asynchronously.
	// Therefore, we cannot use the context of the HTTP request.
	// We create a new context here.
	schemaCxt := context.Background()
	if err := schemaStore.RegisterKeyspace(schemaCxt, common.KeyspaceMeta{
		ID:   keyspaceMeta.Id,
		Name: keyspaceMeta.Name,
	}); err != nil {
		_ = c.Error(err)
		return
	}

	ineligibleTables, _, err := getVerifiedTables(ctx, replicaCfg, kvStorage, cfg.StartTs, scheme, topic, protocol)
	if err != nil {
		_ = c.Error(err)
		return
	}
	if !replicaCfg.ForceReplicate && !cfg.ReplicaConfig.IgnoreIneligibleTable {
		if len(ineligibleTables) != 0 {
			_ = c.Error(errors.ErrTableIneligible.GenWithStackByArgs(ineligibleTables))
			return
		}
	}

	pdClient := h.server.GetPdClient()
	info := &config.ChangeFeedInfo{
		UpstreamID:     pdClient.GetClusterID(ctx),
		ChangefeedID:   changefeedID,
		SinkURI:        cfg.SinkURI,
		CreateTime:     time.Now(),
		StartTs:        cfg.StartTs,
		TargetTs:       cfg.TargetTs,
		Config:         replicaCfg,
		State:          config.StateNormal,
		CreatorVersion: version.ReleaseVersion,
		KeyspaceID:     keyspaceMeta.Id,
	}

	// verify sinkURI
	cfConfig := info.ToChangefeedConfig()
	err = sink.Verify(ctx, cfConfig, changefeedID)
	if err != nil {
		_ = c.Error(errors.WrapError(errors.ErrSinkURIInvalid, err, cfg.SinkURI))
		return
	}

	needRemoveGCSafePoint := false
	defer func() {
		if !needRemoveGCSafePoint {
			return
		}

		err = gc.UndoEnsureChangefeedStartTsSafety(
			ctx,
			pdClient,
			keyspaceMeta.Id,
			createGcServiceID,
			changefeedID,
		)
		if err != nil {
			_ = c.Error(err)
			return
		}
	}()

	err = co.CreateChangefeed(ctx, info)
	if err != nil {
		needRemoveGCSafePoint = true
		_ = c.Error(err)
		return
	}

	log.Info("Create changefeed successfully!",
		zap.String("id", info.ChangefeedID.Name()),
		zap.String("state", string(info.State)),
		zap.String("changefeedInfo", info.String()))

	c.JSON(getStatus(c), CfInfoToAPIModel(
		info,
		&config.ChangeFeedStatus{
			CheckpointTs: info.StartTs,
		},
		nil,
	))
}

// ListChangeFeeds lists all changefeeds in cdc cluster
// @Summary List changefeed
// @Description list all changefeeds in cdc cluster
// @Tags changefeed,v2
// @Accept json
// @Produce json
// @Param state query string false "state"
// @Param keyspace query string false "default"
// @Success 200 {array} ChangefeedCommonInfo
// @Failure 500 {object} common.HTTPError
// @Router /api/v2/changefeeds [get]
func (h *OpenAPIV2) ListChangeFeeds(c *gin.Context) {
	co, err := h.server.GetCoordinator()
	if err != nil {
		_ = c.Error(err)
		return
	}

	ok, err := isBootstrapped(co)
	if err != nil || !ok {
		_ = c.Error(err)
		return
	}

	keyspace := GetKeyspaceValueWithDefault(c)
	changefeeds, statuses, err := co.ListChangefeeds(c, keyspace)
	if err != nil {
		_ = c.Error(err)
		return
	}
	state := c.Query(api.APIOpVarChangefeedState)
	commonInfos := make([]ChangefeedCommonInfo, 0)
	for idx, changefeed := range changefeeds {
		if !changefeed.State.IsNeeded(state) {
			continue
		}
		status := statuses[idx]
		var runningErr *config.RunningError
		if changefeed.Error != nil {
			runningErr = changefeed.Error
		} else {
			runningErr = changefeed.Warning
		}
		commonInfos = append(commonInfos, ChangefeedCommonInfo{
			UpstreamID:     changefeed.UpstreamID,
			ID:             changefeed.ChangefeedID.Name(),
			Keyspace:       changefeed.ChangefeedID.Keyspace(),
			FeedState:      changefeed.State,
			CheckpointTSO:  status.CheckpointTs,
			CheckpointTime: api.JSONTime(oracle.GetTimeFromTS(status.CheckpointTs)),
			RunningError:   runningErr,
		})
	}

	c.JSON(http.StatusOK, toListResponse(c, commonInfos))
}

// VerifyTable verify table, return ineligibleTables and EligibleTables.
func (h *OpenAPIV2) VerifyTable(c *gin.Context) {
	ctx := c.Request.Context()
	cfg := &ChangefeedConfig{ReplicaConfig: GetDefaultReplicaConfig()}

	if err := c.BindJSON(&cfg); err != nil {
		_ = c.Error(errors.WrapError(errors.ErrAPIInvalidParam, err))
		return
	}

	// fill replicaConfig
	replicaCfg := cfg.ReplicaConfig.ToInternalReplicaConfig()

	// verify replicaConfig
	sinkURIParsed, err := url.Parse(cfg.SinkURI)
	if err != nil {
		_ = c.Error(errors.WrapError(errors.ErrSinkURIInvalid, err, cfg.SinkURI))
		return
	}
	err = replicaCfg.ValidateAndAdjust(sinkURIParsed)
	if err != nil {
		_ = c.Error(errors.WrapError(errors.ErrInvalidReplicaConfig, err))
		return
	}

	scheme := sinkURIParsed.Scheme
	topic := ""
	if config.IsMQScheme(scheme) {
		topic, err = helper.GetTopic(sinkURIParsed)
		if err != nil {
			_ = c.Error(errors.WrapError(errors.ErrSinkURIInvalid, err, cfg.SinkURI))
			return
		}
	}
	protocol, _ := config.ParseSinkProtocolFromString(util.GetOrZero(replicaCfg.Sink.Protocol))

	keyspaceManager := appcontext.GetService[keyspace.Manager](appcontext.KeyspaceManager)
	keyspaceName := GetKeyspaceValueWithDefault(c)
	kvStorage, err := keyspaceManager.GetStorage(ctx, keyspaceName)
	if err != nil {
		_ = c.Error(err)
		return
	}
	ineligibleTables, eligibleTables, err := getVerifiedTables(ctx, replicaCfg, kvStorage, cfg.StartTs, scheme, topic, protocol)
	if err != nil {
		_ = c.Error(err)
		return
	}
	log.Info("verify table",
		zap.Bool("forceReplicate", replicaCfg.ForceReplicate),
		zap.Bool("ignoreIneligibleTable", cfg.ReplicaConfig.IgnoreIneligibleTable),
	)

	toAPIModelFunc := func(tbls []string) []TableName {
		var apiModels []TableName
		for _, tbl := range tbls {
			apiModels = append(apiModels, TableName{
				Table: tbl,
			})
		}
		return apiModels
	}
	tables := &Tables{
		IneligibleTables: toAPIModelFunc(ineligibleTables),
		EligibleTables:   toAPIModelFunc(eligibleTables),
	}
	c.JSON(http.StatusOK, tables)
}

// getChangefeed get detailed info of a changefeed
// @Summary Get changefeed
// @Description get detail information of a changefeed
// @Tags changefeed,v2
// @Accept json
// @Produce json
// @Param changefeed_id  path  string  true  "changefeed_id"
// @Param keyspace query string false "default"
// @Success 200 {object} ChangeFeedInfo
// @Failure 500,400 {object} common.HTTPError
// @Router /api/v2/changefeeds/{changefeed_id} [get]
func (h *OpenAPIV2) GetChangeFeed(c *gin.Context) {
	changefeedDisplayName := common.NewChangeFeedDisplayName(c.Param(api.APIOpVarChangefeedID), GetKeyspaceValueWithDefault(c))
	co, err := h.server.GetCoordinator()
	if err != nil {
		_ = c.Error(err)
		return
	}

	ok, err := isBootstrapped(co)
	if err != nil || !ok {
		_ = c.Error(err)
		return
	}

	cfInfo, status, err := co.GetChangefeed(c, changefeedDisplayName)
	if err != nil {
		_ = c.Error(err)
		return
	}

	taskStatus := make([]config.CaptureTaskStatus, 0)
	detail := CfInfoToAPIModel(cfInfo, status, taskStatus)
	c.JSON(http.StatusOK, detail)
}

func shouldShowRunningError(state config.FeedState) bool {
	switch state {
	case config.StateNormal, config.StateStopped, config.StateFinished, config.StateRemoved:
		return false
	default:
		return true
	}
}

func CfInfoToAPIModel(
	info *config.ChangeFeedInfo,
	status *config.ChangeFeedStatus,
	taskStatus []config.CaptureTaskStatus,
) *ChangeFeedInfo {
	var runningError *config.RunningError

	// if the state is normal, we shall not return the error info
	// because changefeed will is retrying. errors will confuse the users
	if info.Error != nil && shouldShowRunningError(info.State) {
		runningError = &config.RunningError{
			Addr:    info.Error.Addr,
			Code:    info.Error.Code,
			Message: info.Error.Message,
		}
	}

	sinkURI, err := util.MaskSinkURI(info.SinkURI)
	if err != nil {
		log.Error("failed to mask sink URI", zap.Error(err))
	}

	apiInfoModel := &ChangeFeedInfo{
		UpstreamID:     info.UpstreamID,
		ID:             info.ChangefeedID.Name(),
		Keyspace:       info.ChangefeedID.Keyspace(),
		SinkURI:        sinkURI,
		CreateTime:     info.CreateTime,
		StartTs:        info.StartTs,
		TargetTs:       info.TargetTs,
		AdminJobType:   info.AdminJobType,
		Config:         ToAPIReplicaConfig(info.Config),
		State:          info.State,
		Error:          runningError,
		CreatorVersion: info.CreatorVersion,
		CheckpointTs:   status.CheckpointTs,
		ResolvedTs:     status.CheckpointTs,
		CheckpointTime: api.JSONTime(oracle.GetTimeFromTS(status.CheckpointTs)),
		TaskStatus:     taskStatus,
		MaintainerAddr: status.GetMaintainerAddr(),
		GID:            info.ChangefeedID.ID(),
	}
	return apiInfoModel
}

// DeleteChangefeed handles delete changefeed request
// @Summary Remove a changefeed
// @Description Remove a changefeed
// @Tags changefeed,v2
// @Accept json
// @Produce json
// @Param changefeed_id path string true "changefeed_id"
// @Param keyspace query string false "default"
// @Success 200 {object} EmptyResponse
// @Failure 500,400 {object} common.HTTPError
// @Router	/api/v2/changefeeds/{changefeed_id} [delete]
func (h *OpenAPIV2) DeleteChangefeed(c *gin.Context) {
	ctx := c.Request.Context()
	changefeedDisplayName := common.NewChangeFeedDisplayName(c.Param(api.APIOpVarChangefeedID), GetKeyspaceValueWithDefault(c))
	if err := common.ValidateChangefeedID(changefeedDisplayName.Name); err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedDisplayName.Name))
		return
	}
	co, err := h.server.GetCoordinator()
	if err != nil {
		_ = c.Error(err)
		return
	}

	ok, err := isBootstrapped(co)
	if err != nil || !ok {
		_ = c.Error(err)
		return
	}

	cfInfo, _, err := co.GetChangefeed(c, changefeedDisplayName)
	if err != nil {
		if errors.ErrChangeFeedNotExists.Equal(err) {
			c.JSON(getStatus(c), nil)
			return
		}
		_ = c.Error(err)
		return
	}
	_, err = co.RemoveChangefeed(ctx, cfInfo.ChangefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.JSON(getStatus(c), &EmptyResponse{})
}

// PauseChangefeed handles pause changefeed request
// PauseChangefeed pauses a changefeed
// @Summary Pause a changefeed
// @Description Pause a changefeed
// @Tags changefeed,v2
// @Accept json
// @Produce json
// @Param changefeed_id  path  string  true  "changefeed_id"
// @Param keyspace query string false "default"
// @Success 200 {object} EmptyResponse
// @Failure 500,400 {object} common.HTTPError
// @Router /api/v2/changefeeds/{changefeed_id}/pause [post]
func (h *OpenAPIV2) PauseChangefeed(c *gin.Context) {
	ctx := c.Request.Context()
	changefeedDisplayName := common.NewChangeFeedDisplayName(c.Param(api.APIOpVarChangefeedID), GetKeyspaceValueWithDefault(c))
	if err := common.ValidateChangefeedID(changefeedDisplayName.Name); err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedDisplayName.Name))
		return
	}

	co, err := h.server.GetCoordinator()
	if err != nil {
		_ = c.Error(err)
		return
	}

	ok, err := isBootstrapped(co)
	if err != nil || !ok {
		_ = c.Error(err)
		return
	}

	cfInfo, _, err := co.GetChangefeed(c, changefeedDisplayName)
	if err != nil {
		_ = c.Error(err)
		return
	}
	err = co.PauseChangefeed(ctx, cfInfo.ChangefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.JSON(getStatus(c), &EmptyResponse{})
}

// ResumeChangefeed handles resume changefeed request.
// ResumeChangefeed resumes a changefeed
// @Summary Resume a changefeed
// @Description Resume a changefeed
// @Tags changefeed,v2
// @Accept json
// @Produce json
// @Param changefeed_id path string true "changefeed_id"
// @Param keyspace query string false "default"
// @Param resumeConfig body ResumeChangefeedConfig true "resume config"
// @Success 200 {object} EmptyResponse
// @Failure 500,400 {object} common.HTTPError
// @Router	/api/v2/changefeeds/{changefeed_id}/resume [post]
func (h *OpenAPIV2) ResumeChangefeed(c *gin.Context) {
	ctx := c.Request.Context()
	keyspaceName := GetKeyspaceValueWithDefault(c)
	changefeedDisplayName := common.NewChangeFeedDisplayName(c.Param(api.APIOpVarChangefeedID), keyspaceName)
	if err := common.ValidateChangefeedID(changefeedDisplayName.Name); err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedDisplayName.Name))
		return
	}

	cfg := new(ResumeChangefeedConfig)
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		_ = c.Error(errors.WrapError(errors.ErrAPIInvalidParam, err))
		return
	}

	// Check if body is empty
	if len(body) == 0 {
		log.Info("resume changefeed config is empty, using defaults")
	} else {
		if err := json.Unmarshal(body, cfg); err != nil {
			log.Error("failed to bind resume changefeed config", zap.Error(err), zap.String("body", string(body)))
			_ = c.Error(errors.WrapError(errors.ErrAPIInvalidParam, err))
			return
		}
	}

	co, err := h.server.GetCoordinator()
	if err != nil {
		_ = c.Error(err)
		return
	}

	ok, err := isBootstrapped(co)
	if err != nil || !ok {
		_ = c.Error(err)
		return
	}

	cfInfo, status, err := co.GetChangefeed(c, changefeedDisplayName)
	if err != nil {
		_ = c.Error(err)
		return
	}

	// If there is no overrideCheckpointTs, then check whether the currentCheckpointTs is smaller than gc safepoint or not.
	newCheckpointTs := status.CheckpointTs
	if cfg.OverwriteCheckpointTs != 0 {
		newCheckpointTs = cfg.OverwriteCheckpointTs
	}

	keyspaceMeta := middleware.GetKeyspaceFromContext(c)

	if keyspaceMeta.State != keyspacepb.KeyspaceState_ENABLED {
		c.IndentedJSON(http.StatusBadRequest, errors.ErrAPIInvalidParam)
		c.Abort()
		return
	}

	resumeGcServiceID := h.server.GetEtcdClient().GetEnsureGCServiceID(gc.EnsureGCServiceResuming)
	if err := verifyResumeChangefeedConfig(
		ctx,
		h.server.GetPdClient(),
		resumeGcServiceID,
		keyspaceMeta.Id,
		cfInfo.ChangefeedID,
		newCheckpointTs); err != nil {
		_ = c.Error(err)
		return
	}

	needRemoveGCSafePoint := false
	defer func() {
		if !needRemoveGCSafePoint {
			return
		}

		err = gc.UndoEnsureChangefeedStartTsSafety(
			ctx,
			h.server.GetPdClient(),
			keyspaceMeta.Id,
			resumeGcServiceID,
			cfInfo.ChangefeedID,
		)
		if err != nil {
			_ = c.Error(err)
			return
		}
	}()

	err = co.ResumeChangefeed(ctx, cfInfo.ChangefeedID, newCheckpointTs, cfg.OverwriteCheckpointTs != 0)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.Errors = nil
	c.JSON(getStatus(c), &EmptyResponse{})
}

// UpdateChangefeed handles update changefeed request,
// it returns the updated changefeedInfo
// Can only update a changefeed's: TargetTs, SinkURI,
// ReplicaConfig, PDAddrs, CAPath, CertPath, KeyPath,
// SyncPointEnabled, SyncPointInterval
// UpdateChangefeed updates a changefeed
// @Summary Update a changefeed
// @Description Update a changefeed
// @Tags changefeed,v2
// @Accept json
// @Produce json
// @Param changefeed_id  path  string  true  "changefeed_id"
// @Param keyspace query string false "default"
// @Param changefeedConfig body ChangefeedConfig true "changefeed config"
// @Success 200 {object} ChangeFeedInfo
// @Failure 500,400 {object} common.HTTPError
// @Router /api/v2/changefeeds/{changefeed_id} [put]
func (h *OpenAPIV2) UpdateChangefeed(c *gin.Context) {
	ctx := c.Request.Context()

	keyspaceName := GetKeyspaceValueWithDefault(c)

	keyspaceMeta := middleware.GetKeyspaceFromContext(c)

	if keyspaceMeta.State != keyspacepb.KeyspaceState_ENABLED {
		c.IndentedJSON(http.StatusBadRequest, errors.ErrAPIInvalidParam)
		c.Abort()
		return
	}

	changefeedDisplayName := common.NewChangeFeedDisplayName(c.Param(api.APIOpVarChangefeedID), keyspaceName)
	if err := common.ValidateChangefeedID(changefeedDisplayName.Name); err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedDisplayName.Name))
		return
	}
	co, err := h.server.GetCoordinator()
	if err != nil {
		_ = c.Error(err)
		return
	}

	ok, err := isBootstrapped(co)
	if err != nil || !ok {
		_ = c.Error(err)
		return
	}

	oldCfInfo, status, err := co.GetChangefeed(c, changefeedDisplayName)
	if err != nil {
		_ = c.Error(err)
		return
	}

	switch oldCfInfo.State {
	case config.StateStopped, config.StateFailed:
	default:
		_ = c.Error(
			errors.ErrChangefeedUpdateRefused.GenWithStackByArgs(
				"can only update changefeed config when it is stopped or failed",
			),
		)
		return
	}

	updateCfConfig := &ChangefeedConfig{}
	if err = c.BindJSON(updateCfConfig); err != nil {
		_ = c.Error(errors.WrapError(errors.ErrAPIInvalidParam, err))
		return
	}

	var configUpdated, sinkURIUpdated bool
	if updateCfConfig.TargetTs != 0 {
		if updateCfConfig.TargetTs <= oldCfInfo.StartTs {
			_ = c.Error(errors.ErrChangefeedUpdateRefused.GenWithStack(
				"can not update target_ts:%d less than start_ts:%d",
				updateCfConfig.TargetTs, oldCfInfo.StartTs))
			return
		}
		oldCfInfo.TargetTs = updateCfConfig.TargetTs
	}
	if updateCfConfig.ReplicaConfig != nil {
		configUpdated = true
		oldCfInfo.Config = updateCfConfig.ReplicaConfig.ToInternalReplicaConfig()
	}
	if updateCfConfig.SinkURI != "" {
		sinkURIUpdated = true
		oldCfInfo.SinkURI = updateCfConfig.SinkURI
	}
	if updateCfConfig.StartTs != 0 {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack("start_ts can not be updated"))
		return
	}

	if configUpdated || sinkURIUpdated {
		// verify replicaConfig
		sinkURIParsed, err := url.Parse(oldCfInfo.SinkURI)
		if err != nil {
			_ = c.Error(errors.WrapError(errors.ErrSinkURIInvalid, err, oldCfInfo.SinkURI))
			return
		}
		err = oldCfInfo.Config.ValidateAndAdjust(sinkURIParsed)
		if err != nil {
			_ = c.Error(errors.WrapError(errors.ErrInvalidReplicaConfig, err))
			return
		}

		scheme := sinkURIParsed.Scheme
		topic := ""
		if config.IsMQScheme(scheme) {
			topic, err = helper.GetTopic(sinkURIParsed)
			if err != nil {
				_ = c.Error(errors.WrapError(errors.ErrSinkURIInvalid, err, oldCfInfo.SinkURI))
				return
			}
		}
		protocol, _ := config.ParseSinkProtocolFromString(util.GetOrZero(oldCfInfo.Config.Sink.Protocol))

		keyspaceManager := appcontext.GetService[keyspace.Manager](appcontext.KeyspaceManager)

		kvStorage, err := keyspaceManager.GetStorage(ctx, keyspaceName)
		if err != nil {
			_ = c.Error(err)
			return
		}

		// use checkpointTs get snapshot from kv storage
		ineligibleTables, _, err := getVerifiedTables(ctx, oldCfInfo.Config, kvStorage, status.CheckpointTs, scheme, topic, protocol)
		if err != nil {
			_ = c.Error(errors.ErrChangefeedUpdateRefused.GenWithStackByCause(err))
			return
		}
		if !oldCfInfo.Config.ForceReplicate && !oldCfInfo.Config.IgnoreIneligibleTable {
			if len(ineligibleTables) != 0 {
				_ = c.Error(errors.ErrTableIneligible.GenWithStackByArgs(ineligibleTables))
				return
			}
		}
	}

	// verify sink
	err = sink.Verify(ctx, oldCfInfo.ToChangefeedConfig(), oldCfInfo.ChangefeedID)
	if err != nil {
		_ = c.Error(errors.WrapError(errors.ErrSinkURIInvalid, err, oldCfInfo.SinkURI))
		return
	}

	if err = co.UpdateChangefeed(ctx, oldCfInfo); err != nil {
		_ = c.Error(err)
		return
	}

	c.JSON(getStatus(c), CfInfoToAPIModel(oldCfInfo, status, nil))
}

// verifyResumeChangefeedConfig verifies the changefeed config before resuming a changefeed
// overrideCheckpointTs is the checkpointTs of the changefeed that specified by the user.
// or it is the checkpointTs of the changefeed before it is paused.
// we need to check weather the resuming changefeed is gc safe or not.
func verifyResumeChangefeedConfig(
	ctx context.Context,
	pdClient pd.Client,
	gcServiceID string,
	keyspaceID uint32,
	changefeedID common.ChangeFeedID,
	overrideCheckpointTs uint64,
) error {
	if overrideCheckpointTs == 0 {
		return nil
	}

	ts, logical, err := pdClient.GetTS(ctx)
	if err != nil {
		return errors.ErrPDEtcdAPIError.GenWithStackByArgs("fail to get ts from pd client")
	}
	currentTSO := oracle.ComposeTS(ts, logical)
	if overrideCheckpointTs > currentTSO {
		return errors.ErrAPIInvalidParam.GenWithStack(
			"invalid checkpoint-ts %v, larger than current tso %v", overrideCheckpointTs, currentTSO)
	}

	// 1h is enough for resuming a changefeed.
	gcTTL := int64(60 * 60)
	err = gc.EnsureChangefeedStartTsSafety(
		ctx,
		pdClient,
		gcServiceID,
		keyspaceID,
		changefeedID,
		gcTTL, overrideCheckpointTs)
	if err != nil {
		if !errors.ErrStartTsBeforeGC.Equal(err) {
			return errors.ErrPDEtcdAPIError.Wrap(err)
		}
		return err
	}

	return nil
}

// MoveTable handles move table in changefeed to target node,
// it returns the move result(success or err)
// This api is for inner test use, not public use. It may be removed in the future.
// Usage:
// curl -X POST http://127.0.0.1:8300/api/v2/changefeeds/changefeed-test1/move_table?tableID={tableID}&targetNodeID={targetNodeID}
// Note:
// 1. tableID is the table id in the changefeed
// 2. targetNodeID is the node id to move the table to
// You can find the node id by using the list_captures api
func (h *OpenAPIV2) MoveTable(c *gin.Context) {
	tableIdStr := c.Query("tableID")
	tableId, err := strconv.ParseInt(tableIdStr, 10, 64)
	if err != nil {
		log.Error("failed to parse tableID", zap.Error(err), zap.String("tableID", tableIdStr))
		_ = c.Error(err)
		return
	}

	changefeedDisplayName := common.NewChangeFeedDisplayName(c.Param(api.APIOpVarChangefeedID), GetKeyspaceValueWithDefault(c))
	if err := common.ValidateChangefeedID(changefeedDisplayName.Name); err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedDisplayName.Name))
		return
	}

	// get changefeedID first
	cfInfo, err := getChangeFeed(c.Request.Host, changefeedDisplayName.Keyspace, changefeedDisplayName.Name)
	if err != nil {
		_ = c.Error(err)
		return
	}

	if cfInfo.MaintainerAddr == "" {
		_ = c.Error(errors.New("Can't not find maintainer for changefeed: " + changefeedDisplayName.Name))
		return
	}

	selfInfo, err := h.server.SelfInfo()
	if err != nil {
		_ = c.Error(err)
		return
	}

	if cfInfo.MaintainerAddr != selfInfo.AdvertiseAddr {
		// Forward the request to the maintainer
		middleware.ForwardToServer(c, selfInfo.ID, cfInfo.MaintainerAddr)
		c.Abort()
		return
	}

	changefeedID := common.ChangeFeedID{
		Id:          cfInfo.GID,
		DisplayName: common.NewChangeFeedDisplayName(cfInfo.ID, cfInfo.Keyspace),
	}

	maintainerManager := h.server.GetMaintainerManager()
	maintainer, ok := maintainerManager.GetMaintainerForChangefeed(changefeedID)

	if !ok {
		log.Error("maintainer not found for changefeed in this node", zap.String("GID", changefeedID.Id.String()), zap.String("Name", changefeedID.DisplayName.String()))
		_ = c.Error(errors.ErrMaintainerNotFounded)
		return
	}

	targetNodeID := c.Query("targetNodeID")
	mode, _ := strconv.ParseInt(c.Query("mode"), 10, 64)
	err = maintainer.MoveTable(int64(tableId), node.ID(targetNodeID), mode)
	if err != nil {
		log.Error("failed to move table", zap.Error(err), zap.Int64("tableID", tableId), zap.String("targetNodeID", targetNodeID))
		_ = c.Error(err)
		return
	}
	c.JSON(getStatus(c), &EmptyResponse{})
}

// MoveSplitTable handles move all dispatchers in the splited table in changefeed to target node,
// it returns the move result(success or err)
// This api is for inner test use, not public use. It may be removed in the future.
// Usage:
// curl -X POST http://127.0.0.1:8300/api/v2/changefeeds/changefeed-test1/move_split_table?tableID={tableID}&targetNodeID={targetNodeID}
// Note:
// 1. tableID is the table id in the changefeed
// 2. targetNodeID is the node id to move the table to
// You can find the node id by using the list_captures api
func (h *OpenAPIV2) MoveSplitTable(c *gin.Context) {
	tableIdStr := c.Query("tableID")
	tableId, err := strconv.ParseInt(tableIdStr, 10, 64)
	if err != nil {
		log.Error("failed to parse tableID", zap.Error(err), zap.String("tableID", tableIdStr))
		_ = c.Error(err)
		return
	}

	changefeedDisplayName := common.NewChangeFeedDisplayName(c.Param(api.APIOpVarChangefeedID), GetKeyspaceValueWithDefault(c))
	if err := common.ValidateChangefeedID(changefeedDisplayName.Name); err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedDisplayName.Name))
		return
	}

	// get changefeedID first
	cfInfo, err := getChangeFeed(c.Request.Host, changefeedDisplayName.Keyspace, changefeedDisplayName.Name)
	if err != nil {
		_ = c.Error(err)
		return
	}

	if cfInfo.MaintainerAddr == "" {
		_ = c.Error(errors.New("Can't not find maintainer for changefeed: " + changefeedDisplayName.Name))
		return
	}

	selfInfo, err := h.server.SelfInfo()
	if err != nil {
		_ = c.Error(err)
		return
	}

	if cfInfo.MaintainerAddr != selfInfo.AdvertiseAddr {
		// Forward the request to the maintainer
		middleware.ForwardToServer(c, selfInfo.ID, cfInfo.MaintainerAddr)
		c.Abort()
		return
	}

	changefeedID := common.ChangeFeedID{
		Id:          cfInfo.GID,
		DisplayName: common.NewChangeFeedDisplayName(cfInfo.ID, cfInfo.Keyspace),
	}

	maintainerManager := h.server.GetMaintainerManager()
	maintainer, ok := maintainerManager.GetMaintainerForChangefeed(changefeedID)

	if !ok {
		log.Error("maintainer not found for changefeed in this node", zap.String("GID", changefeedID.Id.String()), zap.String("Name", changefeedID.DisplayName.String()))
		_ = c.Error(errors.ErrMaintainerNotFounded)
		return
	}

	targetNodeID := c.Query("targetNodeID")
	mode, _ := strconv.ParseInt(c.Query("mode"), 10, 64)
	err = maintainer.MoveSplitTable(int64(tableId), node.ID(targetNodeID), mode)
	if err != nil {
		log.Error("failed to move split table", zap.Error(err), zap.Int64("tableID", tableId), zap.String("targetNodeID", targetNodeID))
		_ = c.Error(err)
		return
	}
	c.JSON(getStatus(c), &EmptyResponse{})
}

// SplitTableByRegionCount do split table by region count in changefeed,
// it can also split the table when there are multiple dispatchers in the table.
// it returns the split result(success or err)
// This api is for inner test use, not public use. It may be removed in the future.
// Usage:
// curl -X POST http://127.0.0.1:8300/api/v2/changefeeds/changefeed-test1/split_table_by_region_count?tableID={tableID}
// Note:
// 1. tableID is the table id in the changefeed
func (h *OpenAPIV2) SplitTableByRegionCount(c *gin.Context) {
	tableIdStr := c.Query("tableID")
	tableId, err := strconv.ParseInt(tableIdStr, 10, 64)
	if err != nil {
		log.Error("failed to parse tableID", zap.Error(err), zap.String("tableID", tableIdStr))
		_ = c.Error(err)
		return
	}

	changefeedDisplayName := common.NewChangeFeedDisplayName(c.Param(api.APIOpVarChangefeedID), GetKeyspaceValueWithDefault(c))
	if err := common.ValidateChangefeedID(changefeedDisplayName.Name); err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedDisplayName.Name))
		return
	}

	// get changefeedID first
	cfInfo, err := getChangeFeed(c.Request.Host, changefeedDisplayName.Keyspace, changefeedDisplayName.Name)
	if err != nil {
		_ = c.Error(err)
		return
	}

	if cfInfo.MaintainerAddr == "" {
		_ = c.Error(errors.New("Can't not find maintainer for changefeed: " + changefeedDisplayName.Name))
		return
	}

	selfInfo, err := h.server.SelfInfo()
	if err != nil {
		_ = c.Error(err)
		return
	}

	if cfInfo.MaintainerAddr != selfInfo.AdvertiseAddr {
		// Forward the request to the maintainer
		middleware.ForwardToServer(c, selfInfo.ID, cfInfo.MaintainerAddr)
		c.Abort()
		return
	}

	changefeedID := common.ChangeFeedID{
		Id:          cfInfo.GID,
		DisplayName: common.NewChangeFeedDisplayName(cfInfo.ID, cfInfo.Keyspace),
	}

	maintainerManager := h.server.GetMaintainerManager()
	maintainer, ok := maintainerManager.GetMaintainerForChangefeed(changefeedID)

	if !ok {
		log.Error("maintainer not found for changefeed in this node", zap.String("GID", changefeedID.Id.String()), zap.String("Name", changefeedID.DisplayName.String()))
		_ = c.Error(errors.ErrMaintainerNotFounded)
		return
	}
	mode, _ := strconv.ParseInt(c.Query("mode"), 10, 64)
	err = maintainer.SplitTableByRegionCount(int64(tableId), mode)
	if err != nil {
		log.Error("failed to split table by region count", zap.Error(err), zap.Int64("tableID", tableId))
		_ = c.Error(err)
		return
	}
	c.JSON(getStatus(c), &EmptyResponse{})
}

// MergeTable merges the split table in changefeed, it just merge two nearby dispatchers into one dispatcher in this table.
// it returns the split result(success or err)
// This api is for inner test use, not public use. It may be removed in the future.
// Usage:
// curl -X POST http://127.0.0.1:8300/api/v2/changefeeds/changefeed-test1/merge_table?tableID={tableID}
// Note:
// 1. tableID is the table id in the changefeed
func (h *OpenAPIV2) MergeTable(c *gin.Context) {
	tableIdStr := c.Query("tableID")
	tableId, err := strconv.ParseInt(tableIdStr, 10, 64)
	if err != nil {
		log.Error("failed to parse tableID", zap.Error(err), zap.String("tableID", tableIdStr))
		_ = c.Error(err)
		return
	}

	changefeedDisplayName := common.NewChangeFeedDisplayName(c.Param(api.APIOpVarChangefeedID), GetKeyspaceValueWithDefault(c))
	if err := common.ValidateChangefeedID(changefeedDisplayName.Name); err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedDisplayName.Name))
		return
	}

	// get changefeedID first
	cfInfo, err := getChangeFeed(c.Request.Host, changefeedDisplayName.Keyspace, changefeedDisplayName.Name)
	if err != nil {
		_ = c.Error(err)
		return
	}

	if cfInfo.MaintainerAddr == "" {
		_ = c.Error(errors.New("Can't not find maintainer for changefeed: " + changefeedDisplayName.Name))
		return
	}

	selfInfo, err := h.server.SelfInfo()
	if err != nil {
		_ = c.Error(err)
		return
	}

	if cfInfo.MaintainerAddr != selfInfo.AdvertiseAddr {
		// Forward the request to the maintainer
		middleware.ForwardToServer(c, selfInfo.ID, cfInfo.MaintainerAddr)
		c.Abort()
		return
	}

	changefeedID := common.ChangeFeedID{
		Id:          cfInfo.GID,
		DisplayName: common.NewChangeFeedDisplayName(cfInfo.ID, cfInfo.Keyspace),
	}

	maintainerManager := h.server.GetMaintainerManager()
	maintainer, ok := maintainerManager.GetMaintainerForChangefeed(changefeedID)

	if !ok {
		log.Error("maintainer not found for changefeed in this node", zap.String("GID", changefeedID.Id.String()), zap.String("Name", changefeedID.DisplayName.String()))
		_ = c.Error(errors.ErrMaintainerNotFounded)
		return
	}

	mode, _ := strconv.ParseInt(c.Query("mode"), 10, 64)
	err = maintainer.MergeTable(int64(tableId), mode)
	if err != nil {
		log.Error("failed to merge table", zap.Error(err), zap.Int64("tableID", tableId))
		_ = c.Error(err)
		return
	}
	c.JSON(getStatus(c), &EmptyResponse{})
}

// ListTables lists all tables in a changefeed
// Usage:
// curl -X GET http://127.0.0.1:8300/api/v2/changefeeds/changefeed-test1/tables
// Note: This api is for inner test use, not public use. It may be changed or removed in the future.
func (h *OpenAPIV2) ListTables(c *gin.Context) {
	changefeedDisplayName := common.NewChangeFeedDisplayName(c.Param(api.APIOpVarChangefeedID), GetKeyspaceValueWithDefault(c))
	if err := common.ValidateChangefeedID(changefeedDisplayName.Name); err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedDisplayName.Name))
		return
	}

	// get changefeedID first
	cfInfo, err := getChangeFeed(c.Request.Host, changefeedDisplayName.Keyspace, changefeedDisplayName.Name)
	if err != nil {
		_ = c.Error(err)
		return
	}

	if cfInfo.MaintainerAddr == "" {
		_ = c.Error(errors.New("Can't not find maintainer for changefeed: " + changefeedDisplayName.Name))
		return
	}

	selfInfo, err := h.server.SelfInfo()
	if err != nil {
		_ = c.Error(err)
		return
	}

	if cfInfo.MaintainerAddr != selfInfo.AdvertiseAddr {
		// Forward the request to the maintainer
		middleware.ForwardToServer(c, selfInfo.ID, cfInfo.MaintainerAddr)
		c.Abort()
		return
	}

	changefeedID := common.ChangeFeedID{
		Id:          cfInfo.GID,
		DisplayName: common.NewChangeFeedDisplayName(cfInfo.ID, cfInfo.Keyspace),
	}

	maintainerManager := h.server.GetMaintainerManager()
	maintainer, ok := maintainerManager.GetMaintainerForChangefeed(changefeedID)
	if !ok {
		log.Error("maintainer not found for changefeed in this node", zap.String("GID", changefeedID.Id.String()), zap.String("Name", changefeedID.DisplayName.String()))
		_ = c.Error(errors.ErrMaintainerNotFounded)
		return
	}

	mode, _ := strconv.ParseInt(c.Query("mode"), 10, 64)
	tables := maintainer.GetTables(mode)

	nodeTableInfoMap := make(map[string]*NodeTableInfo)

	for _, table := range tables {
		nodeID := table.GetNodeID().String()
		nodeTableInfo, ok := nodeTableInfoMap[nodeID]
		if !ok {
			nodeTableInfo = newNodeTableInfo(nodeID)
			nodeTableInfoMap[nodeID] = nodeTableInfo
		}
		nodeTableInfo.addTableID(table.Span.TableID)
	}

	infos := make([]NodeTableInfo, 0, len(nodeTableInfoMap))
	for _, nodeTableInfo := range nodeTableInfoMap {
		infos = append(infos, *nodeTableInfo)
	}

	c.JSON(http.StatusOK, toListResponse(c, infos))
}

// getDispatcherCount returns the count of dispatcher.
// getDispatcherCount is just for inner test use, not public use.
func (h *OpenAPIV2) getDispatcherCount(c *gin.Context) {
	changefeedDisplayName := common.NewChangeFeedDisplayName(c.Param(api.APIOpVarChangefeedID), GetKeyspaceValueWithDefault(c))
	if err := common.ValidateChangefeedID(changefeedDisplayName.Name); err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedDisplayName.Name))
		return
	}

	cfInfo, err := getChangeFeed(c.Request.Host, changefeedDisplayName.Keyspace, changefeedDisplayName.Name)
	if err != nil {
		_ = c.Error(err)
		return
	}

	if cfInfo.MaintainerAddr == "" {
		_ = c.Error(errors.New("Can't not find maintainer for changefeed: " + changefeedDisplayName.Name))
		return
	}

	selfInfo, err := h.server.SelfInfo()
	if err != nil {
		_ = c.Error(err)
		return
	}

	if cfInfo.MaintainerAddr != selfInfo.AdvertiseAddr {
		// Forward the request to the maintainer
		middleware.ForwardToServer(c, selfInfo.ID, cfInfo.MaintainerAddr)
		c.Abort()
		return
	}

	changefeedID := common.ChangeFeedID{
		Id:          cfInfo.GID,
		DisplayName: common.NewChangeFeedDisplayName(cfInfo.ID, cfInfo.Keyspace),
	}

	maintainerManager := h.server.GetMaintainerManager()
	maintainer, ok := maintainerManager.GetMaintainerForChangefeed(changefeedID)

	if !ok {
		log.Error("maintainer not found for changefeed in this node", zap.String("GID", changefeedID.Id.String()), zap.String("changefeed", changefeedID.String()))
		_ = c.Error(errors.ErrMaintainerNotFounded)
		return
	}

	mode, _ := strconv.ParseInt(c.Query("mode"), 10, 64)
	number := maintainer.GetDispatcherCount(mode)
	c.JSON(http.StatusOK, &DispatcherCount{Count: number})
}

// status returns the status of a changefeed.
// Usage:
// curl -X GET http://127.0.0.1:8300/api/v2/changefeeds/changefeed-test1/status
func (h *OpenAPIV2) status(c *gin.Context) {
	changefeedDisplayName := common.NewChangeFeedDisplayName(c.Param(api.APIOpVarChangefeedID), GetKeyspaceValueWithDefault(c))
	co, err := h.server.GetCoordinator()
	if err != nil {
		_ = c.Error(err)
		return
	}

	ok, err := isBootstrapped(co)
	if err != nil || !ok {
		_ = c.Error(err)
		return
	}

	info, status, err := co.GetChangefeed(c, changefeedDisplayName)
	if err != nil {
		_ = c.Error(err)
		return
	}
	var (
		lastError   *config.RunningError
		lastWarning *config.RunningError
	)
	if info.Error != nil &&
		oracle.GetTimeFromTS(status.CheckpointTs).Before(info.Error.Time) {
		err := &config.RunningError{
			Time:    info.Error.Time,
			Addr:    info.Error.Addr,
			Code:    info.Error.Code,
			Message: info.Error.Message,
		}
		switch info.State {
		case config.StateFailed:
			lastError = err
		case config.StateWarning:
			lastWarning = err
		}
	}

	c.JSON(http.StatusOK, &ChangefeedStatus{
		State:        string(info.State),
		CheckpointTs: status.CheckpointTs,
		// FIXME: add correct resolvedTs
		ResolvedTs:  status.CheckpointTs,
		LastError:   lastError,
		LastWarning: lastWarning,
	})
}

// synced returns the sync state of a changefeed.
// Usage:
// curl -X GET http://127.0.0.1:8300/api/v2/changefeeds/changefeed-test1/synced
func (h *OpenAPIV2) synced(c *gin.Context) {
	changefeedDisplayName := common.NewChangeFeedDisplayName(c.Param(api.APIOpVarChangefeedID), GetKeyspaceValueWithDefault(c))
	co, err := h.server.GetCoordinator()
	if err != nil {
		_ = c.Error(err)
		return
	}

	ok, err := isBootstrapped(co)
	if err != nil || !ok {
		_ = c.Error(err)
		return
	}

	co.RequestResolvedTsFromLogCoordinator(c, changefeedDisplayName)
	info, status, err := co.GetChangefeed(c, changefeedDisplayName)
	if err != nil {
		_ = c.Error(err)
		return
	}
	if info.Config.SyncedStatus.SyncedCheckInterval == 0 || info.Config.SyncedStatus.CheckpointInterval == 0 {
		info.Config.SyncedStatus.SyncedCheckInterval = config.GetDefaultReplicaConfig().SyncedStatus.SyncedCheckInterval
		info.Config.SyncedStatus.CheckpointInterval = config.GetDefaultReplicaConfig().SyncedStatus.CheckpointInterval
	}
	syncedCheckInterval := info.Config.SyncedStatus.SyncedCheckInterval
	checkpointInterval := info.Config.SyncedStatus.CheckpointInterval

	// get time from pd
	ctx := c.Request.Context()
	ts, _, err := h.server.GetPdClient().GetTS(ctx)
	if err != nil {
		_ = c.Error(errors.ErrPDEtcdAPIError.GenWithStackByArgs("fail to get ts from pd client"))
		return
	}

	// If physcialNow - lastSyncedTs > SyncedCheckInterval && physcialNow - CheckpointTs < CheckpointInterval
	//         --> reach strict synced status
	if (ts-oracle.ExtractPhysical(status.LastSyncedTs) > syncedCheckInterval*1000) &&
		(ts-oracle.ExtractPhysical(status.CheckpointTs) < checkpointInterval*1000) {
		c.JSON(http.StatusOK, SyncedStatus{
			Synced:           true,
			SinkCheckpointTs: api.JSONTime(oracle.GetTimeFromTS(status.CheckpointTs)),
			PullerResolvedTs: api.JSONTime(oracle.GetTimeFromTS(status.LogCoordinatorResolvedTs)),
			LastSyncedTs:     api.JSONTime(oracle.GetTimeFromTS(status.LastSyncedTs)),
			NowTs:            api.JSONTime(time.Unix(ts/1e3, 0)),
			Info:             "The data syncing is finished",
		})
		return
	}

	// If physcialNow - lastSyncedTs > SyncedCheckInterval && physcialNow - CheckpointTs > CheckpointInterval
	//         we should consider the situation that pd or tikv region is not healthy and blocked the advancing resolveTs.
	//         if pullerResolvedTs - checkpointTs > CheckpointInterval-->  data is not synced
	//         otherwise, if pd & tikv is healthy --> data is not synced
	//                    if not healthy --> data is synced
	if ts-oracle.ExtractPhysical(status.LastSyncedTs) > syncedCheckInterval*1000 {
		var message string
		if oracle.ExtractPhysical(status.LogCoordinatorResolvedTs)-oracle.ExtractPhysical(status.CheckpointTs) < checkpointInterval*1000 {
			message = fmt.Sprintf("Please check whether PD is online and TiKV Regions are all available. " +
				"If PD is offline or some TiKV regions are not available, it means that the data syncing process is complete. " +
				"If the gap is large, such as a few minutes, it means that some regions in TiKV are unavailable. " +
				"Otherwise, if the gap is small and PD is online, it means the data syncing is incomplete, so please wait")
		} else {
			message = "The data syncing is not finished, please wait"
		}
		c.JSON(http.StatusOK, SyncedStatus{
			Synced:           false,
			SinkCheckpointTs: api.JSONTime(oracle.GetTimeFromTS(status.CheckpointTs)),
			PullerResolvedTs: api.JSONTime(oracle.GetTimeFromTS(status.LogCoordinatorResolvedTs)),
			LastSyncedTs:     api.JSONTime(oracle.GetTimeFromTS(status.LastSyncedTs)),
			NowTs:            api.JSONTime(time.Unix(ts/1e3, 0)),
			Info:             message,
		})
		return
	}

	// If physcialNow - lastSyncedTs < SyncedCheckInterval --> data is not synced
	c.JSON(http.StatusOK, SyncedStatus{
		Synced:           false,
		SinkCheckpointTs: api.JSONTime(oracle.GetTimeFromTS(status.CheckpointTs)),
		PullerResolvedTs: api.JSONTime(oracle.GetTimeFromTS(status.LogCoordinatorResolvedTs)),
		LastSyncedTs:     api.JSONTime(oracle.GetTimeFromTS(status.LastSyncedTs)),
		NowTs:            api.JSONTime(time.Unix(ts/1e3, 0)),
		Info:             "The data syncing is not finished, please wait",
	})
}

func getVerifiedTables(
	ctx context.Context,
	replicaConfig *config.ReplicaConfig,
	storage tidbkv.Storage, startTs uint64,
	scheme string, topic string, protocol config.Protocol,
) ([]string, []string, error) {
	f, err := filter.NewFilter(replicaConfig.Filter, "", replicaConfig.CaseSensitive, replicaConfig.ForceReplicate)
	if err != nil {
		return nil, nil, err
	}
	tableInfos, ineligibleTables, eligibleTables, err := schemastore.
		VerifyTables(f, storage, startTs)
	if err != nil {
		return nil, nil, err
	}
	log.Info("verifyTables completed",
		zap.Int("tableCount", len(tableInfos)),
		zap.Uint64("startTs", startTs))

	err = f.Verify(tableInfos)
	if err != nil {
		return nil, nil, err
	}
	if !config.IsMQScheme(scheme) {
		return ineligibleTables, eligibleTables, nil
	}

	eventRouter, err := eventrouter.NewEventRouter(replicaConfig.Sink, topic, config.IsPulsarScheme(protocol.String()), protocol == config.ProtocolAvro)
	if err != nil {
		return nil, nil, err
	}
	err = eventRouter.VerifyTables(tableInfos)
	if err != nil {
		return nil, nil, err
	}

	selectors, err := columnselector.New(replicaConfig.Sink)
	if err != nil {
		return nil, nil, err
	}
	err = selectors.VerifyTables(tableInfos, eventRouter)
	if err != nil {
		return nil, nil, err
	}

	if ctx.Err() != nil {
		return nil, nil, errors.Trace(ctx.Err())
	}

	return ineligibleTables, eligibleTables, nil
}

func GetKeyspaceValueWithDefault(c *gin.Context) string {
	if kerneltype.IsClassic() {
		return common.DefaultKeyspaceNamme
	}

	keyspace := c.Query(api.APIOpVarKeyspace)
	if keyspace == "" {
		keyspace = common.DefaultKeyspaceNamme
	}
	return keyspace
}
