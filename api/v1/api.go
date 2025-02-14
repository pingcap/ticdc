// Copyright 2025 PingCAP, Inc.
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
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/api/middleware"
	pv2 "github.com/pingcap/ticdc/api/v2"
	v2 "github.com/pingcap/ticdc/api/v2"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/server"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

func adjustStatusForV1(c *gin.Context) {
	c.Status(http.StatusAccepted)
	c.Next()
}

// OpenAPIV1 provides CDC v1 APIs
type OpenAPIV1 struct {
	server server.Server
	v2     pv2.OpenAPIV2
}

// NewOpenAPIV1 creates a new OpenAPIV1.
func NewOpenAPIV1(c server.Server) OpenAPIV1 {
	return OpenAPIV1{c, pv2.NewOpenAPIV2(c)}
}

// RegisterOpenAPIV1Routes registers routes for OpenAPIV1
func RegisterOpenAPIV1Routes(router *gin.Engine, api OpenAPIV1) {
	v1 := router.Group("/api/v1")

	v1.Use(middleware.LogMiddleware())
	v1.Use(middleware.ErrorHandleMiddleware())

	v1.GET("status", api.v2.ServerStatus)
	v1.POST("log", api.v2.SetLogLevel)

	coordinatorMiddleware := middleware.ForwardToCoordinatorMiddleware(api.server)
	authenticateMiddleware := middleware.AuthenticateMiddleware(api.server)
	v1.GET("health", coordinatorMiddleware, api.v2.ServerHealth)

	// changefeed API
	changefeedGroup := v1.Group("/changefeeds")
	changefeedGroup.GET("", coordinatorMiddleware, api.v2.ListChangeFeeds)
	changefeedGroup.GET("/:changefeed_id", coordinatorMiddleware, api.v2.GetChangeFeed)

	// These two APIs need to be adjusted to be compatible with the API v1.
	changefeedGroup.POST("", coordinatorMiddleware, authenticateMiddleware, api.createChangefeed)
	changefeedGroup.PUT("/:changefeed_id", coordinatorMiddleware, authenticateMiddleware, api.updateChangefeed)

	changefeedGroup.POST("/:changefeed_id/pause", coordinatorMiddleware, authenticateMiddleware, api.v2.PauseChangefeed, adjustStatusForV1)
	changefeedGroup.POST("/:changefeed_id/resume", coordinatorMiddleware, authenticateMiddleware, api.v2.ResumeChangefeed, adjustStatusForV1)
	changefeedGroup.DELETE("/:changefeed_id", coordinatorMiddleware, authenticateMiddleware, api.v2.DeleteChangefeed, adjustStatusForV1)

	// These two APIs are not useful in new arch cdc, we implement them for compatibility with old arch cdc only.
	changefeedGroup.POST("/:changefeed_id/tables/rebalance_table", coordinatorMiddleware, authenticateMiddleware, api.rebalanceTables)
	changefeedGroup.POST("/:changefeed_id/tables/move_table", coordinatorMiddleware, authenticateMiddleware, api.moveTable)

	// owner API
	ownerGroup := v1.Group("/owner")
	ownerGroup.POST("/resign", coordinatorMiddleware, api.v2.ResignOwner, adjustStatusForV1)

	// processor API
	processorGroup := v1.Group("/processors")
	processorGroup.GET("", coordinatorMiddleware, api.v2.ListProcessor)
	processorGroup.GET("/:changefeed_id/:capture_id",
		coordinatorMiddleware, api.v2.GetProcessor)

	// capture API
	captureGroup := v1.Group("/captures")
	captureGroup.Use(coordinatorMiddleware)
	captureGroup.GET("", api.v2.ListCaptures)
	// This API need to be adjusted to be compatible with the API v1.
	captureGroup.PUT("/drain", api.drainCapture)
}

func (o *OpenAPIV1) createChangefeed(c *gin.Context) {
	var changefeedConfig changefeedConfig
	if err := c.BindJSON(&changefeedConfig); err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.Wrap(err))
		return
	}

	// rewrite the request body
	jsonData, err := json.Marshal(getV2ChangefeedConfig(changefeedConfig))
	if err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.Wrap(err))
		return
	}
	c.Request.Body = io.NopCloser(bytes.NewBuffer(jsonData))
	c.Request.ContentLength = int64(len(jsonData))

	muskURI, err := util.MaskSinkURI(changefeedConfig.SinkURI)
	if err != nil {
		log.Warn("failed to mask sink uri", zap.Error(err))
		muskURI = ""
	}
	changefeedConfig.SinkURI = muskURI
	log.Info("create changefeed api v1", zap.Any("changefeedConfig", changefeedConfig))

	o.v2.CreateChangefeed(c)
}

func (o *OpenAPIV1) updateChangefeed(c *gin.Context) {
	var changefeedConfig changefeedConfig
	if err := c.BindJSON(&changefeedConfig); err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.Wrap(err))
		return
	}

	// rewrite the request body
	jsonData, err := json.Marshal(getV2ChangefeedConfig(changefeedConfig))
	if err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.Wrap(err))
		return
	}
	c.Request.Body = io.NopCloser(bytes.NewBuffer(jsonData))
	c.Request.ContentLength = int64(len(jsonData))

	muskURI, err := util.MaskSinkURI(changefeedConfig.SinkURI)
	if err != nil {
		log.Warn("failed to mask sink uri", zap.Error(err))
		muskURI = ""
	}
	changefeedConfig.SinkURI = muskURI
	log.Info("update changefeed api v1", zap.Any("changefeedConfig", changefeedConfig))

	o.v2.UpdateChangefeed(c)
}

// moveTable moves a table to a specific capture.
// Usage:
// curl -X POST http://127.0.0.1:8300/api/v1/changefeeds/:changefeed_id/tables/move_table
// Body:
//
//	{
//		"table_id": 1,
//		"target_capture_id": "capture-1"
//	}
func (o *OpenAPIV1) moveTable(c *gin.Context) {
	data := moveTableReq{}
	err := c.BindJSON(&data)
	if err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.Wrap(err))
		return
	}
	url := c.Request.URL

	values := url.Query()
	values.Add("tableID", strconv.FormatInt(data.TableID, 10))
	values.Add("targetNodeID", data.CaptureID)
	url.RawQuery = values.Encode()

	c.Request.URL = url

	log.Info("api v1 moveTable", zap.Any("url", url.String()))

	o.v2.MoveTable(c)
	adjustStatusForV1(c)
}

// rebalanceTables is not useful in new arch cdc, we implement it for compatibility with old arch cdc only.
// Usage:
// curl -X POST http://127.0.0.1:8300/api/v1/changefeeds/:changefeed_id/tables/rebalance_table
func (o *OpenAPIV1) rebalanceTables(c *gin.Context) {
	log.Info("rebalanceTables API do nothing in new arch cdc currently, just return accepted")
	c.Status(http.StatusAccepted)
}

// drainCapture drains all tables from a capture.
// Usage:
// curl -X PUT http://127.0.0.1:8300/api/v1/captures/drain
// TODO: Implement this API in the future, currently it is a no-op.
func (o *OpenAPIV1) drainCapture(c *gin.Context) {
	var req drainCaptureRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.Wrap(err))
		return
	}
	drainCaptureCounter.Add(1)
	if drainCaptureCounter.Load()%10 == 0 {
		log.Info("api v1 drainCapture", zap.Any("captureID", req.CaptureID), zap.Int64("currentTableCount", drainCaptureCounter.Load()))
		c.JSON(http.StatusAccepted, &drainCaptureResp{
			CurrentTableCount: 10,
		})
	} else {
		log.Info("api v1 drainCapture done", zap.Any("captureID", req.CaptureID), zap.Int64("currentTableCount", drainCaptureCounter.Load()))
		c.JSON(http.StatusAccepted, &drainCaptureResp{
			CurrentTableCount: 0,
		})
	}
}

func getV2ChangefeedConfig(changefeedConfig changefeedConfig) *v2.ChangefeedConfig {
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Filter.Rules = changefeedConfig.FilterRules
	replicaConfig.Filter.IgnoreTxnStartTs = changefeedConfig.IgnoreTxnStartTs
	replicaConfig.Mounter.WorkerNum = changefeedConfig.MounterWorkerNum
	replicaConfig.Sink = changefeedConfig.SinkConfig
	replicaConfig.ForceReplicate = changefeedConfig.ForceReplicate
	replicaConfig.IgnoreIneligibleTable = changefeedConfig.IgnoreIneligibleTable

	return &v2.ChangefeedConfig{
		ID:            changefeedConfig.ID,
		Namespace:     changefeedConfig.Namespace,
		StartTs:       changefeedConfig.StartTS,
		TargetTs:      changefeedConfig.TargetTS,
		SinkURI:       changefeedConfig.SinkURI,
		ReplicaConfig: v2.ToAPIReplicaConfig(replicaConfig),
	}
}

type changefeedConfig struct {
	Namespace string `json:"namespace"`
	ID        string `json:"changefeed_id"`
	StartTS   uint64 `json:"start_ts"`
	TargetTS  uint64 `json:"target_ts"`
	SinkURI   string `json:"sink_uri"`
	// timezone used when checking sink uri
	TimeZone string `json:"timezone" default:"system"`
	// if true, force to replicate some ineligible tables
	ForceReplicate        bool               `json:"force_replicate" default:"false"`
	IgnoreIneligibleTable bool               `json:"ignore_ineligible_table" default:"false"`
	FilterRules           []string           `json:"filter_rules"`
	IgnoreTxnStartTs      []uint64           `json:"ignore_txn_start_ts"`
	MounterWorkerNum      int                `json:"mounter_worker_num" default:"16"`
	SinkConfig            *config.SinkConfig `json:"sink_config"`
}

type moveTableReq struct {
	TableID   int64  `json:"table_id"`
	CaptureID string `json:"capture_id"`
}

type drainCaptureRequest struct {
	CaptureID string `json:"capture_id"`
}

var drainCaptureCounter atomic.Int64

// drainCaptureResp is response for manual `DrainCapture`
type drainCaptureResp struct {
	CurrentTableCount int `json:"current_table_count"`
}
