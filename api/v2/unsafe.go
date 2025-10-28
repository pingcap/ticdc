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
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/logservice/txnutil"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/keyspace"
	"github.com/pingcap/ticdc/pkg/txnutil/gc"
	"go.uber.org/zap"
)

// CDCMetaData returns all etcd key values used by cdc
func (h *OpenAPIV2) CDCMetaData(c *gin.Context) {
	kvs, err := h.server.GetEtcdClient().GetAllCDCInfo(c)
	if err != nil {
		_ = c.Error(err)
		return
	}
	resp := make([]EtcdData, 0, len(kvs))
	for _, pair := range kvs {
		resp = append(resp, EtcdData{
			Key:   string(pair.Key),
			Value: string(pair.Value),
		})
	}
	c.IndentedJSON(http.StatusOK, resp)
}

// ResolveLock resolve locks in regions
func (h *OpenAPIV2) ResolveLock(c *gin.Context) {
	var resolveLockReq ResolveLockReq
	if err := c.BindJSON(&resolveLockReq); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.Wrap(err))
		return
	}

	keyspaceName := GetKeyspaceValueWithDefault(c)

	keyspaceManager := appcontext.GetService[keyspace.KeyspaceManager](appcontext.KeyspaceManager)
	// The ctx's lifecycle is the same as the HTTP request.
	// The schema store may use the context to fetch database information asynchronously.
	// Therefore, we cannot use the context of the HTTP request.
	// We create a new context here.
	schemaCxt := context.Background()
	keyspaceMeta, err := keyspaceManager.LoadKeyspace(schemaCxt, keyspaceName)
	if err != nil {
		log.Error("LoadKeyspace failed", zap.String("keyspaceName", keyspaceName), zap.Error(err))
		_ = c.Error(err)
		return
	}

	txnResolver := txnutil.NewLockerResolver()
	if err := txnResolver.Resolve(schemaCxt, keyspaceMeta.Id, resolveLockReq.RegionID, resolveLockReq.Ts); err != nil {
		log.Error(
			"resolve lock failed",
			zap.Uint32("keyspaceID", keyspaceMeta.Id),
			zap.Uint64("regionID", resolveLockReq.RegionID),
			zap.Uint64("resolveLockTs", resolveLockReq.Ts),
			zap.Error(err),
		)
		_ = c.Error(err)
		return
	}
	c.JSON(http.StatusOK, &EmptyResponse{})
}

// DeleteServiceGcSafePoint Delete CDC service GC safepoint in PD
func (h *OpenAPIV2) DeleteServiceGcSafePoint(c *gin.Context) {
	upstreamConfig := &UpstreamConfig{}
	if err := c.BindJSON(upstreamConfig); err != nil {
		_ = c.Error(cerror.WrapError(cerror.ErrAPIInvalidParam, err))
		return
	}
	pdClient := h.server.GetPdClient()
	defer pdClient.Close()

	keyspaceName := GetKeyspaceValueWithDefault(c)
	keyspaceManager := appcontext.GetService[keyspace.KeyspaceManager](appcontext.KeyspaceManager)
	keyspaceMeta, err := keyspaceManager.LoadKeyspace(c.Request.Context(), keyspaceName)
	if err != nil {
		_ = c.Error(cerror.WrapError(cerror.ErrKeyspaceNotFound, err))
		return
	}

	err = gc.UnifyDeleteGcSafepoint(
		c,
		pdClient,
		keyspaceMeta.Id,
		h.server.GetEtcdClient().GetGCServiceID(),
	)
	if err != nil {
		_ = c.Error(cerror.WrapError(cerror.ErrInternalServerError, err))
	}
	c.JSON(http.StatusOK, &EmptyResponse{})
}
