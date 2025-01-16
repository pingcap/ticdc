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
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/ticdc/logservice/txnutil"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/txnutil/gc"
	"github.com/tikv/client-go/v2/tikv"
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
	kvStorage := h.server.GetKVStorage()

	if kvStorage == nil {
		c.Status(http.StatusServiceUnavailable)
		return
	}

	txnResolver := txnutil.NewLockerResolver(kvStorage.(tikv.Storage))
	if err := txnResolver.Resolve(c, resolveLockReq.RegionID, resolveLockReq.Ts); err != nil {
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

	err := gc.RemoveServiceGCSafepoint(c, pdClient,
		h.server.GetEtcdClient().GetGCServiceID())
	if err != nil {
		_ = c.Error(cerror.WrapError(cerror.ErrInternalServerError, err))
	}
	c.JSON(http.StatusOK, &EmptyResponse{})
}
