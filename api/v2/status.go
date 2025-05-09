// Copyright 2023 PingCAP, Inc.
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
	"os"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/ticdc/pkg/version"
)

// ServerStatus Get the status information of a TiCDC node
// @Summary Get the status information of a TiCDC node
// @Description This API is a synchronous interface. If the request is successful,
// the status information of the corresponding node is returned.
//
// @Tags common,v2
// @Accept json
// @Produce json
// @Success 200 {object} ServerStatus
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v2/status [get]
func (h *OpenAPIV2) ServerStatus(c *gin.Context) {
	info, err := h.server.SelfInfo()
	if err != nil {
		_ = c.Error(err)
		return
	}
	etcdClient := h.server.GetEtcdClient()
	status := ServerStatus{
		Version:   version.ReleaseVersion,
		GitHash:   version.GitHash,
		Pid:       os.Getpid(),
		ID:        string(info.ID),
		ClusterID: etcdClient.GetClusterID(),
		IsOwner:   h.server.IsCoordinator(),
		Liveness:  h.server.Liveness(),
	}
	c.IndentedJSON(http.StatusOK, status)
}
