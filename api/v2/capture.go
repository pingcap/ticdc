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

	"github.com/gin-gonic/gin"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
)

// ListCaptures lists all captures
// @Summary List captures
// @Description list all captures in cdc cluster
// @Tags capture,v2
// @Produce json
// @Success 200 {array} Capture
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v2/captures [get]
func (h *OpenAPIV2) ListCaptures(c *gin.Context) {
	info, err := h.server.SelfInfo()
	if err != nil {
		_ = c.Error(err)
		return
	}
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodes := nodeManager.GetAliveNodes()
	captures := make([]Capture, 0, len(nodes))
	for _, c := range nodes {
		captures = append(captures,
			Capture{
				ID:            c.ID.String(),
				IsCoordinator: c.ID == info.ID,
				AdvertiseAddr: c.AdvertiseAddr,
				ClusterID:     h.server.GetEtcdClient().GetClusterID(),
			})
	}
	c.JSON(http.StatusOK, toListResponse(c, captures))
}

// DrainCapture triggers drain operation for a capture
// @Summary Drain a capture
// @Description Drain all workloads from a capture before maintenance
// @Tags capture,v2
// @Accept json
// @Produce json
// @Param capture_id path string true "Capture ID"
// @Success 202 {object} DrainCaptureResponse
// @Failure 400,404,409,500 {object} model.HTTPError
// @Router /api/v2/captures/{capture_id}/drain [put]
func (h *OpenAPIV2) DrainCapture(c *gin.Context) {
	captureID := c.Param("capture_id")
	if captureID == "" {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStackByArgs("capture_id is required"))
		return
	}

	// Check if current node is coordinator
	selfInfo, err := h.server.SelfInfo()
	if err != nil {
		_ = c.Error(err)
		return
	}

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)

	// Validate target capture exists
	targetNode := nodeManager.GetNodeInfo(node.ID(captureID))
	if targetNode == nil {
		_ = c.Error(errors.ErrCaptureNotExist.GenWithStackByArgs(captureID))
		return
	}

	// Check if target is coordinator
	if targetNode.ID == selfInfo.ID {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStackByArgs("cannot drain coordinator node"))
		return
	}

	// Check cluster has at least 2 nodes
	aliveNodes := nodeManager.GetAliveNodes()
	if len(aliveNodes) < 2 {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStackByArgs("at least 2 captures required for drain operation"))
		return
	}

	// Get coordinator and call DrainCapture
	coordinator, err := h.server.GetCoordinator()
	if err != nil {
		_ = c.Error(err)
		return
	}
	if coordinator == nil {
		_ = c.Error(errors.New("coordinator not ready"))
		return
	}

	resp, err := coordinator.DrainCapture(c.Request.Context(), node.ID(captureID))
	if err != nil {
		_ = c.Error(err)
		return
	}

	c.JSON(http.StatusAccepted, &DrainCaptureResponse{
		CurrentMaintainerCount: resp.CurrentMaintainerCount,
		CurrentDispatcherCount: resp.CurrentDispatcherCount,
	})
}

// GetDrainStatus queries the drain status of a capture
// @Summary Get drain status
// @Description Get the drain status and progress of a capture
// @Tags capture,v2
// @Produce json
// @Param capture_id path string true "Capture ID"
// @Success 200 {object} DrainStatusResponse
// @Failure 400,404,500 {object} model.HTTPError
// @Router /api/v2/captures/{capture_id}/drain [get]
func (h *OpenAPIV2) GetDrainStatus(c *gin.Context) {
	captureID := c.Param("capture_id")
	if captureID == "" {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStackByArgs("capture_id is required"))
		return
	}

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)

	// Validate target capture exists
	targetNode := nodeManager.GetNodeInfo(node.ID(captureID))
	if targetNode == nil {
		_ = c.Error(errors.ErrCaptureNotExist.GenWithStackByArgs(captureID))
		return
	}

	// Get coordinator and call GetDrainStatus
	coordinator, err := h.server.GetCoordinator()
	if err != nil {
		_ = c.Error(err)
		return
	}
	if coordinator == nil {
		_ = c.Error(errors.New("coordinator not ready"))
		return
	}

	resp, err := coordinator.GetDrainStatus(c.Request.Context(), node.ID(captureID))
	if err != nil {
		_ = c.Error(err)
		return
	}

	c.JSON(http.StatusOK, &DrainStatusResponse{
		IsDraining:               resp.IsDraining,
		DrainingCaptureID:        resp.DrainingCaptureID,
		RemainingMaintainerCount: resp.RemainingMaintainerCount,
		RemainingDispatcherCount: resp.RemainingDispatcherCount,
	})
}
