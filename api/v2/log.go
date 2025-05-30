// Copyright 2024 PingCAP, Inc.
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
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/logger"
	"go.uber.org/zap"
)

// SetLogLevel changes TiCDC log level dynamically.
// @Summary Change TiCDC log level
// @Description change TiCDC log level dynamically
// @Tags common,v2
// @Accept json
// @Produce json
// @Param log_level body LogLevelReq true "log level"
// @Success 200 {object} EmptyResponse
// @Failure 400 {object} model.HTTPError
// @Router	/api/v2/log [post]
func (h *OpenAPIV2) SetLogLevel(c *gin.Context) {
	req := &LogLevelReq{Level: "info"}
	err := c.BindJSON(&req)
	if err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack("invalid log level: %s", err.Error()))
		return
	}

	err = logger.SetLogLevel(req.Level)
	if err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack(
			"fail to change log level: %s", req.Level))
		return
	}
	log.Warn("log level changed", zap.String("level", req.Level))
	c.JSON(http.StatusOK, &EmptyResponse{})
}
