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
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/ticdc/logservice/logpuller"
	apiutil "github.com/pingcap/ticdc/pkg/api"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	cerror "github.com/pingcap/ticdc/pkg/errors"
)

const (
	defaultPullerDebugLimit = 1
	maxPullerDebugLimit     = 20
)

// GetPullerDebugInfo returns the slowest local subscriptions and Regions.
func (h *OpenAPIV2) GetPullerDebugInfo(c *gin.Context) {
	provider, ok := h.getPullerDebugProvider()
	if !ok {
		writePullerDebugError(c, http.StatusServiceUnavailable, "log puller is not initialized")
		return
	}
	subscriptionLimit, ok := parsePullerDebugLimit(c, "subscription_limit")
	if !ok {
		return
	}
	regionLimit, ok := parsePullerDebugLimit(c, "region_limit")
	if !ok {
		return
	}
	c.IndentedJSON(http.StatusOK, provider.GetPullerDebugInfo(logpuller.PullerDebugOptions{
		SubscriptionLimit: subscriptionLimit,
		RegionLimit:       regionLimit,
	}))
}

// GetPullerDebugRegion returns one Region owned by a local subscription.
func (h *OpenAPIV2) GetPullerDebugRegion(c *gin.Context) {
	provider, ok := h.getPullerDebugProvider()
	if !ok {
		writePullerDebugError(c, http.StatusServiceUnavailable, "log puller is not initialized")
		return
	}
	subID, ok := parsePullerDebugUint(c, "subscription_id")
	if !ok {
		return
	}
	regionID, ok := parsePullerDebugUint(c, "region_id")
	if !ok {
		return
	}
	detail, found := provider.GetPullerDebugRegion(
		logpuller.SubscriptionID(subID), regionID)
	if !found {
		writePullerDebugError(c, http.StatusNotFound, "puller region not found")
		return
	}
	c.IndentedJSON(http.StatusOK, detail)
}

func (h *OpenAPIV2) getPullerDebugProvider() (logpuller.DebugInfoProvider, bool) {
	if h.pullerDebugProvider != nil {
		return h.pullerDebugProvider, true
	}
	return appcontext.TryGetService[logpuller.DebugInfoProvider](appcontext.SubscriptionClient)
}

func parsePullerDebugLimit(c *gin.Context, name string) (int, bool) {
	raw := strings.TrimSpace(c.Query(name))
	if raw == "" {
		return defaultPullerDebugLimit, true
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 || value > maxPullerDebugLimit {
		writePullerDebugInvalidParam(c, name+" must be between 1 and 20")
		return 0, false
	}
	return value, true
}

func parsePullerDebugUint(c *gin.Context, name string) (uint64, bool) {
	raw := strings.TrimSpace(c.Param(name))
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || value == 0 {
		writePullerDebugInvalidParam(c, name+" must be a positive integer")
		return 0, false
	}
	return value, true
}

func writePullerDebugInvalidParam(c *gin.Context, message string) {
	err := cerror.ErrAPIInvalidParam.GenWithStackByArgs(message)
	c.IndentedJSON(http.StatusBadRequest, apiutil.NewHTTPError(err))
}

func writePullerDebugError(c *gin.Context, status int, message string) {
	c.IndentedJSON(status, apiutil.HTTPError{Error: message})
}
