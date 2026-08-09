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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/ticdc/logservice/logpuller"
	apiutil "github.com/pingcap/ticdc/pkg/api"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	cerror "github.com/pingcap/ticdc/pkg/errors"
)

const (
	defaultPullerDebugListLimit = 100
	maxPullerDebugListLimit     = 1000
)

type pullerSubscriptionListResponse struct {
	SnapshotAt  time.Time                               `json:"snapshot_at"`
	Items       []logpuller.PullerSubscriptionDebugInfo `json:"items"`
	NextAfterID logpuller.SubscriptionID                `json:"next_after_id,string,omitempty"`
}

type pullerStoreListResponse struct {
	SnapshotAt time.Time                        `json:"snapshot_at"`
	Items      []logpuller.PullerStoreDebugInfo `json:"items"`
}

// GetPullerDebugInfo returns a lightweight snapshot of this node's log puller.
func (h *OpenAPIV2) GetPullerDebugInfo(c *gin.Context) {
	provider, ok := h.getPullerDebugProvider()
	if !ok {
		writePullerDebugError(c, http.StatusServiceUnavailable, "log puller is not initialized")
		return
	}
	c.IndentedJSON(http.StatusOK, provider.GetPullerDebugInfo())
}

// ListPullerDebugSubscriptions lists local puller subscriptions.
func (h *OpenAPIV2) ListPullerDebugSubscriptions(c *gin.Context) {
	provider, ok := h.getPullerDebugProvider()
	if !ok {
		writePullerDebugError(c, http.StatusServiceUnavailable, "log puller is not initialized")
		return
	}

	limit, ok := parsePullerDebugLimit(c, "limit", defaultPullerDebugListLimit)
	if !ok {
		return
	}
	afterID, ok := parsePullerDebugSubscriptionID(c, "after_id", false)
	if !ok {
		return
	}
	state := strings.TrimSpace(c.DefaultQuery("state", "all"))
	if state != "all" && state != "initialized" && state != "uninitialized" && state != "stopping" {
		writePullerDebugInvalidParam(c, "state must be one of all, initialized, uninitialized, or stopping")
		return
	}
	sortBy := strings.TrimSpace(c.DefaultQuery("sort", "subscription_id"))
	if sortBy != "subscription_id" && sortBy != "resolved_ts_lag" {
		writePullerDebugInvalidParam(c, "sort must be subscription_id or resolved_ts_lag")
		return
	}
	if sortBy != "subscription_id" && afterID != logpuller.InvalidSubscriptionID {
		writePullerDebugInvalidParam(c, "after_id is only supported when sorting by subscription_id")
		return
	}

	var tableID *int64
	if rawTableID := strings.TrimSpace(c.Query("table_id")); rawTableID != "" {
		parsed, err := strconv.ParseInt(rawTableID, 10, 64)
		if err != nil {
			writePullerDebugInvalidParam(c, "table_id must be a signed integer")
			return
		}
		tableID = &parsed
	}

	items := provider.GetPullerDebugSubscriptions()
	filtered := make([]logpuller.PullerSubscriptionDebugInfo, 0, min(len(items), limit+1))
	for _, item := range items {
		if item.SubscriptionID <= afterID || !matchesPullerSubscriptionState(item, state) {
			continue
		}
		if tableID != nil && item.TableID != *tableID {
			continue
		}
		filtered = append(filtered, item)
	}
	if sortBy == "resolved_ts_lag" {
		sort.Slice(filtered, func(i, j int) bool {
			if filtered[i].ResolvedTsLagMillis == filtered[j].ResolvedTsLagMillis {
				return filtered[i].SubscriptionID < filtered[j].SubscriptionID
			}
			return filtered[i].ResolvedTsLagMillis > filtered[j].ResolvedTsLagMillis
		})
	}

	response := pullerSubscriptionListResponse{
		SnapshotAt: time.Now(),
		Items:      filtered,
	}
	if len(response.Items) > limit {
		response.Items = response.Items[:limit]
		if sortBy == "subscription_id" {
			response.NextAfterID = response.Items[len(response.Items)-1].SubscriptionID
		}
	}
	c.IndentedJSON(http.StatusOK, response)
}

// GetPullerDebugSubscription returns one subscription and optional Region details.
func (h *OpenAPIV2) GetPullerDebugSubscription(c *gin.Context) {
	provider, ok := h.getPullerDebugProvider()
	if !ok {
		writePullerDebugError(c, http.StatusServiceUnavailable, "log puller is not initialized")
		return
	}
	subID, ok := parsePullerDebugSubscriptionID(c, "subscription_id", true)
	if !ok {
		return
	}
	regionMode := strings.TrimSpace(c.DefaultQuery("regions", "none"))
	if regionMode != "none" && regionMode != "slow" && regionMode != "uninitialized" && regionMode != "all" {
		writePullerDebugInvalidParam(c, "regions must be one of none, slow, uninitialized, or all")
		return
	}
	regionLimit, ok := parsePullerDebugLimit(c, "region_limit", defaultPullerDebugListLimit)
	if !ok {
		return
	}
	includeKeys := false
	if rawIncludeKeys := strings.TrimSpace(c.Query("include_keys")); rawIncludeKeys != "" {
		includeKeys, _ = strconv.ParseBool(rawIncludeKeys)
		if rawIncludeKeys != "true" && rawIncludeKeys != "false" {
			writePullerDebugInvalidParam(c, "include_keys must be true or false")
			return
		}
	}

	detail, found := provider.GetPullerDebugSubscription(subID, logpuller.PullerSubscriptionDebugOptions{
		RegionMode:  regionMode,
		RegionLimit: regionLimit,
		IncludeKeys: includeKeys,
	})
	if !found {
		writePullerDebugError(c, http.StatusNotFound, "puller subscription not found")
		return
	}
	c.IndentedJSON(http.StatusOK, detail)
}

// ListPullerDebugStores lists local TiKV request stores.
func (h *OpenAPIV2) ListPullerDebugStores(c *gin.Context) {
	provider, ok := h.getPullerDebugProvider()
	if !ok {
		writePullerDebugError(c, http.StatusServiceUnavailable, "log puller is not initialized")
		return
	}
	c.IndentedJSON(http.StatusOK, pullerStoreListResponse{
		SnapshotAt: time.Now(),
		Items:      provider.GetPullerDebugStores(),
	})
}

// GetPullerDebugStore returns per-worker queue sizes for one TiKV address.
func (h *OpenAPIV2) GetPullerDebugStore(c *gin.Context) {
	provider, ok := h.getPullerDebugProvider()
	if !ok {
		writePullerDebugError(c, http.StatusServiceUnavailable, "log puller is not initialized")
		return
	}
	address := strings.TrimSpace(c.Param("store_address"))
	if address == "" {
		writePullerDebugInvalidParam(c, "store_address is required")
		return
	}
	store, found := provider.GetPullerDebugStore(address)
	if !found {
		writePullerDebugError(c, http.StatusNotFound, "puller store not found")
		return
	}
	c.IndentedJSON(http.StatusOK, store)
}

func (h *OpenAPIV2) getPullerDebugProvider() (logpuller.DebugInfoProvider, bool) {
	if h.pullerDebugProvider != nil {
		return h.pullerDebugProvider, true
	}
	return appcontext.TryGetService[logpuller.DebugInfoProvider](appcontext.SubscriptionClient)
}

func parsePullerDebugLimit(c *gin.Context, name string, defaultValue int) (int, bool) {
	raw := strings.TrimSpace(c.Query(name))
	if raw == "" {
		return defaultValue, true
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 || value > maxPullerDebugListLimit {
		writePullerDebugInvalidParam(c, name+" must be between 1 and 1000")
		return 0, false
	}
	return value, true
}

func parsePullerDebugSubscriptionID(
	c *gin.Context,
	name string,
	pathParameter bool,
) (logpuller.SubscriptionID, bool) {
	raw := c.Query(name)
	if pathParameter {
		raw = c.Param(name)
	}
	raw = strings.TrimSpace(raw)
	if raw == "" && !pathParameter {
		return logpuller.InvalidSubscriptionID, true
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || value == 0 {
		writePullerDebugInvalidParam(c, name+" must be a positive integer")
		return logpuller.InvalidSubscriptionID, false
	}
	return logpuller.SubscriptionID(value), true
}

func matchesPullerSubscriptionState(
	item logpuller.PullerSubscriptionDebugInfo,
	state string,
) bool {
	switch state {
	case "initialized":
		return item.Initialized && !item.Stopped
	case "uninitialized":
		return !item.Initialized && !item.Stopped
	case "stopping":
		return item.Stopped
	default:
		return true
	}
}

func writePullerDebugInvalidParam(c *gin.Context, message string) {
	err := cerror.ErrAPIInvalidParam.GenWithStackByArgs(message)
	c.IndentedJSON(http.StatusBadRequest, apiutil.NewHTTPError(err))
}

func writePullerDebugError(c *gin.Context, status int, message string) {
	c.IndentedJSON(status, apiutil.HTTPError{Error: message})
}
