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
	"strings"

	"github.com/gin-gonic/gin"
	perrors "github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/logger"
	"github.com/pingcap/ticdc/pkg/util"
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

// SetRedactMode changes TiCDC log redaction mode dynamically.
//
// SECURITY FEATURE: This is a security-critical API with strict restrictions.
// Only allows transitions to MORE restrictive modes, never to less restrictive ones.
// Restriction hierarchy: OFF (least) < MARKER < ON (most restrictive)
//
// Allowed transitions:
//   - OFF → MARKER (increase restriction)
//   - OFF → ON (increase restriction)
//   - MARKER → ON (increase restriction)
//
// Rejected transitions:
//   - ON → MARKER (decrease restriction)
//   - ON → OFF (decrease restriction)
//   - MARKER → OFF (decrease restriction)
//
// Rationale: Enabling redaction is a security decision, typically required by
// compliance or security policies. Allowing it to be weakened at runtime could
// lead to accidental data leaks if triggered by mistake or malicious actors.
//
// @Summary Change TiCDC log redaction mode
// @Description Change log redaction mode. Only allows transitions to more restrictive modes (OFF→MARKER→ON).
// @Tags common,v2
// @Accept json
// @Produce json
// @Param redact_info_log body RedactModeReq true "redaction mode"
// @Success 200 {object} RedactModeResp
// @Failure 400 {object} model.HTTPError
// @Router	/api/v2/log/redact [post]
func (h *OpenAPIV2) SetRedactMode(c *gin.Context) {
	req := &RedactModeReq{}
	err := c.BindJSON(&req)
	if err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack("invalid redact mode: %s", err.Error()))
		return
	}

	// Parse and validate the requested redaction mode
	// API requires non-empty mode (unlike config where empty means "use default")
	// Trim whitespace for robustness (common for REST APIs)
	req.Mode = strings.TrimSpace(req.Mode)
	if req.Mode == "" {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack("missing required field 'redact_info_log': must be 'off', 'on', or 'marker'"))
		return
	}
	requestedMode, err := util.ParseRedactMode(req.Mode)
	if err != nil {
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack("%v", err))
		return
	}

	// SECURITY: Define restriction levels (higher value = more restrictive)
	// Unknown modes treated as ON (level 2) for defense-in-depth - most restrictive
	getRestrictionLevel := func(mode string) int {
		switch mode {
		case perrors.RedactLogDisable:
			return 0 // OFF - no redaction
		case perrors.RedactLogMarker:
			return 1 // MARKER - wrap with markers
		case perrors.RedactLogEnable:
			return 2 // ON - full redaction
		default:
			return 2 // Unknown treated as ON (most restrictive)
		}
	}

	requestedLevel := getRestrictionLevel(requestedMode)

	// SECURITY: Use atomic CAS loop to prevent race conditions.
	// Without CAS, concurrent requests could result in a less restrictive mode winning.
	// Example race without CAS:
	//   Thread A (ON): Load OFF, passes check, Store ON
	//   Thread B (MARKER): Load OFF (stale), passes check, Store MARKER (overwrites ON!)
	// With CAS, Thread B's CompareAndSwap would fail and retry with the new current value.
	const maxRetries = 10
	var previousMode string
	for i := 0; i < maxRetries; i++ {
		// Get current redaction mode - handle empty case (not yet initialized)
		currentMode := perrors.RedactLogEnabled.Load()
		if currentMode == "" {
			currentMode = perrors.RedactLogDisable // Default to OFF if not set
		}

		currentLevel := getRestrictionLevel(currentMode)

		// Warn if current mode is unknown (should not happen in normal operation)
		if currentMode != perrors.RedactLogDisable &&
			currentMode != perrors.RedactLogMarker &&
			currentMode != perrors.RedactLogEnable {
			log.Warn("unknown redaction mode detected, treating as ON (most restrictive)",
				zap.String("unknownMode", currentMode))
		}

		// SECURITY: Only allow transitions to MORE restrictive modes (or same mode for idempotency)
		// Reject any attempt to decrease restriction level
		if requestedLevel < currentLevel {
			_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack(
				"cannot transition from '%s' to '%s': only transitions to more restrictive modes are allowed (OFF→MARKER→ON)",
				currentMode, req.Mode))
			return
		}

		// Same mode - no change needed, return success for idempotency
		if requestedMode == currentMode {
			c.JSON(http.StatusOK, &RedactModeResp{
				PreviousMode: currentMode,
				CurrentMode:  requestedMode,
			})
			return
		}

		// Atomically update only if current state hasn't changed
		// Handle empty string case: if currentMode was defaulted from empty, try swapping from empty first
		swapped := perrors.RedactLogEnabled.CompareAndSwap(currentMode, requestedMode)
		if !swapped && currentMode == perrors.RedactLogDisable {
			// Try swapping from empty string (uninitialized state)
			swapped = perrors.RedactLogEnabled.CompareAndSwap("", requestedMode)
		}

		if swapped {
			previousMode = currentMode
			break
		}
		// CAS failed - another goroutine modified the value, retry with new current value
		// This ensures the "most restrictive wins" property under concurrent requests
	}

	if previousMode == "" {
		// maxRetries exceeded - this should be extremely rare (requires sustained contention)
		_ = c.Error(errors.ErrAPIInvalidParam.GenWithStack(
			"failed to update redaction mode after %d retries due to concurrent modifications", maxRetries))
		return
	}

	log.Info("log redaction mode changed",
		zap.String("from", previousMode),
		zap.String("to", requestedMode))
	c.JSON(http.StatusOK, &RedactModeResp{
		PreviousMode: previousMode,
		CurrentMode:  requestedMode,
	})
}
