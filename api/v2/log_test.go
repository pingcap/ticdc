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
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	perrors "github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

func TestSetRedactMode(t *testing.T) {
	tests := []struct {
		name             string
		initialMode      string // Mode to set before running test
		requestBody      interface{}
		expectedStatus   int
		expectedMode     string // Expected mode after operation
		expectError      bool
		errorContains    string // Substring expected in error message
		expectedPrevMode string // Expected previous_mode in response
		expectedCurrMode string // Expected current_mode in response
	}{
		// Valid transitions - increasing restriction
		{
			name:             "OFF -> MARKER (success, increase restriction)",
			initialMode:      perrors.RedactLogDisable,
			requestBody:      RedactModeReq{Mode: "marker"},
			expectedStatus:   http.StatusOK,
			expectedMode:     perrors.RedactLogMarker,
			expectError:      false,
			expectedPrevMode: perrors.RedactLogDisable,
			expectedCurrMode: perrors.RedactLogMarker,
		},
		{
			name:             "OFF -> ON (success, increase restriction)",
			initialMode:      perrors.RedactLogDisable,
			requestBody:      RedactModeReq{Mode: "on"},
			expectedStatus:   http.StatusOK,
			expectedMode:     perrors.RedactLogEnable,
			expectError:      false,
			expectedPrevMode: perrors.RedactLogDisable,
			expectedCurrMode: perrors.RedactLogEnable,
		},
		{
			name:             "MARKER -> ON (success, increase restriction)",
			initialMode:      perrors.RedactLogMarker,
			requestBody:      RedactModeReq{Mode: "on"},
			expectedStatus:   http.StatusOK,
			expectedMode:     perrors.RedactLogEnable,
			expectError:      false,
			expectedPrevMode: perrors.RedactLogMarker,
			expectedCurrMode: perrors.RedactLogEnable,
		},
		{
			name:             "OFF -> ON with uppercase (success)",
			initialMode:      perrors.RedactLogDisable,
			requestBody:      RedactModeReq{Mode: "ON"},
			expectedStatus:   http.StatusOK,
			expectedMode:     perrors.RedactLogEnable,
			expectError:      false,
			expectedPrevMode: perrors.RedactLogDisable,
			expectedCurrMode: perrors.RedactLogEnable,
		},
		{
			name:             "OFF -> ON with 'true' alias (success)",
			initialMode:      perrors.RedactLogDisable,
			requestBody:      RedactModeReq{Mode: "true"},
			expectedStatus:   http.StatusOK,
			expectedMode:     perrors.RedactLogEnable,
			expectError:      false,
			expectedPrevMode: perrors.RedactLogDisable,
			expectedCurrMode: perrors.RedactLogEnable,
		},
		// Idempotent transitions - same level (allowed)
		{
			name:             "OFF -> OFF (success, idempotent)",
			initialMode:      perrors.RedactLogDisable,
			requestBody:      RedactModeReq{Mode: "off"},
			expectedStatus:   http.StatusOK,
			expectedMode:     perrors.RedactLogDisable,
			expectError:      false,
			expectedPrevMode: perrors.RedactLogDisable,
			expectedCurrMode: perrors.RedactLogDisable,
		},
		{
			name:             "MARKER -> MARKER (success, idempotent)",
			initialMode:      perrors.RedactLogMarker,
			requestBody:      RedactModeReq{Mode: "marker"},
			expectedStatus:   http.StatusOK,
			expectedMode:     perrors.RedactLogMarker,
			expectError:      false,
			expectedPrevMode: perrors.RedactLogMarker,
			expectedCurrMode: perrors.RedactLogMarker,
		},
		{
			name:             "ON -> ON (success, idempotent)",
			initialMode:      perrors.RedactLogEnable,
			requestBody:      RedactModeReq{Mode: "on"},
			expectedStatus:   http.StatusOK,
			expectedMode:     perrors.RedactLogEnable,
			expectError:      false,
			expectedPrevMode: perrors.RedactLogEnable,
			expectedCurrMode: perrors.RedactLogEnable,
		},
		// Invalid transitions - decreasing restriction
		{
			name:           "ON -> MARKER (rejected, decrease restriction)",
			initialMode:    perrors.RedactLogEnable,
			requestBody:    RedactModeReq{Mode: "marker"},
			expectedStatus: http.StatusOK,
			expectedMode:   perrors.RedactLogEnable, // No change
			expectError:    true,
			errorContains:  "only transitions to more restrictive modes are allowed",
		},
		{
			name:           "ON -> OFF (rejected, decrease restriction)",
			initialMode:    perrors.RedactLogEnable,
			requestBody:    RedactModeReq{Mode: "off"},
			expectedStatus: http.StatusOK,
			expectedMode:   perrors.RedactLogEnable, // No change
			expectError:    true,
			errorContains:  "only transitions to more restrictive modes are allowed",
		},
		{
			name:           "MARKER -> OFF (rejected, decrease restriction)",
			initialMode:    perrors.RedactLogMarker,
			requestBody:    RedactModeReq{Mode: "off"},
			expectedStatus: http.StatusOK,
			expectedMode:   perrors.RedactLogMarker, // No change
			expectError:    true,
			errorContains:  "only transitions to more restrictive modes are allowed",
		},
		// Edge cases
		{
			name:           "invalid mode string",
			initialMode:    perrors.RedactLogDisable,
			requestBody:    RedactModeReq{Mode: "invalid"},
			expectedStatus: http.StatusOK,
			expectedMode:   perrors.RedactLogDisable, // No change
			expectError:    true,
			errorContains:  "invalid redact mode",
		},
		{
			name:           "invalid JSON",
			initialMode:    perrors.RedactLogDisable,
			requestBody:    "invalid json",
			expectedStatus: http.StatusBadRequest,
			expectedMode:   perrors.RedactLogDisable, // No change
			expectError:    true,
		},
		{
			name:             "empty initial mode -> ON (success, handles unset state)",
			initialMode:      "", // Empty string in state means uninitialized, defaults to OFF
			requestBody:      RedactModeReq{Mode: "on"},
			expectedStatus:   http.StatusOK,
			expectedMode:     perrors.RedactLogEnable,
			expectError:      false,
			expectedPrevMode: perrors.RedactLogDisable, // Empty defaults to OFF
			expectedCurrMode: perrors.RedactLogEnable,
		},
		{
			name:           "missing redact_info_log field (rejected)",
			initialMode:    perrors.RedactLogDisable,
			requestBody:    RedactModeReq{}, // Empty struct - no Mode set
			expectedStatus: http.StatusOK,
			expectedMode:   perrors.RedactLogDisable, // No change
			expectError:    true,
			errorContains:  "missing required field 'redact_info_log'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save original mode to restore after test
			originalMode := perrors.RedactLogEnabled.Load()
			defer perrors.RedactLogEnabled.Store(originalMode)

			// Set initial state
			if tt.initialMode == "" {
				perrors.RedactLogEnabled.Store("") // Empty string defaults to OFF
			} else {
				perrors.RedactLogEnabled.Store(tt.initialMode)
			}

			// Setup gin context
			gin.SetMode(gin.TestMode)
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)

			// Create request body
			var bodyBytes []byte
			if str, ok := tt.requestBody.(string); ok {
				bodyBytes = []byte(str)
			} else {
				var err error
				bodyBytes, err = json.Marshal(tt.requestBody)
				require.NoError(t, err)
			}

			// Create test request
			req := httptest.NewRequest("POST", "/api/v2/log/redact", bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			c.Request = req

			// Create handler instance
			h := &OpenAPIV2{}

			// Call the handler
			h.SetRedactMode(c)

			// Verify response status
			require.Equal(t, tt.expectedStatus, w.Code)

			// Verify error handling
			if tt.expectError {
				require.NotEmpty(t, c.Errors)
				if tt.errorContains != "" {
					require.Contains(t, c.Errors.String(), tt.errorContains)
				}
			} else {
				require.Empty(t, c.Errors)
			}

			// Verify mode after operation
			if tt.expectedMode != "" {
				actualMode := perrors.RedactLogEnabled.Load()
				require.Equal(t, tt.expectedMode, actualMode,
					"Expected mode %s but got %s", tt.expectedMode, actualMode)
			}

			// Verify response body for success cases
			if !tt.expectError {
				var response RedactModeResp
				err := json.Unmarshal(w.Body.Bytes(), &response)
				require.NoError(t, err)
				require.Equal(t, tt.expectedPrevMode, response.PreviousMode,
					"Expected previous_mode %s but got %s", tt.expectedPrevMode, response.PreviousMode)
				require.Equal(t, tt.expectedCurrMode, response.CurrentMode,
					"Expected current_mode %s but got %s", tt.expectedCurrMode, response.CurrentMode)
			}
		})
	}
}

func TestSetRedactModeSecurityRestriction(t *testing.T) {
	// Test security restriction: Only allow transitions to more restrictive modes
	originalMode := perrors.RedactLogEnabled.Load()
	defer perrors.RedactLogEnabled.Store(originalMode)

	h := &OpenAPIV2{}
	gin.SetMode(gin.TestMode)

	// Step 1: Start with OFF mode, transition to MARKER
	perrors.RedactLogEnabled.Store(perrors.RedactLogDisable)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	body, err := json.Marshal(RedactModeReq{Mode: "marker"})
	require.NoError(t, err)
	req := httptest.NewRequest("POST", "/api/v2/log/redact", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	c.Request = req

	h.SetRedactMode(c)

	require.Equal(t, http.StatusOK, w.Code)
	require.Empty(t, c.Errors)
	require.Equal(t, perrors.RedactLogMarker, perrors.RedactLogEnabled.Load())

	// Verify response body
	var resp1 RedactModeResp
	err = json.Unmarshal(w.Body.Bytes(), &resp1)
	require.NoError(t, err)
	require.Equal(t, perrors.RedactLogDisable, resp1.PreviousMode)
	require.Equal(t, perrors.RedactLogMarker, resp1.CurrentMode)

	// Step 2: Try to downgrade from MARKER to OFF - should fail
	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	body, err = json.Marshal(RedactModeReq{Mode: "off"})
	require.NoError(t, err)
	req = httptest.NewRequest("POST", "/api/v2/log/redact", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	c.Request = req

	h.SetRedactMode(c)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotEmpty(t, c.Errors)
	require.Contains(t, c.Errors.String(), "only transitions to more restrictive modes are allowed")
	// Mode should remain MARKER
	require.Equal(t, perrors.RedactLogMarker, perrors.RedactLogEnabled.Load())

	// Step 3: Upgrade from MARKER to ON - should succeed
	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	body, err = json.Marshal(RedactModeReq{Mode: "on"})
	require.NoError(t, err)
	req = httptest.NewRequest("POST", "/api/v2/log/redact", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	c.Request = req

	h.SetRedactMode(c)

	require.Equal(t, http.StatusOK, w.Code)
	require.Empty(t, c.Errors)
	require.Equal(t, perrors.RedactLogEnable, perrors.RedactLogEnabled.Load())

	// Verify response body
	var resp3 RedactModeResp
	err = json.Unmarshal(w.Body.Bytes(), &resp3)
	require.NoError(t, err)
	require.Equal(t, perrors.RedactLogMarker, resp3.PreviousMode)
	require.Equal(t, perrors.RedactLogEnable, resp3.CurrentMode)

	// Step 4: Try to downgrade from ON to MARKER - should fail
	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	body, err = json.Marshal(RedactModeReq{Mode: "marker"})
	require.NoError(t, err)
	req = httptest.NewRequest("POST", "/api/v2/log/redact", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	c.Request = req

	h.SetRedactMode(c)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotEmpty(t, c.Errors)
	require.Contains(t, c.Errors.String(), "only transitions to more restrictive modes are allowed")
	// Mode should remain ON
	require.Equal(t, perrors.RedactLogEnable, perrors.RedactLogEnabled.Load())
}

func TestSetRedactModeConcurrency(t *testing.T) {
	// Test concurrent attempts to change redaction mode
	// With security restriction, only increasing restriction levels succeed
	originalMode := perrors.RedactLogEnabled.Load()
	defer perrors.RedactLogEnabled.Store(originalMode)

	// Start with OFF mode
	perrors.RedactLogEnabled.Store(perrors.RedactLogDisable)

	done := make(chan bool)
	h := &OpenAPIV2{}

	// Set gin mode once before concurrent tests to avoid data race
	gin.SetMode(gin.TestMode)

	// All goroutines try to transition to different modes
	// Some to MARKER, some to ON
	for i := 0; i < 10; i++ {
		go func(iteration int) {
			mode := "on"
			if iteration%3 == 0 {
				mode = "marker"
			}

			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)

			body, _ := json.Marshal(RedactModeReq{Mode: mode})
			req := httptest.NewRequest("POST", "/api/v2/log/redact", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			c.Request = req

			h.SetRedactMode(c)

			require.Equal(t, http.StatusOK, w.Code)
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Final mode should be either MARKER or ON (both more restrictive than OFF)
	finalMode := perrors.RedactLogEnabled.Load()
	require.True(t,
		finalMode == perrors.RedactLogMarker || finalMode == perrors.RedactLogEnable,
		"Expected redaction to be at more restrictive level after concurrent calls")
}
