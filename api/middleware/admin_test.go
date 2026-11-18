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

package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestAdminMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name               string
		clientUserRequired bool
		adminUsers         []string
		username           string
		authenticated      bool
		wantStatus         int
	}{
		{
			name:       "authentication disabled",
			wantStatus: http.StatusOK,
		},
		{
			name:               "admin user",
			clientUserRequired: true,
			adminUsers:         []string{"root"},
			username:           "root",
			authenticated:      true,
			wantStatus:         http.StatusOK,
		},
		{
			name:               "allowed but not admin user",
			clientUserRequired: true,
			adminUsers:         []string{"root"},
			username:           "alice",
			authenticated:      true,
			wantStatus:         http.StatusForbidden,
		},
		{
			name:               "empty admin user list",
			clientUserRequired: true,
			username:           "root",
			authenticated:      true,
			wantStatus:         http.StatusForbidden,
		},
	}

	originalConfig := config.GetGlobalServerConfig()
	t.Cleanup(func() { config.StoreGlobalServerConfig(originalConfig) })

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := originalConfig.Clone()
			cfg.Security.ClientUserRequired = tt.clientUserRequired
			cfg.Security.ClientAllowedUser = []string{"root", "alice"}
			cfg.Security.ClientAdminUser = tt.adminUsers
			config.StoreGlobalServerConfig(cfg)

			router := gin.New()
			if tt.authenticated {
				// Simulate a request that has already passed AuthenticateMiddleware.
				router.Use(func(c *gin.Context) { c.Set(ctxAuthenticatedKey, true) })
			}
			router.POST("/admin", AdminMiddleware(&mockServer{}), func(c *gin.Context) {
				c.Status(http.StatusOK)
			})

			request := httptest.NewRequest(http.MethodPost, "/admin", nil)
			if tt.username != "" {
				request.SetBasicAuth(tt.username, "secret")
			}
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)

			require.Equal(t, tt.wantStatus, response.Code, response.Body.String())
		})
	}
}
