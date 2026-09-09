// Copyright 2025 PingCAP, Inc.
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

package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	meteringconfig "github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestRegisterRoutesConfig(t *testing.T) {
	gin.SetMode(gin.TestMode)

	originalConfig := config.GetGlobalServerConfig()
	defer config.StoreGlobalServerConfig(originalConfig)

	testConfig := originalConfig.Clone()
	testConfig.DataDir = "/tmp/codex-test-data-dir"
	testConfig.Metering = meteringconfig.NewMeteringConfig().WithAWSConfig(&meteringconfig.MeteringAWSConfig{
		AccessKey: "metering-access", SecretAccessKey: "metering-secret", SessionToken: "metering-token",
	})
	config.StoreGlobalServerConfig(testConfig)

	originalGatherer := prometheus.DefaultGatherer
	defer func() {
		prometheus.DefaultGatherer = originalGatherer
	}()

	router := gin.New()
	RegisterRoutes(router, nil, prometheus.NewRegistry())

	req := httptest.NewRequest(http.MethodGet, "/config", nil)
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &payload))
	require.Equal(t, testConfig.DataDir, payload["data-dir"])
	require.NotContains(t, payload, "metering")
	for _, secret := range []string{"metering-access", "metering-secret", "metering-token"} {
		require.NotContains(t, resp.Body.String(), secret)
	}
	require.Equal(t, "metering-secret", config.GetGlobalServerConfig().Metering.AWS.SecretAccessKey)
}
