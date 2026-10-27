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
	perrors "github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestRegisterRoutesConfig(t *testing.T) {
	gin.SetMode(gin.TestMode)

	originalConfig := config.GetGlobalServerConfig()
	defer config.StoreGlobalServerConfig(originalConfig)
	originalMode := perrors.RedactLogEnabled.Load()
	t.Cleanup(func() { perrors.RedactLogEnabled.Store(originalMode) })

	testConfig := originalConfig.Clone()
	testConfig.DataDir = "/tmp/codex-test-data-dir"
	testConfig.Encryption.KMS.AWS.AccessKey = "aws-access-secret"
	testConfig.Encryption.KMS.AWS.SecretAccessKey = "aws-private-secret"
	testConfig.Encryption.KMS.AWS.SessionToken = "aws-session-secret"
	testConfig.Encryption.KMS.AWS.Endpoint = "https://user:aws-endpoint-password@kms.example.com/path?" +
		"authorization=Bearer%20aws-authorization-secret&token=aws-endpoint-token&region=west"
	testConfig.Encryption.KMS.GCP.CredentialsJSON = `{"private_key":"gcp-private-secret"}`
	testConfig.Encryption.KMS.GCP.Endpoint = "https://user:gcp-endpoint-password@kms.gcp.example.com/path?access_token=gcp-endpoint-token"
	config.StoreGlobalServerConfig(testConfig)

	originalGatherer := prometheus.DefaultGatherer
	defer func() {
		prometheus.DefaultGatherer = originalGatherer
	}()

	router := gin.New()
	RegisterRoutes(router, nil, prometheus.NewRegistry())

	for _, mode := range []string{perrors.RedactLogDisable, perrors.RedactLogMarker, perrors.RedactLogEnable} {
		t.Run(mode, func(t *testing.T) {
			perrors.RedactLogEnabled.Store(mode)
			req := httptest.NewRequest(http.MethodGet, "/config", nil)
			resp := httptest.NewRecorder()
			router.ServeHTTP(resp, req)

			require.Equal(t, http.StatusOK, resp.Code)
			var payload map[string]any
			require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &payload))
			require.Equal(t, testConfig.DataDir, payload["data-dir"])
			for _, secret := range []string{
				"aws-access-secret", "aws-private-secret", "aws-session-secret",
				"aws-endpoint-password", "aws-endpoint-token", "aws-authorization-secret",
				"gcp-private-secret", "gcp-endpoint-password", "gcp-endpoint-token",
			} {
				require.NotContains(t, resp.Body.String(), secret)
			}
			kms := payload["encryption"].(map[string]any)["kms"].(map[string]any)
			aws := kms["aws"].(map[string]any)
			require.Equal(t, "xxxxx", aws["secret_access_key"])
			require.Contains(t, aws["endpoint"], "kms.example.com/path?authorization=xxxxx")
			require.Contains(t, aws["endpoint"], "region=west")
			gcp := kms["gcp"].(map[string]any)
			require.Equal(t, "xxxxx", gcp["credentials_json"])
		})
	}
	require.Equal(t, "aws-private-secret", testConfig.Encryption.KMS.AWS.SecretAccessKey)
	require.Equal(t, `{"private_key":"gcp-private-secret"}`, testConfig.Encryption.KMS.GCP.CredentialsJSON)
}
