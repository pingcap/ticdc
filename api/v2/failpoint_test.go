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

package v2

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func newFailpointContext(method, path string, body string) (*gin.Context, *httptest.ResponseRecorder) {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	ctx.Request = req
	return ctx, recorder
}

func TestFailpointAPIEnableDisable(t *testing.T) {
	gin.SetMode(gin.TestMode)

	api := OpenAPIV2{}
	failpointName := "github.com/pingcap/ticdc/utils/dynstream/PauseArea"

	ctx, recorder := newFailpointContext(http.MethodPost, "/debug/failpoints",
		`{"name":"`+failpointName+`","expr":"return(true)"}`)
	api.EnableFailpoint(ctx)
	require.Equal(t, http.StatusOK, recorder.Code)

	ctx, recorder = newFailpointContext(http.MethodGet, "/debug/failpoints", "")
	api.ListFailpoints(ctx)
	require.Equal(t, http.StatusOK, recorder.Code)

	var list []FailpointInfo
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &list))
	require.NotEmpty(t, list)
	found := false
	for _, item := range list {
		if item.Name == failpointName {
			found = true
			break
		}
	}
	require.True(t, found, "failpoint should be listed")

	ctx, recorder = newFailpointContext(http.MethodDelete,
		"/debug/failpoints?name="+failpointName, "")
	api.DisableFailpoint(ctx)
	require.Equal(t, http.StatusOK, recorder.Code)

	ctx, recorder = newFailpointContext(http.MethodGet, "/debug/failpoints", "")
	api.ListFailpoints(ctx)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.NotContains(t, recorder.Body.String(), failpointName)
}
