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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v2

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/logservice/logpuller"
	logpullermock "github.com/pingcap/ticdc/logservice/logpuller/mock"
	"github.com/stretchr/testify/require"
)

func TestPullerDebugAPI(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	provider := logpullermock.NewMockDebugInfoProvider(ctrl)
	router := newPullerDebugTestRouter(OpenAPIV2{pullerDebugProvider: provider})

	options := logpuller.PullerDebugOptions{SubscriptionLimit: 2, RegionLimit: 3}
	provider.EXPECT().GetPullerDebugInfo(options).Return(logpuller.PullerDebugInfo{
		SlowSubscriptions: []logpuller.PullerSubscriptionDebugInfo{{
			SubscriptionID: 1,
			SlowRegions: []logpuller.PullerRegionDebugInfo{{
				RegionID: 11,
			}},
		}},
	})
	response := performPullerDebugRequest(
		router, "/debug/puller?subscription_limit=2&region_limit=3")
	require.Equal(t, http.StatusOK, response.Code)
	var info logpuller.PullerDebugInfo
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &info))
	require.Equal(t, logpuller.SubscriptionID(1),
		info.SlowSubscriptions[0].SubscriptionID)
	require.Equal(t, uint64(11), info.SlowSubscriptions[0].SlowRegions[0].RegionID)

	provider.EXPECT().GetPullerDebugRegion(
		logpuller.SubscriptionID(1), uint64(11)).
		Return(logpuller.PullerRegionDebugDetail{
			SubscriptionID: 1,
			Region:         logpuller.PullerRegionDebugInfo{RegionID: 11},
		}, true)
	response = performPullerDebugRequest(
		router, "/debug/puller/subscriptions/1/regions/11")
	require.Equal(t, http.StatusOK, response.Code)
	var detail logpuller.PullerRegionDebugDetail
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &detail))
	require.Equal(t, uint64(11), detail.Region.RegionID)
}

func TestPullerDebugAPIValidationAndNotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	provider := logpullermock.NewMockDebugInfoProvider(ctrl)
	router := newPullerDebugTestRouter(OpenAPIV2{pullerDebugProvider: provider})

	response := performPullerDebugRequest(
		router, "/debug/puller?subscription_limit=21")
	require.Equal(t, http.StatusBadRequest, response.Code)

	response = performPullerDebugRequest(
		router, "/debug/puller/subscriptions/not-a-number/regions/11")
	require.Equal(t, http.StatusBadRequest, response.Code)

	provider.EXPECT().GetPullerDebugRegion(
		logpuller.SubscriptionID(999), uint64(11)).
		Return(logpuller.PullerRegionDebugDetail{}, false)
	response = performPullerDebugRequest(
		router, "/debug/puller/subscriptions/999/regions/11")
	require.Equal(t, http.StatusNotFound, response.Code)
}

func newPullerDebugTestRouter(api OpenAPIV2) *gin.Engine {
	router := gin.New()
	router.GET("/debug/puller", api.GetPullerDebugInfo)
	router.GET(
		"/debug/puller/subscriptions/:subscription_id/regions/:region_id",
		api.GetPullerDebugRegion,
	)
	return router
}

func performPullerDebugRequest(
	router *gin.Engine,
	path string,
) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, path, nil)
	router.ServeHTTP(recorder, request)
	return recorder
}
