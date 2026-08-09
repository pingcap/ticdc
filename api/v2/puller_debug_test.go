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
	api := OpenAPIV2{pullerDebugProvider: provider}
	router := newPullerDebugTestRouter(api)

	provider.EXPECT().GetPullerDebugInfo().Return(logpuller.PullerDebugInfo{State: "running"})
	response := performPullerDebugRequest(t, router, "/debug/puller")
	require.Equal(t, http.StatusOK, response.Code)
	var overview logpuller.PullerDebugInfo
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &overview))
	require.Equal(t, "running", overview.State)

	provider.EXPECT().GetPullerDebugSubscriptions().Return([]logpuller.PullerSubscriptionDebugInfo{
		{SubscriptionID: 1, TableID: 10, Initialized: true},
		{SubscriptionID: 2, TableID: 20, Stopped: true},
		{SubscriptionID: 3, TableID: 30},
	})
	response = performPullerDebugRequest(t, router, "/debug/puller/subscriptions?limit=2")
	require.Equal(t, http.StatusOK, response.Code)
	var subscriptions pullerSubscriptionListResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &subscriptions))
	require.Len(t, subscriptions.Items, 2)
	require.Equal(t, logpuller.SubscriptionID(2), subscriptions.NextAfterID)

	detailOptions := logpuller.PullerSubscriptionDebugOptions{
		RegionMode:  "slow",
		RegionLimit: 2,
		IncludeKeys: true,
	}
	provider.EXPECT().GetPullerDebugSubscription(logpuller.SubscriptionID(3), detailOptions).
		Return(logpuller.PullerSubscriptionDetail{
			Subscription: logpuller.PullerSubscriptionDebugInfo{SubscriptionID: 3},
		}, true)
	response = performPullerDebugRequest(t, router,
		"/debug/puller/subscriptions/3?regions=slow&region_limit=2&include_keys=true")
	require.Equal(t, http.StatusOK, response.Code)
	var detail logpuller.PullerSubscriptionDetail
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &detail))
	require.Equal(t, logpuller.SubscriptionID(3), detail.Subscription.SubscriptionID)

	provider.EXPECT().GetPullerDebugStores().Return([]logpuller.PullerStoreDebugInfo{{
		Address: "tikv-1:20160",
	}})
	response = performPullerDebugRequest(t, router, "/debug/puller/stores")
	require.Equal(t, http.StatusOK, response.Code)
	var stores pullerStoreListResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &stores))
	require.Equal(t, "tikv-1:20160", stores.Items[0].Address)

	provider.EXPECT().GetPullerDebugStore("tikv-1:20160").Return(logpuller.PullerStoreDebugInfo{
		Address: "tikv-1:20160",
	}, true)
	response = performPullerDebugRequest(t, router, "/debug/puller/stores/tikv-1:20160")
	require.Equal(t, http.StatusOK, response.Code)
}

func TestPullerDebugAPIValidationAndNotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	provider := logpullermock.NewMockDebugInfoProvider(ctrl)
	api := OpenAPIV2{pullerDebugProvider: provider}
	router := newPullerDebugTestRouter(api)

	response := performPullerDebugRequest(t, router, "/debug/puller/subscriptions?limit=0")
	require.Equal(t, http.StatusBadRequest, response.Code)

	response = performPullerDebugRequest(t, router, "/debug/puller/subscriptions/not-a-number")
	require.Equal(t, http.StatusBadRequest, response.Code)

	provider.EXPECT().GetPullerDebugSubscription(
		logpuller.SubscriptionID(999),
		logpuller.PullerSubscriptionDebugOptions{
			RegionMode:  "none",
			RegionLimit: defaultPullerDebugListLimit,
		},
	).Return(logpuller.PullerSubscriptionDetail{}, false)
	response = performPullerDebugRequest(t, router, "/debug/puller/subscriptions/999")
	require.Equal(t, http.StatusNotFound, response.Code)

	provider.EXPECT().GetPullerDebugStore("missing:20160").Return(logpuller.PullerStoreDebugInfo{}, false)
	response = performPullerDebugRequest(t, router, "/debug/puller/stores/missing:20160")
	require.Equal(t, http.StatusNotFound, response.Code)
}

func newPullerDebugTestRouter(api OpenAPIV2) *gin.Engine {
	router := gin.New()
	router.GET("/debug/puller", api.GetPullerDebugInfo)
	router.GET("/debug/puller/subscriptions", api.ListPullerDebugSubscriptions)
	router.GET("/debug/puller/subscriptions/:subscription_id", api.GetPullerDebugSubscription)
	router.GET("/debug/puller/stores", api.ListPullerDebugStores)
	router.GET("/debug/puller/stores/:store_address", api.GetPullerDebugStore)
	return router
}

func performPullerDebugRequest(
	t *testing.T,
	router *gin.Engine,
	path string,
) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, path, nil)
	router.ServeHTTP(recorder, request)
	return recorder
}
