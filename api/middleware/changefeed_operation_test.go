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

package middleware

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestChangefeedOperationMiddlewareRecordsSuccessfulMutation verifies that a
// successful request keeps the logical target, normalized detail summary, and
// authenticated user for the dashboard history row.
func TestChangefeedOperationMiddlewareRecordsSuccessfulMutation(t *testing.T) {
	resetRecentChangefeedOperationStateForTest(t)

	router := gin.New()
	router.POST("/api/v2/changefeeds/:changefeed_id/resume",
		ChangefeedOperationMiddleware("resume"),
		func(c *gin.Context) {
			SetChangefeedOperationTarget(c, "ks1", "test")
			SetChangefeedOperationDetails(c, "overwrite_checkpoint_ts=true\nnew_checkpoint_ts=123")
			c.Status(http.StatusOK)
		},
	)

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v2/changefeeds/test/resume?keyspace=ks1", nil)
	req.SetBasicAuth("alice", "secret")
	router.ServeHTTP(recorder, req)

	require.Len(t, recentChangefeedOperations.events, 1)
	require.Equal(t, changefeedOperationMetricLabels{
		keyspace:   "ks1",
		changefeed: "test",
		operation:  "resume",
		result:     "success",
		username:   "alice",
		details:    "overwrite_checkpoint_ts=true new_checkpoint_ts=123",
		err:        "",
		eventID:    "1",
	}, recentChangefeedOperations.events[0])
}

// TestChangefeedOperationMiddlewareRecordsFailedMutation verifies that a failed
// request is still retained with its normalized error summary so oncall can see
// rejected user attempts as well as successful changes.
func TestChangefeedOperationMiddlewareRecordsFailedMutation(t *testing.T) {
	resetRecentChangefeedOperationStateForTest(t)

	router := gin.New()
	router.PUT("/api/v2/changefeeds/:changefeed_id",
		ChangefeedOperationMiddleware("update"),
		func(c *gin.Context) {
			_ = c.Error(errors.New("update rejected\nbecause state is normal"))
			c.Status(http.StatusBadRequest)
		},
	)

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/api/v2/changefeeds/test?keyspace=ks1", nil)
	router.ServeHTTP(recorder, req)

	require.Len(t, recentChangefeedOperations.events, 1)
	require.Equal(t, "failed", recentChangefeedOperations.events[0].result)
	require.Equal(t, "anonymous", recentChangefeedOperations.events[0].username)
	require.Equal(t, "update rejected because state is normal", recentChangefeedOperations.events[0].err)
}

// TestRecentChangefeedOperationStoreEvictsOldestMetric verifies that the recent
// operation cache preserves only the newest bounded rows and removes the evicted
// metric series to avoid unbounded label cardinality.
func TestRecentChangefeedOperationStoreEvictsOldestMetric(t *testing.T) {
	resetRecentChangefeedOperationStateForTest(t)
	store := newRecentChangefeedOperationStore(2)
	now := time.UnixMilli(1_700_000_000_000)

	first := changefeedOperationMetricLabels{eventID: "1"}
	second := changefeedOperationMetricLabels{eventID: "2"}
	third := changefeedOperationMetricLabels{eventID: "3"}
	t.Cleanup(func() {
		for _, labels := range []changefeedOperationMetricLabels{first, second, third} {
			metrics.ChangefeedOperationTimeGauge.DeleteLabelValues(labels.labelValues()...)
		}
	})
	store.record(first, now)
	store.record(second, now.Add(time.Millisecond))
	store.record(third, now.Add(2*time.Millisecond))

	require.Equal(t, []changefeedOperationMetricLabels{second, third}, store.events)
	require.Equal(t, float64(0), testutil.ToFloat64(metricsGaugeForTest(first)))
	require.Equal(t, float64(now.Add(time.Millisecond).UnixMilli()), testutil.ToFloat64(metricsGaugeForTest(second)))
	require.Equal(t, float64(now.Add(2*time.Millisecond).UnixMilli()), testutil.ToFloat64(metricsGaugeForTest(third)))
}

// TestNormalizeChangefeedOperationMetricText verifies that dashboard labels are
// kept single-line and bounded so a malformed request cannot create oversized
// operation-history fields.
func TestNormalizeChangefeedOperationMetricText(t *testing.T) {
	longText := strings.Repeat("a", changefeedOperationMetricTextLimit+10)
	require.Equal(t, "a b", normalizeChangefeedOperationMetricText("a\n b"))
	require.Equal(t, changefeedOperationMetricTextLimit, len(normalizeChangefeedOperationMetricText(longText)))
	require.True(t, strings.HasSuffix(normalizeChangefeedOperationMetricText(longText), "..."))
}

func resetRecentChangefeedOperationStateForTest(t *testing.T) {
	t.Helper()
	for _, labels := range recentChangefeedOperations.events {
		metrics.ChangefeedOperationTimeGauge.DeleteLabelValues(labels.labelValues()...)
	}
	recentChangefeedOperations = newRecentChangefeedOperationStore(changefeedOperationHistoryLimit)
	atomic.StoreUint64(&changefeedOperationEventID, 0)
}

func metricsGaugeForTest(labels changefeedOperationMetricLabels) prometheus.Gauge {
	return metrics.ChangefeedOperationTimeGauge.WithLabelValues(labels.labelValues()...)
}
