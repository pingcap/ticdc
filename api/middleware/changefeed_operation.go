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
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/api"
	"github.com/pingcap/ticdc/pkg/metrics"
	"go.uber.org/zap"
)

const (
	changefeedOperationContextKey      = "changefeed-operation-info"
	changefeedOperationHistoryLimit    = 100
	changefeedOperationMetricTextLimit = 256
)

var (
	recentChangefeedOperations = newRecentChangefeedOperationStore(changefeedOperationHistoryLimit)
	changefeedOperationEventID uint64
)

type changefeedOperationInfo struct {
	keyspace   string
	changefeed string
	details    string
}

type changefeedOperationMetricLabels struct {
	keyspace   string
	changefeed string
	operation  string
	result     string
	username   string
	details    string
	err        string
	eventID    string
}

func (l changefeedOperationMetricLabels) labelValues() []string {
	return []string{
		l.keyspace,
		l.changefeed,
		l.operation,
		l.result,
		l.username,
		l.details,
		l.err,
		l.eventID,
	}
}

type recentChangefeedOperationStore struct {
	mu     sync.Mutex
	limit  int
	events []changefeedOperationMetricLabels
}

func newRecentChangefeedOperationStore(limit int) *recentChangefeedOperationStore {
	return &recentChangefeedOperationStore{limit: limit}
}

func (s *recentChangefeedOperationStore) record(
	labels changefeedOperationMetricLabels,
	operationTime time.Time,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// The dashboard only needs a recent investigation window. Keep this cache
	// bounded so user names and detail strings do not become unbounded metric
	// cardinality over long-running clusters.
	metrics.ChangefeedOperationTimeGauge.WithLabelValues(labels.labelValues()...).Set(float64(operationTime.UnixMilli()))
	s.events = append(s.events, labels)
	if len(s.events) <= s.limit {
		return
	}

	oldest := s.events[0]
	s.events = s.events[1:]
	metrics.ChangefeedOperationTimeGauge.DeleteLabelValues(oldest.labelValues()...)
}

// ChangefeedOperationMiddleware records user initiated changefeed mutations for
// logs and the bounded recent-operation Grafana panel.
func ChangefeedOperationMiddleware(operation string) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		statusCode := c.Writer.Status()
		lastError := c.Errors.Last()
		result := "success"
		if statusCode >= http.StatusBadRequest || lastError != nil {
			result = "failed"
		}

		info := getChangefeedOperationInfo(c)
		if info.keyspace == "" {
			info.keyspace = c.Query(api.APIOpVarKeyspace)
		}
		if info.changefeed == "" {
			info.changefeed = c.Param(api.APIOpVarChangefeedID)
		}

		username, _, _ := c.Request.BasicAuth()
		if username == "" {
			username = "anonymous"
		}

		var operationErr error
		if lastError != nil {
			operationErr = lastError.Err
		}

		operationTime := time.Now()
		eventID := atomic.AddUint64(&changefeedOperationEventID, 1)
		labels := changefeedOperationMetricLabels{
			keyspace:   normalizeChangefeedOperationMetricText(info.keyspace),
			changefeed: normalizeChangefeedOperationMetricText(info.changefeed),
			operation:  operation,
			result:     result,
			username:   normalizeChangefeedOperationMetricText(username),
			details:    normalizeChangefeedOperationMetricText(info.details),
			err:        normalizeChangefeedOperationMetricError(operationErr),
			eventID:    fmt.Sprintf("%d", eventID),
		}
		recentChangefeedOperations.record(labels, operationTime)

		log.Info("changefeed operation",
			zap.String("operation", operation),
			zap.String("result", result),
			zap.String("keyspace", info.keyspace),
			zap.String("changefeedID", info.changefeed),
			zap.String("details", info.details),
			zap.Int("status", statusCode),
			zap.String("username", username),
			zap.String("ip", c.ClientIP()),
			zap.String("userAgent", c.Request.UserAgent()),
			zap.String("clientVersion", c.Request.Header.Get(ClientVersionHeader)),
			zap.Duration("duration", time.Since(start)),
			zap.Error(operationErr),
		)
	}
}

// SetChangefeedOperationTarget attaches the logical changefeed identity that the
// current mutation request targets.
func SetChangefeedOperationTarget(c *gin.Context, keyspace, changefeed string) {
	info := getChangefeedOperationInfo(c)
	info.keyspace = keyspace
	info.changefeed = changefeed
	c.Set(changefeedOperationContextKey, info)
}

// SetChangefeedOperationDetails attaches a concise, non-sensitive summary for
// the current mutation request.
func SetChangefeedOperationDetails(c *gin.Context, details string) {
	info := getChangefeedOperationInfo(c)
	info.details = details
	c.Set(changefeedOperationContextKey, info)
}

func getChangefeedOperationInfo(c *gin.Context) changefeedOperationInfo {
	raw, ok := c.Get(changefeedOperationContextKey)
	if !ok {
		return changefeedOperationInfo{}
	}
	info, ok := raw.(changefeedOperationInfo)
	if !ok {
		return changefeedOperationInfo{}
	}
	return info
}

func normalizeChangefeedOperationMetricError(err error) string {
	if err == nil {
		return ""
	}
	return normalizeChangefeedOperationMetricText(err.Error())
}

func normalizeChangefeedOperationMetricText(value string) string {
	value = strings.Join(strings.Fields(value), " ")
	if utf8.RuneCountInString(value) <= changefeedOperationMetricTextLimit {
		return value
	}

	runes := []rune(value)
	return string(runes[:changefeedOperationMetricTextLimit-3]) + "..."
}
