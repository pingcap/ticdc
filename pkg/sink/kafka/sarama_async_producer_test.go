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

package kafka

import (
	"errors"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/statistics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// gatherMetric returns the metric of the given name whose labels match
// labelValues (alternating key/value pairs), or nil if it does not exist.
func gatherMetric(
	t *testing.T, reg *prometheus.Registry, name string, labelValues ...string,
) *dto.Metric {
	t.Helper()
	require.Lenf(t, labelValues, len(labelValues)&^1, "labelValues must be key/value pairs")
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			matched := true
			for i := 0; i < len(labelValues); i += 2 {
				found := false
				for _, lp := range m.GetLabel() {
					if lp.GetName() == labelValues[i] && lp.GetValue() == labelValues[i+1] {
						found = true
						break
					}
				}
				if !found {
					matched = false
					break
				}
			}
			if matched {
				return m
			}
		}
	}
	return nil
}

func newTestStatistics(t *testing.T, changefeed string) (*saramaAsyncProducer, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	statistics.InitMetrics(reg)
	stat := statistics.New(common.NewChangefeedID4Test("test-keyspace", changefeed), 123)
	t.Cleanup(stat.Close)
	return &saramaAsyncProducer{statistics: stat}, reg
}

func TestHandleSuccessRecordsRowsAndRunsCallback(t *testing.T) {
	p, reg := newTestStatistics(t, "handle-success")

	callbackCalled := make(chan struct{})
	p.handleSuccess(&messageMetadata{rowCount: 3, callback: func() { close(callbackCalled) }})

	<-callbackCalled
	// The row count is observed into the batch histogram on success.
	hist := gatherMetric(t, reg, "ticdc_sink_batch_row_count",
		"namespace", "test-keyspace", "changefeed", "handle-success")
	require.NotNil(t, hist)
	require.Equal(t, uint64(1), hist.GetHistogram().GetSampleCount())
	require.Equal(t, float64(3), hist.GetHistogram().GetSampleSum())
}

func TestHandleSuccessNilMetaIsNoop(t *testing.T) {
	p, _ := newTestStatistics(t, "handle-success-nil")
	require.NotPanics(t, func() { p.handleSuccess(nil) })
}

func TestHandleFailureRecordsErrorAndReturnsIt(t *testing.T) {
	p, reg := newTestStatistics(t, "handle-failure")

	sentinel := errors.New("broker boom")
	require.ErrorIs(t, p.handleFailure(5, sentinel), sentinel)

	// The failed message increments the DML error counter and observes no rows.
	errMetric := gatherMetric(t, reg, "ticdc_sink_execution_error",
		"namespace", "test-keyspace", "changefeed", "handle-failure", "event_type", "dml")
	require.NotNil(t, errMetric)
	require.Equal(t, float64(1), errMetric.GetCounter().GetValue())
	// No rows are observed for failed attempts; the histogram series exists
	// (created eagerly by New) but has no samples.
	hist := gatherMetric(t, reg, "ticdc_sink_batch_row_count",
		"namespace", "test-keyspace", "changefeed", "handle-failure")
	require.NotNil(t, hist)
	require.Equal(t, uint64(0), hist.GetHistogram().GetSampleCount())
}
