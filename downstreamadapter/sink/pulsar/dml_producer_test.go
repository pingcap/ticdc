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

package pulsar

import (
	"errors"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/statistics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

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

func newTestMessage(rows int, callback func()) *codecCommon.Message {
	message := &codecCommon.Message{Key: []byte("k"), Value: []byte("v")}
	message.SetRowsCount(rows)
	message.Callback = callback
	return message
}

func newTestDMLProducers(t *testing.T, changefeed string) (*dmlProducers, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	statistics.InitMetrics(reg)
	stat := statistics.New(common.NewChangefeedID4Test("test-keyspace", changefeed), 123)
	t.Cleanup(stat.Close)
	return &dmlProducers{statistics: stat}, reg
}

func TestHandleAsyncSendResultSuccess(t *testing.T) {
	p, reg := newTestDMLProducers(t, "async-send-success")

	callbackCalled := make(chan struct{})
	message := newTestMessage(2, func() { close(callbackCalled) })
	p.handleAsyncSendResult(message, nil)

	<-callbackCalled
	// The row count is observed into the batch histogram on success.
	hist := gatherMetric(t, reg, "ticdc_sink_batch_row_count",
		"namespace", "test-keyspace", "changefeed", "async-send-success")
	require.NotNil(t, hist)
	require.Equal(t, uint64(1), hist.GetHistogram().GetSampleCount())
	require.Equal(t, float64(2), hist.GetHistogram().GetSampleSum())
}

func TestHandleAsyncSendResultErrorSkipsCallback(t *testing.T) {
	p, reg := newTestDMLProducers(t, "async-send-error")

	message := newTestMessage(4, func() { t.Fatal("callback must not run on failure") })
	p.handleAsyncSendResult(message, errors.New("broker boom"))

	errMetric := gatherMetric(t, reg, "ticdc_sink_execution_error",
		"namespace", "test-keyspace", "changefeed", "async-send-error", "event_type", "dml")
	require.NotNil(t, errMetric)
	require.Equal(t, float64(1), errMetric.GetCounter().GetValue())
	// No rows are observed for failed attempts; the histogram series exists
	// (created eagerly by New) but has no samples.
	hist := gatherMetric(t, reg, "ticdc_sink_batch_row_count",
		"namespace", "test-keyspace", "changefeed", "async-send-error")
	require.NotNil(t, hist)
	require.Equal(t, uint64(0), hist.GetHistogram().GetSampleCount())
}

func TestHandleSendFailureRecordsErrorAndReturnsIt(t *testing.T) {
	p, reg := newTestDMLProducers(t, "send-failure")

	sentinel := errors.New("producer closed")
	message := newTestMessage(4, nil)
	require.ErrorIs(t, p.handleSendFailure(message, sentinel), sentinel)

	errMetric := gatherMetric(t, reg, "ticdc_sink_execution_error",
		"namespace", "test-keyspace", "changefeed", "send-failure", "event_type", "dml")
	require.NotNil(t, errMetric)
	require.Equal(t, float64(1), errMetric.GetCounter().GetValue())
}
