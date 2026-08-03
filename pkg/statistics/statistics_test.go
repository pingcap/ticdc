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

package statistics

import (
	"errors"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func newTestEvent(size int64) *commonEvent.DMLEvent {
	event := commonEvent.NewDMLEvent(common.NewDispatcherID(), 1, 1, 2, nil)
	event.ApproximateSize = size
	return event
}

func TestRecordDMLResult(t *testing.T) {
	stat := New(common.NewChangefeedID4Test("test-keyspace", "record-dml-result"), 123)
	defer stat.Close()

	// failed attempts increment the DML error counter and do not observe rows.
	stat.RecordDMLResult(10, errors.New("boom"))
	require.Equal(t, float64(1), testutil.ToFloat64(stat.metricExecErrCntForDML))

	// successful attempts observe the row count into the batch histogram.
	stat.RecordDMLResult(2, nil)
	var m dto.Metric
	require.NoError(t, stat.metricExecBatchHis.(prometheus.Metric).Write(&m))
	require.Equal(t, uint64(1), m.Histogram.GetSampleCount())
	require.Equal(t, float64(2), m.Histogram.GetSampleSum())
}

func TestTrackDMLEventCountsBytesOnPostFlush(t *testing.T) {
	stat := New(common.NewChangefeedID4Test("test-keyspace", "track-dml-event"), 123)
	defer stat.Close()

	event := newTestEvent(1024)
	stat.TrackDMLEvent(event)

	// Bytes are only counted after the event is flushed.
	require.Zero(t, testutil.ToFloat64(stat.metricTotalWriteBytesCnt))
	event.PostFlush()
	require.Equal(t, float64(1024), testutil.ToFloat64(stat.metricTotalWriteBytesCnt))
}

func TestCloseDeletesMetricSeries(t *testing.T) {
	stat := New(common.NewChangefeedID4Test("test-keyspace", "close-deletes"), 123)

	stat.RecordDMLResult(1, nil)
	stat.RecordDMLResult(1, errors.New("boom"))
	event := newTestEvent(100)
	stat.TrackDMLEvent(event)
	event.PostFlush()

	require.Equal(t, 1, testutil.CollectAndCount(execBatchHistogram))
	// New() eagerly creates the ddl series, RecordDMLResult adds the dml one.
	require.Equal(t, 2, testutil.CollectAndCount(executionErrorCounter))
	require.Equal(t, 1, testutil.CollectAndCount(totalWriteBytesCounter))

	stat.Close()
	require.Equal(t, 0, testutil.CollectAndCount(execBatchHistogram))
	require.Equal(t, 0, testutil.CollectAndCount(executionErrorCounter))
	require.Equal(t, 0, testutil.CollectAndCount(totalWriteBytesCounter))
}
