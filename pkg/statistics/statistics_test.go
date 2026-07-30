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
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestTrackDMLEventRecordsRawBytesOnPostFlush(t *testing.T) {
	changefeedID := common.NewChangefeedID4Test("test", t.Name())
	statistics := New(changefeedID, common.DefaultKeyspaceID)
	t.Cleanup(statistics.Close)

	event := commonEvent.NewDMLEvent(common.NewDispatcherID(), 1, 1, 2, nil)
	event.ApproximateSize = 123
	statistics.TrackDMLEvent(event)
	require.NoError(t, statistics.RecordBatchExecution(func() (int, error) {
		return int(event.Len()), nil
	}))

	require.Zero(t, testutil.ToFloat64(statistics.metricTotalWriteBytesCnt))

	// The tracked size is a snapshot, so the callback does not retain the event.
	event.ApproximateSize = 456
	event.PostFlush()
	require.Equal(t, float64(123), testutil.ToFloat64(statistics.metricTotalWriteBytesCnt))
}
