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

package mysql

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestDMLEventRowsAffectedMetrics(t *testing.T) {
	execDMLEventRowsAffectedCounter.Reset()
	t.Cleanup(execDMLEventRowsAffectedCounter.Reset)

	changefeedID := common.NewChangefeedID4Test("test-keyspace", "rows-affected")
	writer := &Writer{ChangefeedID: changefeedID}
	writer.recordRowsAffected(2, common.RowTypeInsert)
	writer.recordTotalRowsAffected(3, 2)

	keyspace := changefeedID.Keyspace()
	changefeed := changefeedID.Name()
	require.Equal(t, float64(2), testutil.ToFloat64(
		execDMLEventRowsAffectedCounter.WithLabelValues(keyspace, changefeed, "actual", "insert")))
	require.Equal(t, float64(1), testutil.ToFloat64(
		execDMLEventRowsAffectedCounter.WithLabelValues(keyspace, changefeed, "expected", "insert")))
	require.Equal(t, float64(5), testutil.ToFloat64(
		execDMLEventRowsAffectedCounter.WithLabelValues(keyspace, changefeed, "actual", "total")))
	require.Equal(t, float64(3), testutil.ToFloat64(
		execDMLEventRowsAffectedCounter.WithLabelValues(keyspace, changefeed, "expected", "total")))

	otherChangefeedID := common.NewChangefeedID4Test("test-keyspace", "other-changefeed")
	otherWriter := &Writer{ChangefeedID: otherChangefeedID}
	otherWriter.recordRowsAffected(4, common.RowTypeInsert)

	DeleteDMLEventRowsAffectedMetrics(changefeedID)
	require.Equal(t, 4, testutil.CollectAndCount(execDMLEventRowsAffectedCounter))
	require.Equal(t, float64(4), testutil.ToFloat64(execDMLEventRowsAffectedCounter.WithLabelValues(
		otherChangefeedID.Keyspace(), otherChangefeedID.Name(), "actual", "insert")))
}
