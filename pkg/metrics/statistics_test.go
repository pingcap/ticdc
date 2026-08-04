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

package metrics

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestExecBatchHistogramKeyspaceIDLabel(t *testing.T) {
	ExecBatchHistogram.Reset()
	t.Cleanup(ExecBatchHistogram.Reset)

	statistics := NewStatistics(
		common.NewChangefeedID4Test("test-keyspace", "batch-row-count-keyspace-id"),
		123,
		"sink",
	)
	require.NoError(t, statistics.RecordBatchExecution(func() (int, int64, error) {
		return 2, 10, nil
	}))

	require.Equal(t, 1, testutil.CollectAndCount(ExecBatchHistogram))
	requireMetricHasLabel(t, ExecBatchHistogram, "keyspace_id", "123")

	statistics.Close()
	require.Equal(t, 0, testutil.CollectAndCount(ExecBatchHistogram))
}
