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

package server

import (
	"testing"

	"github.com/pingcap/ticdc/downstreamadapter/sink/statistics"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestRegisterMetrics(t *testing.T) {
	originalRegistry := registry
	registry = prometheus.NewRegistry()
	t.Cleanup(func() { registry = originalRegistry })

	require.NotPanics(t, registerMetrics)

	changefeedID := common.NewChangeFeedIDWithName("test-register-metrics", common.DefaultKeyspaceName)
	stats := statistics.New(changefeedID, common.DefaultKeyspaceID)
	t.Cleanup(stats.Close)
	stats.RecordDMLBatch(2, 128)
	require.NoError(t, stats.RecordDDLExecution("CreateTable", func() error { return nil }))

	count, err := testutil.GatherAndCount(registry,
		"ticdc_sink_batch_row_count",
		"ticdc_sink_batch_write_bytes",
		"ticdc_sink_execution_error",
		"ticdc_ddl_exec_duration",
		"ticdc_ddl_exec_running",
		"ticdc_ddl_execution",
	)
	require.NoError(t, err)
	require.Equal(t, 7, count)
}
