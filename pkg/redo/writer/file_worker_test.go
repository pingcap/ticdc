// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package writer

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/redo/testutil"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestFileWorkerBusyRatioRecordsActualWork(t *testing.T) {
	changefeedID := common.NewChangeFeedIDWithName(t.Name(), common.DefaultKeyspaceName)
	consistentCfg := testutil.NewConsistentConfig("nfs:///tmp/redo")
	flushInterval := int64(time.Hour / time.Millisecond)
	consistentCfg.FlushIntervalInMs = util.AddressOf(flushInterval)
	cfg, err := NewConfig(changefeedID, consistentCfg)
	require.NoError(t, err)

	inputCh := make(chan *polymorphicRedoEvent, 1)
	worker := newFileWorkerGroup(cfg, inputCh, nil)
	busyTime := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_redo_worker_busy_seconds"})
	worker.metricBusyRatio = busyTime
	defer worker.close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- worker.bgWriteLogs(ctx, inputCh)
	}()

	inputCh <- &polymorphicRedoEvent{commitTs: 1, data: []byte("redo")}
	require.Eventually(t, func() bool {
		return promtestutil.ToFloat64(busyTime) > 0
	}, time.Second, time.Millisecond)

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}
