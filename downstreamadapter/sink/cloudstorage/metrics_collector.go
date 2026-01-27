// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cloudstorage

import (
	"context"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/metrics"
	"go.uber.org/atomic"
)

type metricsCollector struct {
	changefeedID common.ChangeFeedID

	fileCount atomic.Uint64

	unflushedRawBytes atomic.Int64
	totalRawBytes     atomic.Int64

	unflushedEncodedBytes atomic.Int64
	totalEncodedBytes     atomic.Int64
}

func newMetricsCollector(changefeedID common.ChangeFeedID) *metricsCollector {
	return &metricsCollector{
		changefeedID: changefeedID,
	}
}

func (m *metricsCollector) run(ctx context.Context) {
	namespace := m.changefeedID.Keyspace()
	changefeed := m.changefeedID.Name()

	fileCount := metrics.CloudStorageFileCountCounter.WithLabelValues(namespace, changefeed)
	unflushedRawBytes := metrics.CloudStorageRawBytesGauge.WithLabelValues(namespace, changefeed, "unflushed")
	totalRawBytes := metrics.CloudStorageRawBytesGauge.WithLabelValues(namespace, changefeed, "total")

	unflushedEncodedBytes := metrics.CloudStorageEncodedBytesGauge.WithLabelValues(namespace, changefeed, "unflushed")
	totalEncodedBytes := metrics.CloudStorageEncodedBytesGauge.WithLabelValues(namespace, changefeed, "total")

	ticker := time.NewTicker(5 * time.Second)

	defer func() {
		ticker.Stop()
		metrics.CloudStorageFileCountCounter.DeleteLabelValues(namespace, changefeed)
		metrics.CloudStorageFlushDurationHistogram.DeleteLabelValues(namespace, changefeed)
		metrics.CloudStorageRawBytesGauge.DeleteLabelValues(namespace, changefeed, "unflushed")
		metrics.CloudStorageEncodedBytesGauge.DeleteLabelValues(namespace, changefeed, "total")
		metrics.CloudStorageRawBytesGauge.DeleteLabelValues(namespace, changefeed, "unflushed")
		metrics.CloudStorageEncodedBytesGauge.DeleteLabelValues(namespace, changefeed, "total")
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fileCount.Add(float64(m.fileCount.Load()))
			unflushedRawBytes.Set(float64(m.unflushedRawBytes.Load()))
			totalRawBytes.Set(float64(m.totalRawBytes.Load()))
			unflushedEncodedBytes.Set(float64(m.unflushedEncodedBytes.Load()))
			totalEncodedBytes.Set(float64(m.totalEncodedBytes.Load()))
		}
	}
}
