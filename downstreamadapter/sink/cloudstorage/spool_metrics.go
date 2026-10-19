// Copyright 2026 PingCAP, Inc.
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
	sinkmetrics "github.com/pingcap/ticdc/downstreamadapter/sink/metrics"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/sink/spool"
)

func newSpoolMetrics(changefeedID common.ChangeFeedID) *spool.Metrics {
	keyspace := changefeedID.Keyspace()
	changefeed := changefeedID.Name()
	return &spool.Metrics{
		MemoryBytes:        sinkmetrics.CloudStorageSpoolMemoryBytesGauge.WithLabelValues(keyspace, changefeed),
		DiskBytes:          sinkmetrics.CloudStorageSpoolDiskBytesGauge.WithLabelValues(keyspace, changefeed),
		PendingPostEnqueue: sinkmetrics.CloudStoragePendingPostEnqueueGauge.WithLabelValues(keyspace, changefeed),
		DiskQuotaWaiters:   sinkmetrics.CloudStorageSpoolDiskQuotaWaitersGauge.WithLabelValues(keyspace, changefeed),
		DiskQuotaWait:      sinkmetrics.CloudStorageSpoolDiskQuotaWaitDurationHistogram.WithLabelValues(keyspace, changefeed),
		LoadedBytes:        sinkmetrics.CloudStorageLoadBytesHistogram.WithLabelValues(keyspace, changefeed),
		RotatedCount:       sinkmetrics.CloudStorageRotateCountCounter.WithLabelValues(keyspace, changefeed),
		SegmentCount:       sinkmetrics.CloudStorageSpoolSegmentCountGauge.WithLabelValues(keyspace, changefeed),
		Close: func() {
			sinkmetrics.CloudStorageSpoolMemoryBytesGauge.DeleteLabelValues(keyspace, changefeed)
			sinkmetrics.CloudStorageSpoolDiskBytesGauge.DeleteLabelValues(keyspace, changefeed)
			sinkmetrics.CloudStoragePendingPostEnqueueGauge.DeleteLabelValues(keyspace, changefeed)
			sinkmetrics.CloudStorageSpoolDiskQuotaWaitersGauge.DeleteLabelValues(keyspace, changefeed)
			sinkmetrics.CloudStorageSpoolDiskQuotaWaitDurationHistogram.DeleteLabelValues(keyspace, changefeed)
			sinkmetrics.CloudStorageLoadBytesHistogram.DeleteLabelValues(keyspace, changefeed)
			sinkmetrics.CloudStorageRotateCountCounter.DeleteLabelValues(keyspace, changefeed)
			sinkmetrics.CloudStorageSpoolSegmentCountGauge.DeleteLabelValues(keyspace, changefeed)
		},
	}
}
