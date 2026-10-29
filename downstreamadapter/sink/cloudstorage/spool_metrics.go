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
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/sink/spool"
)

func newSpoolMetrics(changefeedID common.ChangeFeedID) *spool.Metrics {
	keyspace := changefeedID.Keyspace()
	changefeed := changefeedID.Name()
	return &spool.Metrics{
		MemoryBytes:        cloudStorageSpoolMemoryBytesGauge.WithLabelValues(keyspace, changefeed),
		DiskBytes:          cloudStorageSpoolDiskBytesGauge.WithLabelValues(keyspace, changefeed),
		PendingPostEnqueue: cloudStoragePendingPostEnqueueGauge.WithLabelValues(keyspace, changefeed),
		DiskQuotaWaiters:   cloudStorageSpoolDiskQuotaWaitersGauge.WithLabelValues(keyspace, changefeed),
		DiskQuotaWait:      cloudStorageSpoolDiskQuotaWaitDurationHistogram.WithLabelValues(keyspace, changefeed),
		LoadedBytes:        cloudStorageLoadBytesHistogram.WithLabelValues(keyspace, changefeed),
		RotatedCount:       cloudStorageRotateCountCounter.WithLabelValues(keyspace, changefeed),
		SegmentCount:       cloudStorageSpoolSegmentCountGauge.WithLabelValues(keyspace, changefeed),
		Close: func() {
			cloudStorageSpoolMemoryBytesGauge.DeleteLabelValues(keyspace, changefeed)
			cloudStorageSpoolDiskBytesGauge.DeleteLabelValues(keyspace, changefeed)
			cloudStoragePendingPostEnqueueGauge.DeleteLabelValues(keyspace, changefeed)
			cloudStorageSpoolDiskQuotaWaitersGauge.DeleteLabelValues(keyspace, changefeed)
			cloudStorageSpoolDiskQuotaWaitDurationHistogram.DeleteLabelValues(keyspace, changefeed)
			cloudStorageLoadBytesHistogram.DeleteLabelValues(keyspace, changefeed)
			cloudStorageRotateCountCounter.DeleteLabelValues(keyspace, changefeed)
			cloudStorageSpoolSegmentCountGauge.DeleteLabelValues(keyspace, changefeed)
		},
	}
}
