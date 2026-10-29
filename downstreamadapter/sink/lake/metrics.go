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

package lake

import (
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/sink/spool"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	lakeBufferBytes    = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "ticdc", Subsystem: "lake", Name: "buffer_bytes", Help: "Local Lake memory and physical spool bytes."}, []string{"namespace", "changefeed", "kind"})
	lakeBufferEntries  = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "ticdc", Subsystem: "lake", Name: "buffer_entries", Help: "Local Lake pending enqueue, segment and quota waiter counts."}, []string{"namespace", "changefeed", "kind"})
	lakeSpoolDuration  = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "ticdc", Subsystem: "lake", Name: "spool_wait_seconds", Help: "Lake spool quota wait time.", Buckets: prometheus.ExponentialBuckets(0.001, 2, 20)}, []string{"namespace", "changefeed"})
	lakeSpoolLoad      = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "ticdc", Subsystem: "lake", Name: "spool_load_bytes", Help: "Lake encoded bytes loaded from local spool.", Buckets: prometheus.ExponentialBuckets(1024, 4, 9)}, []string{"namespace", "changefeed"})
	lakeSpoolRotations = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "ticdc", Subsystem: "lake", Name: "spool_rotations_total", Help: "Lake local spool segment rotations."}, []string{"namespace", "changefeed"})
)

// InitMetrics registers the metrics owned by the Lake sink.
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(lakeBufferBytes, lakeBufferEntries, lakeSpoolDuration, lakeSpoolLoad, lakeSpoolRotations)
}

func newSpoolMetrics(id common.ChangeFeedID) *spool.Metrics {
	k, f := id.Keyspace(), id.Name()
	return &spool.Metrics{
		MemoryBytes:        lakeBufferBytes.WithLabelValues(k, f, "memory"),
		DiskBytes:          lakeBufferBytes.WithLabelValues(k, f, "disk"),
		PendingPostEnqueue: lakeBufferEntries.WithLabelValues(k, f, "enqueue"),
		DiskQuotaWaiters:   lakeBufferEntries.WithLabelValues(k, f, "waiters"),
		SegmentCount:       lakeBufferEntries.WithLabelValues(k, f, "segments"),
		DiskQuotaWait:      lakeSpoolDuration.WithLabelValues(k, f),
		LoadedBytes:        lakeSpoolLoad.WithLabelValues(k, f),
		RotatedCount:       lakeSpoolRotations.WithLabelValues(k, f),
		Close: func() {
			for _, kind := range []string{"memory", "disk"} {
				lakeBufferBytes.DeleteLabelValues(k, f, kind)
			}
			for _, kind := range []string{"enqueue", "waiters", "segments"} {
				lakeBufferEntries.DeleteLabelValues(k, f, kind)
			}
			lakeSpoolDuration.DeleteLabelValues(k, f)
			lakeSpoolLoad.DeleteLabelValues(k, f)
			lakeSpoolRotations.DeleteLabelValues(k, f)
		},
	}
}
