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
	"github.com/pingcap/ticdc/downstreamadapter/sink/metrics"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/prometheus/client_golang/prometheus"
)

type metricsCollector struct {
	unflushedRawBytes     prometheus.Gauge
	unflushedEncodedBytes prometheus.Gauge
	rawBytesTotal         prometheus.Counter
	encodedBytesTotal     prometheus.Counter
}

func newMetricsCollector(changefeedID commonType.ChangeFeedID) *metricsCollector {
	namespace := changefeedID.Keyspace()
	changefeed := changefeedID.Name()
	return &metricsCollector{
		unflushedRawBytes: metrics.CloudStorageUnflushedRawBytesGauge.
			WithLabelValues(namespace, changefeed),
		unflushedEncodedBytes: metrics.CloudStorageUnflushedEncodedBytesGauge.
			WithLabelValues(namespace, changefeed),
		rawBytesTotal: metrics.CloudStorageDMLRawBytesCounter.
			WithLabelValues(namespace, changefeed),
		encodedBytesTotal: metrics.CloudStorageDMLEncodedBytesCounter.
			WithLabelValues(namespace, changefeed),
	}
}
