// Copyright 2020 PingCAP, Inc.
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

package mysql

import (
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// ChangefeedDownstreamIsTiDBGauge indicates whether the downstream of a
// MySQL-compatible sink is confirmed to be TiDB (1 means yes).
//
// This metric is only set when the sink can positively identify TiDB (for
// example by executing `SELECT tidb_version()`), and is intentionally absent
// for MySQL or unknown downstreams. Dashboards can use it to override the
// scheme-derived "mysql" label value with "tidb" for running changefeeds.
var ChangefeedDownstreamIsTiDBGauge = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "ticdc",
		Subsystem: "sink",
		Name:      "changefeed_downstream_is_tidb",
		Help:      "Whether the downstream of a changefeed is confirmed to be TiDB (1 means yes).",
	}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

// RegisterMetrics registers the metrics owned by this package.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(ChangefeedDownstreamIsTiDBGauge)
}
