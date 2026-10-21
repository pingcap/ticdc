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

package schemastore

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	schemaStoreResolvedTsLagGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "schema_store",
			Name:      "resolved_ts_lag",
			Help:      "The resolved ts lag of schema store in seconds",
		})

	schemaStoreResolvedRegisterTableGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "schema_store",
			Name:      "register_table_num",
			Help:      "The number of registered tables in schema store",
		})

	schemaStoreGetTableInfoCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "schema_store",
			Name:      "get_table_info_count",
			Help:      "The number of GetTableInfo requests",
		})

	schemaStoreGetTableInfoLagHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "schema_store",
			Name:      "get_table_info_duration",
			Help:      "The duration of GetTableInfo requests",
			Buckets:   prometheus.ExponentialBuckets(0.00001, 2.0, 28), // 10us to 1.5h
		})

	// schemaStoreWaitResolvedTsDurationHist is the histogram of waiting duration for resolved ts in schema store.
	schemaStoreWaitResolvedTsDurationHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "schema_store",
			Name:      "wait_resolved_ts_duration",
			Help:      "The duration of waiting for resolved ts in schema store.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
		})
)

// RegisterMetrics registers the metrics owned by this package.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(schemaStoreResolvedTsLagGauge)
	registry.MustRegister(schemaStoreResolvedRegisterTableGauge)
	registry.MustRegister(schemaStoreGetTableInfoCounter)
	registry.MustRegister(schemaStoreGetTableInfoLagHist)
	registry.MustRegister(schemaStoreWaitResolvedTsDurationHist)
}
