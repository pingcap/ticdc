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

package metrics

import (
	"os"
	"runtime"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

var (
	goGC = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "server",
			Name:      "go_gc",
			Help:      "The value of GOGC",
		})

	goMaxProcs = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "server",
			Name:      "go_max_procs",
			Help:      "The value of GOMAXPROCS",
		})

	// buildInfo is a metric with a constant '1' value, labeled by the build metadata.
	// It is used by dashboards to identify the exact binary running on each TiCDC server.
	BuildInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "server",
			Name:      "build_info",
			Help:      "TiCDC build information as labels.",
		},
		[]string{"release_version", "git_hash", "utc_build_time", "kernel_type"},
	)
)

// RecordGoRuntimeSettings records GOGC settings.
func RecordGoRuntimeSettings() {
	// The default GOGC value is 100. See debug.SetGCPercent.
	gogcValue := 100
	if val, err := strconv.Atoi(os.Getenv("GOGC")); err == nil {
		gogcValue = val
	}
	goGC.Set(float64(gogcValue))

	maxProcs := runtime.GOMAXPROCS(0)
	goMaxProcs.Set(float64(maxProcs))
}

func initServerMetrics(registry *prometheus.Registry) {
	registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	registry.MustRegister(collectors.NewGoCollector(
		collectors.WithGoCollections(collectors.GoRuntimeMemStatsCollection | collectors.GoRuntimeMetricsCollection)))
	registry.MustRegister(goGC)
	registry.MustRegister(goMaxProcs)
	registry.MustRegister(BuildInfo)
}
