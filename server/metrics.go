// Copyright 2024 PingCAP, Inc.
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

package server

import (
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/version"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// buildInfo is a metric with a constant '1' value, labeled by the build metadata.
	// It is used by dashboards to identify the exact binary running on each TiCDC server,
	// including whether the process is running in TiCDC's new or old architecture mode.
	buildInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "server",
			Name:      "build_info",
			Help:      "TiCDC build information as labels.",
		},
		[]string{"release_version", "git_hash", "utc_build_time", "kernel_type", "ticdc_arch"},
	)

	captureWriteGateState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "server",
			Name:      "capture_write_gate_state",
			Help:      "Whether the capture write gate is in the labeled state.",
		}, []string{"state"})

	captureP2PLeaseRemainingSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "server",
			Name:      "capture_p2p_lease_remaining_seconds",
			Help:      "Remaining lifetime of the capture P2P write lease.",
		})

	captureEtcdProofRemainingSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "server",
			Name:      "capture_etcd_proof_remaining_seconds",
			Help:      "Remaining lifetime of the capture etcd write proof.",
		})

	captureWriteBlockCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "server",
			Name:      "capture_write_block_total",
			Help:      "Number of capture write gate transitions from writable to blocked.",
		}, []string{"reason"})
)

// RecordBuildInfo records the server build metadata.
func RecordBuildInfo() {
	buildInfo.WithLabelValues(version.ReleaseVersion, version.GitHash, version.BuildTS, kerneltype.Name(), "newarch").Set(1)
}

// RegisterMetrics registers capture and process metrics.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(captureWriteGateState)
	registry.MustRegister(captureP2PLeaseRemainingSeconds)
	registry.MustRegister(captureEtcdProofRemainingSeconds)
	registry.MustRegister(captureWriteBlockCounter)
	registry.MustRegister(buildInfo)
	metrics.RegisterMetrics(registry)
}
