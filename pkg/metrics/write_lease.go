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
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	CaptureWriteGateState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "server",
			Name:      "capture_write_gate_state",
			Help:      "Whether the capture write gate is in the labeled state.",
		}, []string{"state"})
	CaptureP2PLeaseRemainingSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "server",
			Name:      "capture_p2p_lease_remaining_seconds",
			Help:      "Remaining lifetime of the capture P2P write lease.",
		})
	CaptureEtcdProofRemainingSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "server",
			Name:      "capture_etcd_proof_remaining_seconds",
			Help:      "Remaining lifetime of the capture etcd write proof.",
		})
	CaptureWriteBlockCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "server",
			Name:      "capture_write_block_total",
			Help:      "Number of capture write gate transitions from writable to blocked.",
		}, []string{"reason"})
	CaptureLastWriteAdmissionTimestamp = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "server",
			Name:      "capture_last_write_admission_timestamp_seconds",
			Help:      "Unix timestamp of the most recent downstream write admitted by this capture.",
		})
	CaptureLeaseResponseRejectedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "server",
			Name:      "capture_lease_response_rejected_total",
			Help:      "Number of rejected P2P write lease responses.",
		}, []string{"reason"})
	CaptureLeaseHeartbeatCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "coordinator",
			Name:      "capture_lease_heartbeat_total",
			Help:      "Number of capture write lease heartbeats by handling result.",
		}, []string{"result"})
	CaptureLeaseResponseCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "server",
			Name:      "capture_lease_response_total",
			Help:      "Number of capture write lease responses by handling result.",
		}, []string{"result"})
	CaptureSafeToRescheduleDelaySeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "server",
			Name:      "capture_safe_to_reschedule_delay_seconds",
			Help:      "Delay after capture lease-key deletion before removal is published.",
		})
	CaptureP2PWitnessAvailable = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "coordinator",
			Name:      "capture_p2p_witness_available",
			Help:      "Whether a remote capture is available to witness the coordinator capture.",
		})
)

func initCaptureWriteLeaseMetrics(registry *prometheus.Registry) {
	registry.MustRegister(CaptureWriteGateState)
	registry.MustRegister(CaptureP2PLeaseRemainingSeconds)
	registry.MustRegister(CaptureEtcdProofRemainingSeconds)
	registry.MustRegister(CaptureWriteBlockCounter)
	registry.MustRegister(CaptureLastWriteAdmissionTimestamp)
	registry.MustRegister(CaptureLeaseResponseRejectedCounter)
	registry.MustRegister(CaptureLeaseHeartbeatCounter)
	registry.MustRegister(CaptureLeaseResponseCounter)
	registry.MustRegister(CaptureSafeToRescheduleDelaySeconds)
	registry.MustRegister(CaptureP2PWitnessAvailable)
}
