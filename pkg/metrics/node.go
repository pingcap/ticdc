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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// ServerNodeInfo exposes node information for monitoring
	ServerNodeInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "server",
			Name:      "node_info",
			Help:      "Information about TiCDC server nodes",
		}, []string{"node_id", "address"},
	)
)

// InitNodeMetrics registers node-related metrics
func InitNodeMetrics(registry *prometheus.Registry) {
	registry.MustRegister(ServerNodeInfo)
}
