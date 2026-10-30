// Copyright 2025 PingCAP, Inc.
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

package pulsar

import "github.com/prometheus/client_golang/prometheus"

// metricRegistry is set during server startup, before any Pulsar clients are created.
var metricRegistry *prometheus.Registry

// RegisterMetrics configures the shared registry for Pulsar client metrics.
func RegisterMetrics(registry *prometheus.Registry) {
	metricRegistry = registry
}
