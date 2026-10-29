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

package downstreamadapter

import (
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcherorchestrator"
	"github.com/pingcap/ticdc/downstreamadapter/eventcollector"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/prometheus/client_golang/prometheus"
)

// RegisterMetrics registers metrics for the downstream adapter components.
func RegisterMetrics(registry *prometheus.Registry) {
	dispatcher.RegisterMetrics(registry)
	dispatcherorchestrator.RegisterMetrics(registry)
	eventcollector.RegisterMetrics(registry)
	sink.RegisterMetrics(registry)
}
