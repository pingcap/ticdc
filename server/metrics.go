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
	"github.com/pingcap/ticdc/api"
	"github.com/pingcap/ticdc/coordinator"
	"github.com/pingcap/ticdc/downstreamadapter"
	"github.com/pingcap/ticdc/logservice"
	"github.com/pingcap/ticdc/maintainer"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/logger"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/ticdc/pkg/txnutil/gc"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/prometheus/client_golang/prometheus"
)

var registry = prometheus.NewRegistry()

func registerMetrics() {
	metrics.RegisterMetrics(registry)
	coordinator.RegisterMetrics(registry)
	maintainer.RegisterMetrics(registry)
	downstreamadapter.RegisterMetrics(registry)
	api.RegisterMetrics(registry)
	logger.RegisterMetrics(registry)
	messaging.RegisterMetrics(registry)
	logservice.RegisterMetrics(registry)
	common.RegisterMetrics(registry)
	event.RegisterMetrics(registry)
	codec.RegisterMetrics(registry)
	gc.RegisterMetrics(registry)
	watcher.RegisterMetrics(registry)
}
