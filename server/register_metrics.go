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

package server

import (
	"github.com/pingcap/ticdc/api/middleware"
	"github.com/pingcap/ticdc/coordinator"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/coordinator/operator"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/downstreamadapter/dispatchermanager"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcherorchestrator"
	"github.com/pingcap/ticdc/downstreamadapter/eventcollector"
	"github.com/pingcap/ticdc/downstreamadapter/sink/cloudstorage"
	"github.com/pingcap/ticdc/downstreamadapter/sink/metrics"
	mysqlSink "github.com/pingcap/ticdc/downstreamadapter/sink/mysql"
	"github.com/pingcap/ticdc/downstreamadapter/sink/mysql/causality"
	"github.com/pingcap/ticdc/downstreamadapter/sink/redo"
	"github.com/pingcap/ticdc/logservice/coordinator"
	"github.com/pingcap/ticdc/logservice/eventstore"
	"github.com/pingcap/ticdc/logservice/logpuller"
	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/logservice/txnutil"
	"github.com/pingcap/ticdc/maintainer"
	maintainerOperator "github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/eventservice"
	"github.com/pingcap/ticdc/pkg/logger"
	"github.com/pingcap/ticdc/pkg/messaging"
	redoCommon "github.com/pingcap/ticdc/pkg/redo/common"
	"github.com/pingcap/ticdc/pkg/redo/writer"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	"github.com/pingcap/ticdc/pkg/sink/kafka/claimcheck"
	"github.com/pingcap/ticdc/pkg/sink/mysql"
	"github.com/pingcap/ticdc/pkg/sink/pulsar"
	"github.com/pingcap/ticdc/pkg/txnutil/gc"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/prometheus/client_golang/prometheus"
)

var registry = prometheus.NewRegistry()

func registerMetrics() {
	RegisterMetrics(registry)
	watcher.RegisterMetrics(registry)
	middleware.RegisterMetrics(registry)
	coordinator.RegisterMetrics(registry)
	changefeed.RegisterMetrics(registry)
	operator.RegisterMetrics(registry)
	dispatcher.RegisterMetrics(registry)
	dispatchermanager.RegisterMetrics(registry)
	dispatcherorchestrator.RegisterMetrics(registry)
	eventcollector.RegisterMetrics(registry)
	metrics.RegisterMetrics(registry)
	cloudstorage.RegisterMetrics(registry)
	mysqlSink.RegisterMetrics(registry)
	causality.RegisterMetrics(registry)
	redo.RegisterMetrics(registry)
	logcoordinator.RegisterMetrics(registry)
	eventstore.RegisterMetrics(registry)
	logpuller.RegisterMetrics(registry)
	schemastore.RegisterMetrics(registry)
	txnutil.RegisterMetrics(registry)
	maintainer.RegisterMetrics(registry)
	maintainerOperator.RegisterMetrics(registry)
	replica.RegisterMetrics(registry)
	common.RegisterMetrics(registry)
	event.RegisterMetrics(registry)
	eventservice.RegisterMetrics(registry)
	logger.RegisterMetrics(registry)
	messaging.RegisterMetrics(registry)
	redoCommon.RegisterMetrics(registry)
	writer.RegisterMetrics(registry)
	codec.RegisterMetrics(registry)
	codecCommon.RegisterMetrics(registry)
	kafka.RegisterMetrics(registry)
	claimcheck.RegisterMetrics(registry)
	mysql.RegisterMetrics(registry)
	pulsar.RegisterMetrics(registry)
	gc.RegisterMetrics(registry)
	dynstream.RegisterMetrics(registry)
}
