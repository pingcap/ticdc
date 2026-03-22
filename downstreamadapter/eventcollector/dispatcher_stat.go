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

package eventcollector

import "github.com/pingcap/ticdc/downstreamadapter/dispatcher"

// dispatcherStat is the collector-local view of one dispatcher.
// It is intentionally split into three state buckets:
// 1. dispatcherControlState: request/ack convergence and target binding
// 2. dispatcherBootstrapState: ready/not-reusable/handshake processing
// 3. dispatcherDataState: ordered stream consumption
type dispatcherStat struct {
	target         dispatcher.DispatcherService
	eventCollector *EventCollector
	readyCallback  func()

	dispatcherControlState
	dispatcherBootstrapState
	dispatcherDataState
}

func newDispatcherStat(
	target dispatcher.DispatcherService,
	eventCollector *EventCollector,
	readyCallback func(),
) *dispatcherStat {
	stat := &dispatcherStat{
		target:         target,
		eventCollector: eventCollector,
		readyCallback:  readyCallback,
	}
	stat.initControlState()
	stat.initDataState(target.GetStartTs())
	return stat
}

func (d *dispatcherStat) run() {
	d.registerTo(d.eventCollector.getLocalServerID())
}

func (d *dispatcherStat) clear() {
	d.dispatcherControlState.clearBinding()
}
