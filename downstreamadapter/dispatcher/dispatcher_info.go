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

package dispatcher

import (
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"go.uber.org/zap"
)

/*
 dispatcher_info.go is used to store some basic and easy function of the dispatcher
*/

func (d *Dispatcher) GetId() common.DispatcherID {
	return d.id
}

func (d *Dispatcher) GetChangefeedID() common.ChangeFeedID {
	return d.changefeedID
}

func (d *Dispatcher) GetSchemaID() int64 {
	return d.schemaID
}

func (d *Dispatcher) EnableSyncPoint() bool {
	return d.syncPointConfig != nil
}

func (d *Dispatcher) GetIntegrityConfig() *eventpb.IntegrityConfig {
	return d.integrityConfig
}

func (d *Dispatcher) GetTimezone() string {
	return d.timezone
}

func (d *Dispatcher) GetFilterConfig() *eventpb.FilterConfig {
	return d.filterConfig
}

func (d *Dispatcher) GetSyncPointInterval() time.Duration {
	if d.syncPointConfig != nil {
		return d.syncPointConfig.SyncPointInterval
	}
	return time.Duration(0)
}

func (d *Dispatcher) GetStartTsIsSyncpoint() bool {
	return d.startTsIsSyncpoint
}

func (d *Dispatcher) SetStartTsIsSyncpoint(startTsIsSyncpoint bool) {
	d.startTsIsSyncpoint = startTsIsSyncpoint
}

func (d *Dispatcher) GetComponentStatus() heartbeatpb.ComponentState {
	return d.componentStatus.Get()
}

func (d *Dispatcher) SetComponentStatus(status heartbeatpb.ComponentState) {
	d.componentStatus.Set(status)
}

func (d *Dispatcher) GetRemovingStatus() bool {
	return d.isRemoving.Load()
}

func (d *Dispatcher) GetEventSizePerSecond() float32 {
	return d.tableProgress.GetEventSizePerSecond()
}

func (d *Dispatcher) IsTableTriggerEventDispatcher() bool {
	return d.tableSpan == common.DDLSpan
}

func (d *Dispatcher) SetSeq(seq uint64) {
	d.seq = seq
}

func (d *Dispatcher) GetBDRMode() bool {
	return d.bdrMode
}

func (d *Dispatcher) GetTableSpan() *heartbeatpb.TableSpan {
	return d.tableSpan
}

func (d *Dispatcher) GetStartTs() uint64 {
	return d.startTs
}

// addToDynamicStream add self to dynamic stream
func (d *Dispatcher) addToStatusDynamicStream() {
	dispatcherStatusDS := GetDispatcherStatusDynamicStream()
	err := dispatcherStatusDS.AddPath(d.id, d)
	if err != nil {
		log.Error("add dispatcher to dynamic stream failed",
			zap.Stringer("changefeedID", d.changefeedID),
			zap.Stringer("dispatcher", d.id),
			zap.Error(err))
	}
}

// SetStartTs only be called after the dispatcher is created
func (d *Dispatcher) SetStartTs(startTs uint64) {
	d.startTs = startTs
	d.resolvedTs = startTs
}

func (d *Dispatcher) SetCurrentPDTs(currentPDTs uint64) {
	d.creationPDTs = currentPDTs
}
