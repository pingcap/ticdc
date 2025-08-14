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

package dispatcher

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
)

func (d *BasicDispatcher) GetId() common.DispatcherID {
	return d.id
}

func (d *BasicDispatcher) GetSchemaID() int64 {
	return d.schemaID
}

func (d *BasicDispatcher) GetType() int {
	return d.dispatcherType
}

func (d *BasicDispatcher) GetChangefeedID() common.ChangeFeedID {
	return d.changefeedID
}

func (d *BasicDispatcher) GetComponentStatus() heartbeatpb.ComponentState {
	return d.componentStatus.Get()
}

func (d *BasicDispatcher) SetComponentStatus(status heartbeatpb.ComponentState) {
	d.componentStatus.Set(status)
}

func (d *BasicDispatcher) GetRemovingStatus() bool {
	return d.isRemoving.Load()
}

func (d *BasicDispatcher) EnableSyncPoint() bool {
	return d.syncPointConfig != nil
}

func (d *BasicDispatcher) SetSeq(seq uint64) {
	d.seq = seq
}

func (d *BasicDispatcher) GetBDRMode() bool {
	return d.bdrMode
}

func (d *BasicDispatcher) GetTimezone() string {
	return d.timezone
}

func (d *BasicDispatcher) IsOutputRawChangeEvent() bool {
	return d.outputRawChangeEvent
}

func (d *BasicDispatcher) GetFilterConfig() *eventpb.FilterConfig {
	return d.filterConfig
}

func (d *BasicDispatcher) GetIntegrityConfig() *eventpb.IntegrityConfig {
	return d.integrityConfig
}

func (d *BasicDispatcher) GetStartTs() uint64 {
	return d.startTs
}

func (d *BasicDispatcher) SetStartTsIsSyncpoint(startTsIsSyncpoint bool) {
	d.startTsIsSyncpoint = startTsIsSyncpoint
}

func (d *BasicDispatcher) GetStartTsIsSyncpoint() bool {
	return d.startTsIsSyncpoint
}

func (d *BasicDispatcher) GetSyncPointInterval() time.Duration {
	if d.syncPointConfig != nil {
		return d.syncPointConfig.SyncPointInterval
	}
	return time.Duration(0)
}

func (d *BasicDispatcher) GetTableSpan() *heartbeatpb.TableSpan {
	return d.tableSpan
}

func (d *BasicDispatcher) GetBlockStatusesChan() chan *heartbeatpb.TableSpanBlockStatus {
	return d.blockStatusesChan
}

func (d *BasicDispatcher) GetEventSizePerSecond() float32 {
	return d.tableProgress.GetEventSizePerSecond()
}

func (d *BasicDispatcher) IsTableTriggerEventDispatcher() bool {
	return d.tableSpan == common.DDLSpan
}

// SetStartTs only be called after the dispatcher is created
func (d *BasicDispatcher) SetStartTs(startTs uint64) {
	atomic.StoreUint64(&d.startTs, startTs)
	atomic.StoreUint64(&d.resolvedTs, startTs)
}

func (d *BasicDispatcher) SetCurrentPDTs(currentPDTs uint64) {
	d.creationPDTs = currentPDTs
}
