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

	"github.com/pingcap/ticdc/downstreamadapter/syncpoint"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
)

// SharedInfo contains all the shared configuration and resources
// that are common across all dispatchers within a DispatcherManager.
// This eliminates the need to pass these parameters individually to each dispatcher.
type SharedInfo struct {
	// Basic configuration
	changefeedID         common.ChangeFeedID
	timezone             string
	bdrMode              bool
	outputRawChangeEvent bool

	// Configuration objects
	integrityConfig *eventpb.IntegrityConfig
	// the config of filter
	filterConfig *eventpb.FilterConfig
	// if syncPointInfo is not nil, means enable Sync Point feature,
	syncPointConfig *syncpoint.SyncPointConfig

	// Shared resources
	// statusesChan is used to store the status of dispatchers when status changed
	// and push to heartbeatRequestQueue
	statusesChan chan TableSpanStatusWithSeq
	// blockStatusesChan use to collector block status of ddl/sync point event to Maintainer
	// shared by the event dispatcher manager
	blockStatusesChan chan *heartbeatpb.TableSpanBlockStatus
	// schemaIDToDispatchers is shared in the DispatcherManager,
	// it store all the infos about schemaID->Dispatchers
	// Dispatchers may change the schemaID when meets some special events, such as rename ddl
	// we use schemaIDToDispatchers to calculate the dispatchers that need to receive the dispatcher status
	schemaIDToDispatchers *SchemaIDToDispatchers
	// errCh is used to collect the errors that need to report to maintainer
	// such as error of flush ddl events
	errCh chan error
}

// NewSharedInfo creates a new SharedInfo with the given parameters
func NewSharedInfo(
	changefeedID common.ChangeFeedID,
	timezone string,
	bdrMode bool,
	outputRawChangeEvent bool,
	integrityConfig *eventpb.IntegrityConfig,
	filterConfig *eventpb.FilterConfig,
	syncPointConfig *syncpoint.SyncPointConfig,
	statusesChan chan TableSpanStatusWithSeq,
	blockStatusesChan chan *heartbeatpb.TableSpanBlockStatus,
	schemaIDToDispatchers *SchemaIDToDispatchers,
	errCh chan error,
) *SharedInfo {
	return &SharedInfo{
		changefeedID:          changefeedID,
		timezone:              timezone,
		bdrMode:               bdrMode,
		outputRawChangeEvent:  outputRawChangeEvent,
		integrityConfig:       integrityConfig,
		filterConfig:          filterConfig,
		syncPointConfig:       syncPointConfig,
		statusesChan:          statusesChan,
		blockStatusesChan:     blockStatusesChan,
		schemaIDToDispatchers: schemaIDToDispatchers,
		errCh:                 errCh,
	}
}

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
	return d.sharedInfo.changefeedID
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
	return d.sharedInfo.syncPointConfig != nil
}

func (d *BasicDispatcher) SetSeq(seq uint64) {
	d.seq = seq
}

func (d *BasicDispatcher) GetBDRMode() bool {
	return d.sharedInfo.bdrMode
}

func (d *BasicDispatcher) GetTimezone() string {
	return d.sharedInfo.timezone
}

func (d *BasicDispatcher) IsOutputRawChangeEvent() bool {
	return d.sharedInfo.outputRawChangeEvent
}

func (d *BasicDispatcher) GetFilterConfig() *eventpb.FilterConfig {
	return d.sharedInfo.filterConfig
}

func (d *BasicDispatcher) GetIntegrityConfig() *eventpb.IntegrityConfig {
	return d.sharedInfo.integrityConfig
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
	if d.sharedInfo.syncPointConfig != nil {
		return d.sharedInfo.syncPointConfig.SyncPointInterval
	}
	return time.Duration(0)
}

func (d *BasicDispatcher) GetTableSpan() *heartbeatpb.TableSpan {
	return d.tableSpan
}

func (d *BasicDispatcher) GetBlockStatusesChan() chan *heartbeatpb.TableSpanBlockStatus {
	return d.sharedInfo.blockStatusesChan
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

// SharedInfo methods
func (s *SharedInfo) IsOutputRawChangeEvent() bool {
	return s.outputRawChangeEvent
}

func (s *SharedInfo) GetSchemaIDToDispatchers() *SchemaIDToDispatchers {
	return s.schemaIDToDispatchers
}

func (s *SharedInfo) GetStatusesChan() chan TableSpanStatusWithSeq {
	return s.statusesChan
}

func (s *SharedInfo) GetBlockStatusesChan() chan *heartbeatpb.TableSpanBlockStatus {
	return s.blockStatusesChan
}

func (s *SharedInfo) GetErrCh() chan error {
	return s.errCh
}
