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
	"github.com/pingcap/ticdc/pkg/config"
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

	// The atomicity level of a transaction.
	txnAtomicity config.AtomicityLevel

	// enableSplittableCheck controls whether to check if a table is splittable before splitting.
	// If true, only tables with a primary key and no unique key can be split.
	// If false, all tables can be split without checking.
	// If true, we need to check whether a DDL event received by a split dispatcher
	// will break the splittability of this table.
	enableSplittableCheck bool

	// Shared resources
	// statusesChan is used to store the status of dispatchers when status changed
	// and push to heartbeatRequestQueue
	statusesChan chan TableSpanStatusWithSeq
	// blockStatusesChan use to collector block status of ddl/sync point event to Maintainer
	// shared by the event dispatcher manager
	blockStatusesChan chan *heartbeatpb.TableSpanBlockStatus

	// blockExecutor is used to execute block events such as DDL and sync point events asynchronously
	// to avoid callback() called in handleEvents, causing deadlock in ds
	blockExecutor *blockEventExecutor

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
	txnAtomicity *config.AtomicityLevel,
	enableSplittableCheck bool,
	statusesChan chan TableSpanStatusWithSeq,
	blockStatusesChan chan *heartbeatpb.TableSpanBlockStatus,
	errCh chan error,
) *SharedInfo {
	sharedInfo := &SharedInfo{
		changefeedID:          changefeedID,
		timezone:              timezone,
		bdrMode:               bdrMode,
		outputRawChangeEvent:  outputRawChangeEvent,
		integrityConfig:       integrityConfig,
		filterConfig:          filterConfig,
		syncPointConfig:       syncPointConfig,
		enableSplittableCheck: enableSplittableCheck,
		statusesChan:          statusesChan,
		blockStatusesChan:     blockStatusesChan,
		blockExecutor:         newBlockEventExecutor(),
		errCh:                 errCh,
	}

	if txnAtomicity != nil {
		sharedInfo.txnAtomicity = *txnAtomicity
	} else {
		sharedInfo.txnAtomicity = config.DefaultAtomicityLevel()
	}
	return sharedInfo
}

func (d *BasicDispatcher) GetId() common.DispatcherID {
	return d.id
}

func (d *BasicDispatcher) GetSchemaID() int64 {
	return d.schemaID
}

func (d *BasicDispatcher) GetMode() int64 {
	return d.mode
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

func (d *BasicDispatcher) SetSkipSyncpointAtStartTs(skipSyncpointAtStartTs bool) {
	d.skipSyncpointAtStartTs = skipSyncpointAtStartTs
}

func (d *BasicDispatcher) GetSkipSyncpointAtStartTs() bool {
	return d.skipSyncpointAtStartTs
}

func (d *BasicDispatcher) SetSkipDMLAsStartTs(skipDMLAsStartTs bool) {
	d.skipDMLAsStartTs = skipDMLAsStartTs
}

func (d *BasicDispatcher) GetSkipDMLAsStartTs() bool {
	return d.skipDMLAsStartTs
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

func (d *BasicDispatcher) GetTxnAtomicity() config.AtomicityLevel {
	return d.sharedInfo.txnAtomicity
}

func (d *BasicDispatcher) GetBlockStatusesChan() chan *heartbeatpb.TableSpanBlockStatus {
	return d.sharedInfo.blockStatusesChan
}

func (d *BasicDispatcher) GetEventSizePerSecond() float32 {
	return d.tableProgress.GetEventSizePerSecond()
}

func (d *BasicDispatcher) IsTableTriggerEventDispatcher() bool {
	return d.tableSpan.Equal(common.KeyspaceDDLSpan(d.tableSpan.KeyspaceID))
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

func (s *SharedInfo) GetStatusesChan() chan TableSpanStatusWithSeq {
	return s.statusesChan
}

func (s *SharedInfo) GetBlockStatusesChan() chan *heartbeatpb.TableSpanBlockStatus {
	return s.blockStatusesChan
}

func (s *SharedInfo) GetErrCh() chan error {
	return s.errCh
}

func (s *SharedInfo) GetBlockEventExecutor() *blockEventExecutor {
	return s.blockExecutor
}

func (s *SharedInfo) Close() {
	if s.blockExecutor != nil {
		s.blockExecutor.Close()
	}
}
