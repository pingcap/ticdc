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
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/syncpoint"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// SharedInfo contains all the shared configuration and resources
// that are common across all dispatchers within a DispatcherManager.
// This eliminates the need to pass these parameters individually to each dispatcher.
type SharedInfo struct {
	// Basic configuration
	changefeedID         common.ChangeFeedID
	timezone             string
	bdrMode              bool
	enableActiveActive   bool
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

	// Normal event dispatchers inherit these shared batch defaults.
	eventCollectorBatchCount int
	eventCollectorBatchBytes int

	// Shared resources
	// statusesChan is used to store the status of dispatchers when status changed
	// and push to heartbeatRequestQueue
	statusesChan chan TableSpanStatusWithSeq
	// blockStatusBuffer keeps block statuses for the dispatcher manager.
	// Identical WAITING and DONE statuses are coalesced while pending to reduce local memory amplification.
	blockStatusBuffer *BlockStatusBuffer

	// blockExecutor is used to execute block events such as DDL and sync point events asynchronously
	// to avoid callback() called in handleEvents, causing deadlock in ds
	blockExecutor *blockEventExecutor

	// errCh is used to collect the errors that need to report to maintainer
	// such as error of flush ddl events
	errCh chan error

	// metricHandleDDLHis records each DDL handling time duration,
	// which includes the time of executing the DDL and waiting for the DDL to be resolved.
	metricHandleDDLHis prometheus.Observer
}

// NewSharedInfo creates a new SharedInfo with the given parameters
func NewSharedInfo(
	changefeedID common.ChangeFeedID,
	timezone string,
	bdrMode bool,
	enableActiveActive bool,
	outputRawChangeEvent bool,
	integrityConfig *eventpb.IntegrityConfig,
	filterConfig *eventpb.FilterConfig,
	syncPointConfig *syncpoint.SyncPointConfig,
	txnAtomicity *config.AtomicityLevel,
	enableSplittableCheck bool,
	eventCollectorBatchCount int,
	eventCollectorBatchBytes int,
	statusesChan chan TableSpanStatusWithSeq,
	blockStatusBufferSize int,
	errCh chan error,
) *SharedInfo {
	sharedInfo := &SharedInfo{
		changefeedID:             changefeedID,
		timezone:                 timezone,
		bdrMode:                  bdrMode,
		enableActiveActive:       enableActiveActive,
		outputRawChangeEvent:     outputRawChangeEvent,
		integrityConfig:          integrityConfig,
		filterConfig:             filterConfig,
		syncPointConfig:          syncPointConfig,
		enableSplittableCheck:    enableSplittableCheck,
		eventCollectorBatchCount: eventCollectorBatchCount,
		eventCollectorBatchBytes: eventCollectorBatchBytes,
		statusesChan:             statusesChan,
		blockExecutor:            newBlockEventExecutor(),
		errCh:                    errCh,
		metricHandleDDLHis:       metrics.HandleDDLHistogram.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name()),
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

func (d *BasicDispatcher) GetEventCollectorBatchConfig() (batchCount int, batchBytes int) {
	return d.eventCollectorBatchCount, d.eventCollectorBatchBytes
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

func (d *BasicDispatcher) GetTryRemoving() bool {
	return d.tryRemoving.Load()
}

func (d *BasicDispatcher) SetTryRemoving() {
	d.tryRemoving.Store(true)
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

func (d *BasicDispatcher) EnableActiveActive() bool {
	return d.sharedInfo.EnableActiveActive()
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

func (d *BasicDispatcher) OfferBlockStatus(status *heartbeatpb.TableSpanBlockStatus) {
	d.sharedInfo.OfferBlockStatus(status)
}

func (d *BasicDispatcher) OfferDoneBlockStatus(blockTs uint64, isSyncPoint bool) {
	d.sharedInfo.OfferDoneBlockStatus(d.id, blockTs, isSyncPoint, d.GetMode())
}

func (d *BasicDispatcher) TakeBlockStatus(ctx context.Context) *heartbeatpb.TableSpanBlockStatus {
	return d.sharedInfo.TakeBlockStatus(ctx)
}

func (d *BasicDispatcher) TakeBlockStatusWithTimeout(timeout time.Duration) (*heartbeatpb.TableSpanBlockStatus, bool) {
	return d.sharedInfo.TakeBlockStatusWithTimeout(timeout)
}

func (d *BasicDispatcher) GetEventSizePerSecond() float32 {
	return d.tableProgress.GetEventSizePerSecond()
}

func (d *BasicDispatcher) IsTableTriggerDispatcher() bool {
	return d.tableSpan.Equal(common.KeyspaceDDLSpan(d.tableSpan.KeyspaceID))
}

// SetStartTs only be called after the dispatcher is created
func (d *BasicDispatcher) SetStartTs(startTs uint64) {
	atomic.StoreUint64(&d.startTs, startTs)
	d.resolvedTs.Store(startTs)
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

func (s *SharedInfo) EnableActiveActive() bool {
	return s.enableActiveActive
}

func (s *SharedInfo) OfferBlockStatus(status *heartbeatpb.TableSpanBlockStatus) {
	s.blockStatusBuffer.Offer(status)
}

func (s *SharedInfo) OfferDoneBlockStatus(dispatcherID common.DispatcherID, blockTs uint64, isSyncPoint bool, mode int64) {
	s.blockStatusBuffer.OfferDone(dispatcherID, blockTs, isSyncPoint, mode)
}

func (s *SharedInfo) TakeBlockStatus(ctx context.Context) *heartbeatpb.TableSpanBlockStatus {
	return s.blockStatusBuffer.Take(ctx)
}

func (s *SharedInfo) TakeBlockStatusWithTimeout(timeout time.Duration) (*heartbeatpb.TableSpanBlockStatus, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	status := s.TakeBlockStatus(ctx)
	if status == nil {
		return nil, false
	}
	return status, true
}

func (s *SharedInfo) TryTakeBlockStatus() (*heartbeatpb.TableSpanBlockStatus, bool) {
	return s.blockStatusBuffer.TryTake()
}

func (s *SharedInfo) BlockStatusLen() int {
	return s.blockStatusBuffer.Len()
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
	keyspace := s.changefeedID.Keyspace()
	changefeedID := s.changefeedID.Name()
	metrics.HandleDDLHistogram.DeleteLabelValues(keyspace, changefeedID)
}
