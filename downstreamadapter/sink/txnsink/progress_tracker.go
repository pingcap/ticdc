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

package txnsink

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

// TxnKey represents a unique transaction identifier
type TxnKey struct {
	commitTs uint64
	startTs  uint64
}

// ProgressTracker tracks the progress of data processing and flushing
type ProgressTracker struct {
	// Current progress state
	checkpointTs uint64

	// Track pending transactions using list + map pattern (like tableProgress)
	list    *list.List               // Maintains order (FIFO)
	elemMap map[TxnKey]*list.Element // TxnKey -> list.Element for O(1) removal

	// Thread safety
	mu sync.RWMutex

	// Monitoring
	pdClock        pdutil.Clock
	cancelMonitor  context.CancelFunc
	changefeedName string
}

// NewProgressTracker creates a new progress tracker
func NewProgressTracker() *ProgressTracker {
	return &ProgressTracker{
		checkpointTs: 0,
		list:         list.New(),
		elemMap:      make(map[TxnKey]*list.Element),
	}
}

// NewProgressTrackerWithMonitor creates a new progress tracker with monitoring enabled
func NewProgressTrackerWithMonitor(changefeedName string) *ProgressTracker {
	pt := &ProgressTracker{
		checkpointTs:   0,
		list:           list.New(),
		elemMap:        make(map[TxnKey]*list.Element),
		changefeedName: changefeedName,
	}

	pt.pdClock = appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock)

	// Start monitoring goroutine
	ctx, cancel := context.WithCancel(context.Background())
	pt.cancelMonitor = cancel
	go pt.runMonitor(ctx)

	return pt
}

// AddPendingTxn adds a pending transaction to track
func (pt *ProgressTracker) AddPendingTxn(commitTs, startTs uint64) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	key := TxnKey{commitTs: commitTs, startTs: startTs}
	// Add to list (maintains order)
	elem := pt.list.PushBack(key)
	pt.elemMap[key] = elem
}

// RemoveCompletedTxn removes a completed transaction from pending list
func (pt *ProgressTracker) RemoveCompletedTxn(commitTs, startTs uint64) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	key := TxnKey{commitTs: commitTs, startTs: startTs}
	if elem, ok := pt.elemMap[key]; ok {
		pt.list.Remove(elem)
		delete(pt.elemMap, key)
	}
}

// UpdateCheckpointTs updates the latest checkpoint TS received
func (pt *ProgressTracker) UpdateCheckpointTs(ts uint64) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	if ts > pt.checkpointTs {
		pt.checkpointTs = ts
	}
}

// calculateEffectiveTs calculates the effective progress TS
// If there are pending transactions: effectiveTs = min(pendingTxns) - 1
// If no pending transactions: effectiveTs = checkpointTs
func (pt *ProgressTracker) calculateEffectiveTs() uint64 {
	if pt.list.Len() > 0 {
		// Return min(pendingTxns) - 1 (first element in list)
		key := pt.list.Front().Value.(TxnKey)
		return key.commitTs - 1
	}

	// No pending transactions, use checkpointTs
	return pt.checkpointTs
}

// Reset resets the progress tracker to initial state
func (pt *ProgressTracker) Reset() {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.checkpointTs = 0
	pt.list.Init()                              // Clear list
	pt.elemMap = make(map[TxnKey]*list.Element) // Clear map
}

// Close stops the monitoring goroutine
func (pt *ProgressTracker) Close() {
	if pt.cancelMonitor != nil {
		pt.cancelMonitor()
	}
}

// runMonitor runs the monitoring goroutine that prints progress and lag every second
func (pt *ProgressTracker) runMonitor(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	log.Info("txnSink: ProgressTracker monitor started", zap.String("changefeed", pt.changefeedName))

	for {
		select {
		case <-ctx.Done():
			log.Info("txnSink: ProgressTracker monitor stopped", zap.String("changefeed", pt.changefeedName))
			return
		case <-ticker.C:
			pt.printProgress()
		}
	}
}

// printProgress prints current progress and lag information
func (pt *ProgressTracker) printProgress() {
	pt.mu.RLock()
	effectiveTs := pt.calculateEffectiveTs()
	checkpointTs := pt.checkpointTs
	pendingCount := pt.list.Len()
	pt.mu.RUnlock()

	if pt.pdClock == nil {
		log.Info("txnSink: Progress status",
			zap.String("changefeed", pt.changefeedName),
			zap.Uint64("effectiveTs", effectiveTs),
			zap.Uint64("checkpointTs", checkpointTs),
			zap.Int("pendingCount", pendingCount))
		return
	}

	// Calculate lag using the same logic as maintainer
	pdTime := pt.pdClock.CurrentTime()
	phyEffectiveTs := oracle.ExtractPhysical(effectiveTs)
	lag := float64(oracle.GetPhysical(pdTime)-phyEffectiveTs) / 1e3 // Convert to seconds

	log.Info("txnSink: Progress status",
		zap.String("changefeed", pt.changefeedName),
		zap.Uint64("effectiveTs", effectiveTs),
		zap.Int64("phyEffectiveTs", phyEffectiveTs),
		zap.Uint64("checkpointTs", checkpointTs),
		zap.Int("pendingCount", pendingCount),
		zap.Float64("lagSeconds", lag))
}
