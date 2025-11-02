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
	"context"
	"database/sql"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Sink implements the txnSink interface for transaction-level SQL output
type Sink struct {
	changefeedID common.ChangeFeedID

	// Core components
	txnStore         *TxnStore
	conflictDetector *ConflictDetector
	eventProcessor   *EventProcessor
	progressTracker  *ProgressTracker

	// Workers
	workers []*Worker

	// Configuration
	config *TxnSinkConfig

	// Channels for coordination
	dmlEventChan   chan *commonEvent.DMLEvent
	checkpointChan chan uint64
	txnChan        chan *TxnGroup

	// State management
	isNormal *atomic.Bool
	ctx      context.Context

	// Statistics and metrics
	statistics *metrics.Statistics
}

// New creates a new txnSink instance
func New(ctx context.Context, changefeedID common.ChangeFeedID, db *sql.DB, config *TxnSinkConfig) *Sink {
	if config == nil {
		config = &TxnSinkConfig{
			MaxConcurrentTxns: 32,
			BatchSize:         256,
			MaxSQLBatchSize:   1024 * 16,
		}
	}

	txnStore := NewTxnStore()
	conflictDetector := NewConflictDetector(changefeedID, config.MaxConcurrentTxns)
	progressTracker := NewProgressTrackerWithMonitor(changefeedID.Namespace(), changefeedID.Name())
	eventProcessor := NewEventProcessor(txnStore, progressTracker)

	// Create workers
	workers := make([]*Worker, config.MaxConcurrentTxns)
	for i := 0; i < config.MaxConcurrentTxns; i++ {
		inputCh := conflictDetector.GetOutChByCacheID(i)
		if inputCh == nil {
			log.Error("txnSink: failed to get output channel from conflict detector",
				zap.String("namespace", changefeedID.Namespace()),
				zap.String("changefeed", changefeedID.Name()),
				zap.Int("workerID", i))
			continue
		}
		workers[i] = NewWorker(i, changefeedID, config, db, inputCh, progressTracker, metrics.NewStatistics(changefeedID, "txnsink"))
	}

	return &Sink{
		changefeedID:     changefeedID,
		txnStore:         txnStore,
		conflictDetector: conflictDetector,
		eventProcessor:   eventProcessor,
		progressTracker:  progressTracker,
		workers:          workers,
		config:           config,
		dmlEventChan:     make(chan *commonEvent.DMLEvent, 10000),
		checkpointChan:   make(chan uint64, 100),
		txnChan:          make(chan *TxnGroup, 10000),
		isNormal:         atomic.NewBool(true),
		ctx:              ctx,
		statistics:       metrics.NewStatistics(changefeedID, "txnsink"),
	}
}

// SinkType returns the sink type
func (s *Sink) SinkType() common.SinkType {
	return common.TxnSinkType
}

// IsNormal returns whether the sink is in normal state
func (s *Sink) IsNormal() bool {
	return s.isNormal.Load()
}

// AddDMLEvent adds a DML event to the sink
func (s *Sink) AddDMLEvent(event *commonEvent.DMLEvent) {
	// Note: We don't add to pending here, as we need to wait for txnGroup formation
	s.dmlEventChan <- event
	event.PostFlush()
}

// AddCheckpointTs adds a checkpoint timestamp to trigger transaction processing
func (s *Sink) AddCheckpointTs(ts uint64) {
	s.checkpointChan <- ts
}

// WriteBlockEvent writes a block event (not supported in txnSink)
func (s *Sink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	return errors.New("txnSink does not support block events")
}

// SetTableSchemaStore sets the table schema store (not used in txnSink)
func (s *Sink) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
	// Not used in txnSink
}

// Close closes the sink and releases resources
func (s *Sink) Close(removeChangefeed bool) {
	s.isNormal.Store(false)

	// Close conflict detector
	s.conflictDetector.Close()

	// Close all workers
	for _, worker := range s.workers {
		if worker != nil {
			worker.Close()
		}
	}

	// Close progress tracker
	s.progressTracker.Close()

	// Close channels
	close(s.dmlEventChan)
	close(s.checkpointChan)
	close(s.txnChan)

	log.Info("txnSink: closed",
		zap.String("namespace", s.changefeedID.Namespace()),
		zap.String("changefeed", s.changefeedID.Name()))
}

// Run starts the sink processing
func (s *Sink) Run(ctx context.Context) error {
	namespace := s.changefeedID.Namespace()
	changefeed := s.changefeedID.Name()

	log.Info("txnSink: starting",
		zap.String("namespace", namespace),
		zap.String("changefeed", changefeed))

	// Start conflict detector
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return s.conflictDetector.Run(ctx)
	})

	// Start event processor for DML events
	eg.Go(func() error {
		return s.eventProcessor.ProcessDMLEvents(ctx, s.dmlEventChan)
	})

	// Start event processor for checkpoints
	eg.Go(func() error {
		return s.eventProcessor.ProcessCheckpoints(ctx, s.checkpointChan, s.txnChan)
	})

	// Start transaction processor
	eg.Go(func() error {
		return s.processTransactions(ctx)
	})

	// Start all workers
	for _, worker := range s.workers {
		if worker != nil {
			worker := worker
			eg.Go(func() error {
				return worker.Run(ctx)
			})
		}
	}

	err := eg.Wait()
	if err != nil {
		log.Error("txnSink: stopped with error",
			zap.String("namespace", namespace),
			zap.String("changefeed", changefeed),
			zap.Error(err))
		return err
	}

	log.Info("txnSink: stopped normally",
		zap.String("namespace", namespace),
		zap.String("changefeed", changefeed))

	return nil
}

// processTransactions processes transactions from the transaction channel
func (s *Sink) processTransactions(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case txnGroup, ok := <-s.txnChan:
			if !ok {
				return nil
			}
			// Add transaction group to conflict detector
			s.conflictDetector.AddTxnGroup(txnGroup)
		}
	}
}
