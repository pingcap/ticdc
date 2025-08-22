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
	dbExecutor       *DBExecutor
	sqlGenerator     *SQLGenerator
	eventProcessor   *EventProcessor

	// Configuration
	config *TxnSinkConfig

	// Channels for coordination
	dmlEventChan   chan *commonEvent.DMLEvent
	checkpointChan chan uint64
	txnChan        chan *TxnGroup
	sqlChan        chan *TxnSQL

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
			MaxConcurrentTxns: 16,
			BatchSize:         16,
			FlushInterval:     100,
			MaxSQLBatchSize:   1024 * 16,
		}
	}

	txnStore := NewTxnStore()
	conflictDetector := NewConflictDetector(changefeedID)
	dbExecutor := NewDBExecutor(db)
	sqlGenerator := NewSQLGenerator()
	eventProcessor := NewEventProcessor(txnStore)

	return &Sink{
		changefeedID:     changefeedID,
		txnStore:         txnStore,
		conflictDetector: conflictDetector,
		dbExecutor:       dbExecutor,
		sqlGenerator:     sqlGenerator,
		eventProcessor:   eventProcessor,
		config:           config,
		dmlEventChan:     make(chan *commonEvent.DMLEvent, 10000),
		checkpointChan:   make(chan uint64, 100),
		txnChan:          make(chan *TxnGroup, 10000),
		sqlChan:          make(chan *TxnSQL, 10000),
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
	s.dmlEventChan <- event
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

	// Close database executor
	s.dbExecutor.Close()

	// Close channels
	close(s.dmlEventChan)
	close(s.checkpointChan)
	close(s.txnChan)
	close(s.sqlChan)

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
	s.conflictDetector.Run(ctx)

	// Start multiple transaction workers
	eg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < s.config.MaxConcurrentTxns; i++ {
		workerID := i
		eg.Go(func() error {
			return s.runTxnWorker(ctx, workerID)
		})
	}

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

	// Start SQL batch processor
	eg.Go(func() error {
		return s.processSQLBatch(ctx)
	})

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

// runTxnWorker runs a transaction worker (similar to mysqlSink's runDMLWriter)
func (s *Sink) runTxnWorker(ctx context.Context, idx int) error {
	namespace := s.changefeedID.Namespace()
	changefeed := s.changefeedID.Name()

	log.Info("txnSink: starting txn worker",
		zap.String("namespace", namespace),
		zap.String("changefeed", changefeed),
		zap.Int("workerID", idx))

	inputCh := s.conflictDetector.GetOutChByCacheID(idx)
	if inputCh == nil {
		return errors.New("failed to get output channel from conflict detector")
	}

	buffer := make([]*TxnGroup, 0, s.config.BatchSize)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		default:
			// Get multiple txn groups from the channel
			txnGroups, ok := inputCh.GetMultipleNoGroup(buffer)
			if !ok {
				return errors.Trace(ctx.Err())
			}

			if len(txnGroups) == 0 {
				buffer = buffer[:0]
				continue
			}

			// Process each txn group
			for _, txnGroup := range txnGroups {
				if err := s.processTxnGroup(txnGroup); err != nil {
					log.Error("txnSink: failed to process transaction group",
						zap.String("namespace", namespace),
						zap.String("changefeed", changefeed),
						zap.Int("workerID", idx),
						zap.Uint64("commitTs", txnGroup.CommitTs),
						zap.Uint64("startTs", txnGroup.StartTs),
						zap.Error(err))
					return err
				}
			}

			buffer = buffer[:0]
		}
	}
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

// processTxnGroup processes a single transaction group
func (s *Sink) processTxnGroup(txnGroup *TxnGroup) error {
	// Convert to SQL and send to SQL channel
	txnSQL, err := s.sqlGenerator.ConvertTxnGroupToSQL(txnGroup)
	if err != nil {
		return err
	}

	// Send to SQL channel for batch processing
	select {
	case s.sqlChan <- txnSQL:
		return nil
	default:
		return errors.New("SQL channel is full")
	}
}

// processSQLBatch processes SQL batches from the SQL channel
func (s *Sink) processSQLBatch(ctx context.Context) error {
	namespace := s.changefeedID.Namespace()
	changefeed := s.changefeedID.Name()

	log.Info("txnSink: starting SQL batch processor",
		zap.String("namespace", namespace),
		zap.String("changefeed", changefeed))

	batch := make([]*TxnSQL, 0, s.config.BatchSize)
	currentBatchSize := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case txnSQL, ok := <-s.sqlChan:
			if !ok {
				// Channel closed, flush remaining batch
				if len(batch) > 0 {
					if err := s.executeSQLBatch(batch); err != nil {
						log.Error("txnSink: failed to execute final SQL batch",
							zap.String("namespace", namespace),
							zap.String("changefeed", changefeed),
							zap.Error(err))
						return err
					}
				}
				return nil
			}

			// Calculate SQL size for this transaction
			sqlSize := s.calculateSQLSize(txnSQL)

			// Check if adding this SQL would exceed batch size limit
			if len(batch) > 0 && (currentBatchSize+sqlSize > s.config.MaxSQLBatchSize || len(batch) >= s.config.BatchSize) {
				// Execute current batch before adding new SQL
				if err := s.executeSQLBatch(batch); err != nil {
					log.Error("txnSink: failed to execute SQL batch",
						zap.String("namespace", namespace),
						zap.String("changefeed", changefeed),
						zap.Error(err))
					return err
				}

				// Reset batch
				batch = batch[:0]
				currentBatchSize = 0
			}

			// Add SQL to batch
			batch = append(batch, txnSQL)
			currentBatchSize += sqlSize
		}
	}
}

// calculateSQLSize calculates the total size of SQL statements in a transaction
func (s *Sink) calculateSQLSize(txnSQL *TxnSQL) int {
	totalSize := 0
	for _, sql := range txnSQL.SQLs {
		totalSize += len(sql)
	}
	return totalSize
}

// executeSQLBatch executes a batch of SQL transactions
func (s *Sink) executeSQLBatch(batch []*TxnSQL) error {
	if len(batch) == 0 {
		return nil
	}

	return s.dbExecutor.ExecuteSQLBatch(batch)
}
