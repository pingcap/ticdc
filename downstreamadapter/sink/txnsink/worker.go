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
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/utils/chann"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Worker represents a worker that processes transaction groups and executes SQL
type Worker struct {
	workerID     int
	changefeedID common.ChangeFeedID
	config       *TxnSinkConfig

	// Core components
	sqlGenerator    *SQLGenerator
	dbExecutor      *DBExecutor
	progressTracker *ProgressTracker

	// Channels
	inputCh *chann.UnlimitedChannel[*TxnGroup, any]
	sqlChan *chann.UnlimitedChannel[*TxnSQL, any] // Simple FIFO channel for SQL batching

	// Statistics
	statistics *metrics.Statistics
}

// NewWorker creates a new worker instance
func NewWorker(
	workerID int,
	changefeedID common.ChangeFeedID,
	config *TxnSinkConfig,
	db *sql.DB,
	inputCh *chann.UnlimitedChannel[*TxnGroup, any],
	progressTracker *ProgressTracker,
	statistics *metrics.Statistics,
) *Worker {
	// Create unlimited channel for SQL batching
	sqlChan := chann.NewUnlimitedChannel[*TxnSQL, any](
		nil, // No grouping function needed
		func(txnSQL *TxnSQL) int {
			// Calculate SQL size for batching
			return len(txnSQL.SQL)
		},
	)

	return &Worker{
		workerID:        workerID,
		changefeedID:    changefeedID,
		config:          config,
		sqlGenerator:    NewSQLGenerator(),
		dbExecutor:      NewDBExecutor(db),
		progressTracker: progressTracker,
		inputCh:         inputCh,
		sqlChan:         sqlChan,
		statistics:      statistics,
	}
}

// Run starts the worker processing
func (w *Worker) Run(ctx context.Context) error {
	namespace := w.changefeedID.Namespace()
	changefeed := w.changefeedID.Name()

	log.Info("txnSink: starting worker",
		zap.String("namespace", namespace),
		zap.String("changefeed", changefeed),
		zap.Int("workerID", w.workerID))

	// Start multiple goroutines for different responsibilities
	eg, ctx := errgroup.WithContext(ctx)

	// Start transaction processor (converts TxnGroup to SQL)
	eg.Go(func() error {
		return w.processTransactions(ctx)
	})

	// Start SQL executor (executes SQL batches)
	eg.Go(func() error {
		return w.executeSQLBatches(ctx)
	})

	err := eg.Wait()
	if err != nil {
		log.Error("txnSink: worker stopped with error",
			zap.String("namespace", namespace),
			zap.String("changefeed", changefeed),
			zap.Int("workerID", w.workerID),
			zap.Error(err))
		return err
	}

	log.Info("txnSink: worker stopped normally",
		zap.String("namespace", namespace),
		zap.String("changefeed", changefeed),
		zap.Int("workerID", w.workerID))

	return nil
}

// processTransactions processes transaction groups from input channel and converts them to SQL
func (w *Worker) processTransactions(ctx context.Context) error {
	namespace := w.changefeedID.Namespace()
	changefeed := w.changefeedID.Name()

	log.Info("hyy processTransactions")

	buffer := make([]*TxnGroup, 0, w.config.BatchSize)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		default:
			// Get multiple txn groups from the channel
			txnGroups, ok := w.inputCh.GetMultipleNoGroup(buffer)
			if !ok {
				return errors.Trace(ctx.Err())
			}
			log.Info("hyy get txn group", zap.Int("txnGroupSize", len(txnGroups)))

			if len(txnGroups) == 0 {
				buffer = buffer[:0]
				continue
			}

			// Process each txn group
			for _, txnGroup := range txnGroups {
				if len(txnGroup.Events) == 0 {
					continue
				}
				if err := w.processTxnGroup(txnGroup); err != nil {
					log.Error("txnSink: failed to process transaction group",
						zap.String("namespace", namespace),
						zap.String("changefeed", changefeed),
						zap.Int("workerID", w.workerID),
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

// processTxnGroup converts a transaction group to SQL and pushes to sqlChan
func (w *Worker) processTxnGroup(txnGroup *TxnGroup) error {
	// Convert to SQL
	txnSQL, err := w.sqlGenerator.ConvertTxnGroupToSQL(txnGroup)
	if err != nil {
		return err
	}

	// Push to worker's own sqlChan
	w.sqlChan.Push(txnSQL)

	return nil
}

// executeSQLBatches processes SQL batches from the SQL channel and executes them
func (w *Worker) executeSQLBatches(ctx context.Context) error {
	namespace := w.changefeedID.Namespace()
	changefeed := w.changefeedID.Name()

	log.Info("txnSink: starting SQL batch executor",
		zap.String("namespace", namespace),
		zap.String("changefeed", changefeed),
		zap.Int("workerID", w.workerID))

	buffer := make([]*TxnSQL, 0, w.config.BatchSize)
	currentBatchSize := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Get multiple SQLs from the channel (no grouping needed)
			sqlBatch, ok := w.sqlChan.GetMultipleNoGroup(buffer, w.config.MaxSQLBatchSize)
			if !ok {
				return nil
			}

			if len(sqlBatch) == 0 {
				buffer = buffer[:0]
				continue
			}

			batch := make([]*TxnSQL, 0, w.config.BatchSize)

			log.Debug("txnSink: got sql batch",
				zap.String("namespace", namespace),
				zap.String("changefeed", changefeed),
				zap.Int("workerID", w.workerID),
				zap.Int("sqlBatchSize", len(sqlBatch)),
				zap.Int("batchSize", len(batch)),
				zap.Int("currentBatchSize", currentBatchSize))

			// Process each SQL in the batch, respecting size and count limits
			for _, txnSQL := range sqlBatch {
				// Calculate SQL size for this transaction
				sqlSize := w.calculateSQLSize(txnSQL)

				// Check if adding this SQL would exceed batch size limit
				if len(batch) > 0 && (currentBatchSize+sqlSize > w.config.MaxSQLBatchSize || len(batch) >= w.config.BatchSize) {
					// Execute current batch before adding new SQL
					if err := w.executeSQLBatch(batch); err != nil {
						log.Error("txnSink: failed to execute SQL batch",
							zap.String("namespace", namespace),
							zap.String("changefeed", changefeed),
							zap.Int("workerID", w.workerID),
							zap.Error(err))
						return err
					}

					// Reset batch
					batch = batch[:0]
					currentBatchSize = 0
				}

				// Add SQL to batch
				log.Debug("txnSink: add sql to batch",
					zap.String("namespace", namespace),
					zap.String("changefeed", changefeed),
					zap.Int("workerID", w.workerID),
					zap.Int("batchSize", len(batch)),
					zap.Int("currentBatchSize", currentBatchSize))
				batch = append(batch, txnSQL)
				currentBatchSize += sqlSize
			}

			if len(batch) > 0 {
				if err := w.executeSQLBatch(batch); err != nil {
					log.Error("txnSink: failed to execute SQL batch",
						zap.String("namespace", namespace),
						zap.String("changefeed", changefeed),
						zap.Int("workerID", w.workerID),
						zap.Error(err))
					return err
				}
			}
			buffer = buffer[:0]
		}
	}
}

// calculateSQLSize calculates the total size of SQL statements in a transaction
func (w *Worker) calculateSQLSize(txnSQL *TxnSQL) int {
	return len(txnSQL.SQL)
}

// executeSQLBatch executes a batch of SQL transactions
func (w *Worker) executeSQLBatch(batch []*TxnSQL) error {
	namespace := w.changefeedID.Namespace()
	changefeed := w.changefeedID.Name()

	log.Debug("txnSink: execute sql batch",
		zap.String("namespace", namespace),
		zap.String("changefeed", changefeed),
		zap.Int("workerID", w.workerID),
		zap.Int("batchSize", len(batch)))

	if len(batch) == 0 {
		return nil
	}

	err := w.dbExecutor.ExecuteSQLBatch(batch)
	if err != nil {
		return err
	}

	// Update flushed progress for all transactions in the batch
	for _, txnSQL := range batch {
		w.progressTracker.RemoveCompletedTxn(txnSQL.TxnGroup.CommitTs, txnSQL.TxnGroup.StartTs)
	}

	return nil
}

// Close closes the worker and releases resources
func (w *Worker) Close() {
	w.sqlChan.Close()
	w.dbExecutor.Close()
}
