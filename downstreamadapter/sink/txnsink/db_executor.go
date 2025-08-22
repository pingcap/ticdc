package txnsink

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

// DBExecutor handles database execution for transaction SQL
type DBExecutor struct {
	db *sql.DB
}

// NewDBExecutor creates a new database executor
func NewDBExecutor(db *sql.DB) *DBExecutor {
	return &DBExecutor{
		db: db,
	}
}

// ExecuteSQLBatch executes a batch of SQL transactions
func (e *DBExecutor) ExecuteSQLBatch(batch []*TxnSQL) error {
	if len(batch) == 0 {
		return nil
	}

	log.Debug("txnSink: executing SQL batch",
		zap.Int("batchSize", len(batch)))

	// If batch size is 1, execute directly (SQL already contains BEGIN/COMMIT)
	if len(batch) == 1 {
		txnSQL := batch[0]
		for _, sql := range txnSQL.SQLs {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_, execErr := e.db.ExecContext(ctx, sql)
			cancel()

			if execErr != nil {
				log.Error("txnSink: failed to execute single SQL",
					zap.String("sql", sql),
					zap.Uint64("commitTs", txnSQL.TxnGroup.CommitTs),
					zap.Uint64("startTs", txnSQL.TxnGroup.StartTs),
					zap.Error(execErr))
				return errors.Trace(execErr)
			}
		}

		log.Debug("txnSink: successfully executed single transaction",
			zap.Uint64("commitTs", txnSQL.TxnGroup.CommitTs),
			zap.Uint64("startTs", txnSQL.TxnGroup.StartTs))
		return nil
	}

	// For multiple transactions, use explicit transaction and combine SQLs
	tx, err := e.db.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Build combined SQL from all transactions
	var combinedSQL strings.Builder

	// Collect all SQL statements from all transactions
	for _, txnSQL := range batch {
		for _, sql := range txnSQL.SQLs {
			// Keep the original SQL with BEGIN/COMMIT
			cleanSQL := strings.TrimSpace(sql)
			if len(cleanSQL) > 0 {
				combinedSQL.WriteString(cleanSQL)
				if !strings.HasSuffix(cleanSQL, ";") {
					combinedSQL.WriteString(";")
				}
			}
		}
	}

	finalSQL := combinedSQL.String()

	log.Debug("txnSink: executing combined SQL batch with explicit transaction",
		zap.String("sql", finalSQL),
		zap.Int("batchSize", len(batch)))

	// Execute the combined SQL within transaction
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	_, execErr := tx.ExecContext(ctx, finalSQL)
	cancel()

	if execErr != nil {
		log.Error("txnSink: failed to execute SQL batch",
			zap.String("sql", finalSQL),
			zap.Int("batchSize", len(batch)),
			zap.Error(execErr))
		return errors.Trace(execErr)
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		log.Error("txnSink: failed to commit batch transaction",
			zap.Int("batchSize", len(batch)),
			zap.Error(err))
		return errors.Trace(err)
	}

	log.Debug("txnSink: successfully executed SQL batch",
		zap.Int("batchSize", len(batch)),
		zap.Int("sqlLength", len(finalSQL)))

	return nil
}

// Close closes the database connection
func (e *DBExecutor) Close() error {
	return e.db.Close()
}
