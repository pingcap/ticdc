package txnsink

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strings"
	"time"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"go.uber.org/zap"
)

const (
	// BackoffBaseDelay indicates the base delay time for retrying.
	BackoffBaseDelay = 500 * time.Millisecond
	// BackoffMaxDelay indicates the max delay time for retrying.
	BackoffMaxDelay = 60 * time.Second
	// DefaultDMLMaxRetry is the default maximum number of retries for DML operations
	DefaultDMLMaxRetry = 8
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

// ExecuteSQLBatch executes a batch of SQL transactions with retry mechanism
func (e *DBExecutor) ExecuteSQLBatch(batch []*TxnSQL) error {
	if len(batch) == 0 {
		return nil
	}

	log.Info("txnSink: executing SQL batch",
		zap.Int("batchSize", len(batch)))

	// Define the execution function that will be retried
	tryExec := func() error {
		// If batch size is 1, execute directly (SQL already contains BEGIN/COMMIT)
		if len(batch) == 1 {
			txnSQL := batch[0]
			// Skip execution if SQL is empty
			if txnSQL.SQL == "" {
				log.Debug("txnSink: skipping empty SQL execution",
					zap.Uint64("commitTs", txnSQL.TxnGroup.CommitTs),
					zap.Uint64("startTs", txnSQL.TxnGroup.StartTs))
				return nil
			}

			log.Info("hyy execute single sql",
				zap.String("sql", txnSQL.SQL),
				zap.Uint64("commitTs", txnSQL.TxnGroup.CommitTs),
				zap.Uint64("startTs", txnSQL.TxnGroup.StartTs))

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_, execErr := e.db.ExecContext(ctx, txnSQL.SQL, txnSQL.Args...)
			cancel()

			if execErr != nil {
				log.Error("txnSink: failed to execute single SQL",
					zap.String("sql", txnSQL.SQL),
					zap.Uint64("commitTs", txnSQL.TxnGroup.CommitTs),
					zap.Uint64("startTs", txnSQL.TxnGroup.StartTs),
					zap.Error(execErr))
				return errors.Trace(execErr)
			}

			log.Info("txnSink: successfully executed single transaction",
				zap.Uint64("commitTs", txnSQL.TxnGroup.CommitTs),
				zap.Uint64("startTs", txnSQL.TxnGroup.StartTs))
			return nil
		}

		// For multiple transactions, combine them into a single SQL statement
		var combinedSQL []string
		var combinedArgs []interface{}

		for _, txnSQL := range batch {
			// Skip execution if SQL is empty
			if txnSQL.SQL == "" {
				log.Debug("txnSink: skipping empty SQL execution in batch",
					zap.Uint64("commitTs", txnSQL.TxnGroup.CommitTs),
					zap.Uint64("startTs", txnSQL.TxnGroup.StartTs))
				continue
			}

			combinedSQL = append(combinedSQL, txnSQL.SQL)
			combinedArgs = append(combinedArgs, txnSQL.Args...)
		}

		if len(combinedSQL) == 0 {
			log.Debug("txnSink: no valid SQL to execute in batch")
			return nil
		}

		// Join all SQL statements with semicolons
		finalSQL := strings.Join(combinedSQL, ";")

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, execErr := e.db.ExecContext(ctx, finalSQL, combinedArgs...)
		cancel()

		if execErr != nil {
			log.Error("txnSink: failed to execute combined SQL batch",
				zap.String("sql", finalSQL),
				zap.Int("batchSize", len(batch)),
				zap.Error(execErr))
			return errors.Trace(execErr)
		}

		log.Debug("txnSink: successfully executed combined SQL batch",
			zap.String("sql", finalSQL),
			zap.Int("batchSize", len(batch)))

		return nil
	}

	// Use retry mechanism
	return retry.Do(context.Background(), func() error {
		err := tryExec()
		if err != nil {
			log.Warn("txnSink: SQL execution failed, will retry",
				zap.Int("batchSize", len(batch)),
				zap.Error(err))
		}
		return err
	}, retry.WithBackoffBaseDelay(BackoffBaseDelay.Milliseconds()),
		retry.WithBackoffMaxDelay(BackoffMaxDelay.Milliseconds()),
		retry.WithMaxTries(DefaultDMLMaxRetry),
		retry.WithIsRetryableErr(isRetryableDMLError))
}

// isRetryableDMLError determines if a DML error is retryable
func isRetryableDMLError(err error) bool {
	// Check if it's a retryable error
	if !errors.IsRetryableError(err) {
		return false
	}

	// Check for specific MySQL error codes
	if mysqlErr, ok := errors.Cause(err).(*dmysql.MySQLError); ok {
		switch mysqlErr.Number {
		case uint16(mysql.ErrNoSuchTable), uint16(mysql.ErrBadDB), uint16(mysql.ErrDupEntry):
			return false
		}
	}

	// Check for driver errors
	if err == driver.ErrBadConn {
		return true
	}

	return true
}

// Close closes the database connection
func (e *DBExecutor) Close() error {
	return e.db.Close()
}
