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
		// Filter out empty SQL statements
		var validSQLs []string
		var validArgs []interface{}

		for _, txnSQL := range batch {
			if txnSQL.SQL != "" {
				validSQLs = append(validSQLs, txnSQL.SQL)
				validArgs = append(validArgs, txnSQL.Args...)
			}
		}

		if len(validSQLs) == 0 {
			log.Debug("txnSink: no valid SQL to execute in batch")
			return nil
		}

		// Combine SQL statements with semicolons
		finalSQL := strings.Join(validSQLs, ";")

		log.Info("executing transaction",
			zap.String("sql", finalSQL),
			zap.Any("args", validArgs))

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, execErr := e.db.ExecContext(ctx, finalSQL, validArgs...)
		cancel()

		if execErr != nil {
			log.Error("txnSink: failed to execute SQL batch",
				zap.String("sql", finalSQL),
				zap.Int("batchSize", len(batch)),
				zap.Error(execErr))
			return errors.Trace(execErr)
		}

		log.Debug("txnSink: successfully executed SQL batch",
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
