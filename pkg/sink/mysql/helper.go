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

package mysql

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/coreos/go-semver/semver"
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/dumpling/export"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	dmutils "github.com/pingcap/tiflow/dm/pkg/conn"
	"go.uber.org/zap"
)

const checkRunningAddIndexSQL = `
SELECT JOB_ID, JOB_TYPE, SCHEMA_STATE, SCHEMA_ID, TABLE_ID, STATE, QUERY
FROM information_schema.ddl_jobs
WHERE TABLE_ID = "%d"
    AND JOB_TYPE LIKE "add index%%"
    AND (STATE = "running" OR STATE = "queueing");
`

const checkRunningSQL = `SELECT JOB_ID, JOB_TYPE, SCHEMA_STATE, SCHEMA_ID, TABLE_ID, STATE, QUERY FROM information_schema.ddl_jobs 
	WHERE CREATE_TIME >= "%s" AND QUERY = "%s";`

// CheckIfBDRModeIsSupported checks if the downstream supports BDR mode.
func CheckIfBDRModeIsSupported(ctx context.Context, db *sql.DB) (bool, error) {
	isTiDB := CheckIsTiDB(ctx, db)
	if !isTiDB {
		return false, nil
	}
	testSourceID := 1
	// downstream is TiDB, set system variables.
	// We should always try to set this variable, and ignore the error if
	// downstream does not support this variable, it is by design.
	query := fmt.Sprintf("SET SESSION %s = %d", "tidb_cdc_write_source", testSourceID)
	_, err := db.ExecContext(ctx, query)
	if err != nil {
		if mysqlErr, ok := errors.Cause(err).(*dmysql.MySQLError); ok &&
			mysqlErr.Number == mysql.ErrUnknownSystemVariable {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// CheckIsTiDB checks if the downstream is TiDB.
func CheckIsTiDB(ctx context.Context, db *sql.DB) bool {
	var tidbVer string
	// check if downstream is TiDB
	row := db.QueryRowContext(ctx, "select tidb_version()")
	err := row.Scan(&tidbVer)
	if err != nil {
		log.Warn("check tidb version error, the downstream db is not tidb?", zap.Error(err))
		// In earlier versions, this function returned an `error` along with a boolean value,
		// which allowed callers to differentiate between network-related issues and
		// the absence of TiDB. However, since the specific error content wasn't critical to
		// the logic—external callers only needed to know whether the downstream was TiDB—the
		// decision was made to simplify the function. External callers should not handle the
		// error returned here because even if the downstream is not TiDB, TiCDC should still
		// function properly, for more details: https://github.com/pingcap/tiflow/pull/11214
		//
		// So instead of returning an `error`, we now log a warning if the
		// query fails. This keeps the external interface clean while still
		// providing observability into network or query issues through logs.
		//
		// Note: The function returns `false` and logs a warning if the
		// query to retrieve the TiDB version fails.
		return false
	}
	return true
}

// GenBasicDSN generates a basic DSN from the given config.
func GenBasicDSN(cfg *Config) (*dmysql.Config, error) {
	// dsn format of the driver:
	// [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
	username := cfg.sinkURI.User.Username()
	if username == "" {
		username = "root"
	}
	password, _ := cfg.sinkURI.User.Password()

	hostName := cfg.sinkURI.Hostname()
	port := cfg.sinkURI.Port()
	if port == "" {
		port = "4000"
	}

	// This will handle the IPv6 address format.
	var dsn *dmysql.Config
	var err error
	host := net.JoinHostPort(hostName, port)
	dsnStr := fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, host, cfg.TLS)
	if dsn, err = dmysql.ParseDSN(dsnStr); err != nil {
		return nil, errors.Trace(err)
	}

	// create test db used for parameter detection
	// Refer https://github.com/go-sql-driver/mysql#parameters
	if dsn.Params == nil {
		dsn.Params = make(map[string]string, 1)
	}
	if cfg.Timezone != "" {
		dsn.Params["time_zone"] = cfg.Timezone
	}
	dsn.Params["readTimeout"] = cfg.ReadTimeout
	dsn.Params["writeTimeout"] = cfg.WriteTimeout
	dsn.Params["timeout"] = cfg.DialTimeout

	setDryRunConfig(cfg)

	return dsn, nil
}

func setDryRunConfig(cfg *Config) {
	dryRun := cfg.sinkURI.Query().Get("dry-run")
	if dryRun == "true" {
		log.Info("dry-run mode is enabled, will not write data to downstream")
		cfg.DryRun = true
	}
	if !cfg.DryRun {
		return
	}

	dryRunDelay := cfg.sinkURI.Query().Get("dry-run-delay")
	if dryRunDelay != "" {
		dryRunDelayInt, err := strconv.Atoi(dryRunDelay)
		if err != nil {
			log.Error("invalid dry-run-delay", zap.Error(err))
			return
		}
		log.Info("set dry-run-delay", zap.Duration("duration", time.Duration(dryRunDelayInt)*time.Millisecond))
		cfg.DryRunDelay = time.Duration(dryRunDelayInt) * time.Millisecond
	}

	dryRunBlockInterval := cfg.sinkURI.Query().Get("dry-run-block-interval")
	if dryRunBlockInterval != "" {
		dryRunBlockIntervalInt, err := strconv.Atoi(dryRunBlockInterval)
		if err != nil {
			log.Error("invalid dry-run-block-interval", zap.Error(err))
			return
		}
		log.Info("set dry-run-block-interval", zap.Duration("duration", time.Duration(dryRunBlockIntervalInt)*time.Second))
		cfg.DryRunBlockInterval = time.Duration(dryRunBlockIntervalInt) * time.Second
	}
}

// GetTestDB checks and adjusts the password of the given DSN,
// it will return a DB instance opened with the adjusted password.
func GetTestDB(dbConfig *dmysql.Config) (*sql.DB, error) {
	password := dbConfig.Passwd

	testDB, err := CreateMysqlDBConn(dbConfig.FormatDSN())
	if err != nil {
		// If access is denied and password is encoded by base64, try to decoded password.
		if mysqlErr, ok := errors.Cause(err).(*dmysql.MySQLError); ok && mysqlErr.Number == mysql.ErrAccessDenied {
			if dePassword, decodeErr := base64.StdEncoding.DecodeString(password); decodeErr == nil && string(dePassword) != password {
				dbConfig.Passwd = string(dePassword)
				testDB, err = CreateMysqlDBConn(dbConfig.FormatDSN())
			}
		}
	}
	return testDB, err
}

func checkTiDBVariable(db *sql.DB, variableName, defaultValue string) (string, error) {
	var name string
	var value string
	querySQL := fmt.Sprintf("show session variables like '%s';", variableName)
	err := db.QueryRowContext(context.Background(), querySQL).Scan(&name, &value)
	if err != nil && err != sql.ErrNoRows {
		errMsg := "fail to query session variable " + variableName
		return "", cerror.ErrMySQLQueryError.Wrap(err).GenWithStack(errMsg)
	}
	// session variable works, use given default value
	if err == nil {
		return defaultValue, nil
	}
	// session variable not exists, return "" to ignore it
	return "", nil
}

func generateDSNByConfig(
	dsnCfg *dmysql.Config,
	cfg *Config,
	testDB *sql.DB,
) (string, error) {
	if dsnCfg.Params == nil {
		dsnCfg.Params = make(map[string]string, 1)
	}
	dsnCfg.DBName = ""
	dsnCfg.InterpolateParams = true
	dsnCfg.MultiStatements = true
	// if timezone is empty string, we don't pass this variable in dsn
	if cfg.Timezone != "" {
		dsnCfg.Params["time_zone"] = cfg.Timezone
	}
	dsnCfg.Params["readTimeout"] = cfg.ReadTimeout
	dsnCfg.Params["writeTimeout"] = cfg.WriteTimeout
	dsnCfg.Params["timeout"] = cfg.DialTimeout
	// auto fetch max_allowed_packet on every new connection
	dsnCfg.Params["maxAllowedPacket"] = "0"

	autoRandom, err := checkTiDBVariable(testDB, "allow_auto_random_explicit_insert", "1")
	if err != nil {
		return "", err
	}
	if autoRandom != "" {
		dsnCfg.Params["allow_auto_random_explicit_insert"] = autoRandom
	}

	txnMode, err := checkTiDBVariable(testDB, "tidb_txn_mode", cfg.tidbTxnMode)
	if err != nil {
		return "", err
	}
	if txnMode != "" {
		dsnCfg.Params["tidb_txn_mode"] = txnMode
	}

	// Since we don't need select, just set default isolation level to read-committed
	// transaction_isolation is mysql newly introduced variable and will vary from MySQL5.7/MySQL8.0/Mariadb
	isolation, err := checkTiDBVariable(testDB, "transaction_isolation", defaultTxnIsolationRC)
	if err != nil {
		return "", err
	}
	if isolation != "" {
		dsnCfg.Params["transaction_isolation"] = fmt.Sprintf(`"%s"`, defaultTxnIsolationRC)
	} else {
		dsnCfg.Params["tx_isolation"] = fmt.Sprintf(`"%s"`, defaultTxnIsolationRC)
	}

	// equals to executing "SET NAMES utf8mb4"
	dsnCfg.Params["charset"] = defaultCharacterSet

	// disable foreign_key_checks
	dsnCfg.Params["foreign_key_checks"] = "0"

	tidbPlacementMode, err := checkTiDBVariable(testDB, "tidb_placement_mode", "ignore")
	if err != nil {
		return "", err
	}
	if tidbPlacementMode != "" {
		dsnCfg.Params["tidb_placement_mode"] = fmt.Sprintf(`"%s"`, tidbPlacementMode)
	}
	tidbEnableExternalTSRead, err := checkTiDBVariable(testDB, "tidb_enable_external_ts_read", "OFF")
	if err != nil {
		return "", err
	}
	if tidbEnableExternalTSRead != "" {
		// set the `tidb_enable_external_ts_read` to `OFF`, so cdc could write to the sink
		dsnCfg.Params["tidb_enable_external_ts_read"] = fmt.Sprintf(`"%s"`, tidbEnableExternalTSRead)
	}
	dsnClone := dsnCfg.Clone()
	dsnClone.Passwd = "******"
	log.Info("sink uri is configured", zap.String("dsn", dsnClone.FormatDSN()))

	return dsnCfg.FormatDSN(), nil
}

// check whether the target charset is supported
func checkCharsetSupport(db *sql.DB, charsetName string) (bool, error) {
	// validate charsetName
	_, err := charset.GetCharsetInfo(charsetName)
	if err != nil {
		return false, errors.Trace(err)
	}

	var characterSetName string
	querySQL := "select character_set_name from information_schema.character_sets " +
		"where character_set_name = '" + charsetName + "';"
	err = db.QueryRowContext(context.Background(), querySQL).Scan(&characterSetName)
	if err != nil && err != sql.ErrNoRows {
		return false, cerror.WrapError(cerror.ErrMySQLQueryError, err)
	}
	if err != nil {
		return false, nil
	}

	return true, nil
}

// return dsn
func GenerateDSN(cfg *Config) (string, error) {
	dsn, err := GenBasicDSN(cfg)
	if err != nil {
		return "", err
	}

	var testDB *sql.DB
	testDB, err = GetTestDB(dsn)
	if err != nil {
		return "", err
	}
	defer testDB.Close()

	// we use default sql mode for downstream because all dmls generated and ddls in ticdc
	// are based on default sql mode.
	dsn.Params["sql_mode"], err = dmutils.AdjustSQLModeCompatible(mysql.DefaultSQLMode)
	if err != nil {
		return "", err
	}
	// NOTE: quote the string is necessary to avoid ambiguities.
	dsn.Params["sql_mode"] = strconv.Quote(dsn.Params["sql_mode"])

	dsnStr, err := generateDSNByConfig(dsn, cfg, testDB)
	if err != nil {
		return "", err
	}

	// check if GBK charset is supported by downstream
	var gbkSupported bool
	gbkSupported, err = checkCharsetSupport(testDB, charset.CharsetGBK)
	if err != nil {
		return "", err
	}
	if !gbkSupported {
		log.Warn("GBK charset is not supported by the downstream. "+
			"Some types of DDLs may fail to execute",
			zap.String("host", dsn.Addr))
	}

	return dsnStr, nil
}

func CreateMysqlDBConn(dsnStr string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsnStr)
	if err != nil {
		return nil, cerror.ErrMySQLConnectionError.Wrap(err).GenWithStack("fail to open MySQL connection")
	}

	err = db.PingContext(context.Background())
	if err != nil {
		// close db to recycle resources
		if closeErr := db.Close(); closeErr != nil {
			log.Warn("close db failed", zap.Error(err))
		}
		return nil, cerror.ErrMySQLConnectionError.Wrap(err).GenWithStack("fail to open MySQL connection")
	}
	return db, nil
}

func needSwitchDB(event *commonEvent.DDLEvent) bool {
	if len(event.GetDDLSchemaName()) == 0 {
		return false
	}
	if event.GetDDLType() == timodel.ActionCreateSchema || event.GetDDLType() == timodel.ActionDropSchema {
		return false
	}
	return true
}

func needWaitAsyncExecDone(t timodel.ActionType) bool {
	switch t {
	case timodel.ActionCreateTable, timodel.ActionCreateTables:
		return false
	case timodel.ActionCreateSchema:
		return false
	default:
		return true
	}
}

// SetWriteSource sets write source for the transaction.
// When this variable is set to a value other than 0, data written in this session is considered to be written by TiCDC.
// DDLs executed in a PRIMARY cluster can be replicated to a SECONDARY cluster by TiCDC.
func SetWriteSource(ctx context.Context, cfg *Config, txn *sql.Tx) error {
	// we only set write source when donwstream is TiDB and write source is existed.
	if !cfg.IsWriteSourceExisted {
		return nil
	}
	// downstream is TiDB, set system variables.
	// We should always try to set this variable, and ignore the error if
	// downstream does not support this variable, it is by design.
	query := fmt.Sprintf("SET SESSION %s = %d", "tidb_cdc_write_source", cfg.SourceID)
	_, err := txn.ExecContext(ctx, query)
	if err != nil {
		if mysqlErr, ok := errors.Cause(err).(*dmysql.MySQLError); ok &&
			mysqlErr.Number == mysql.ErrUnknownSystemVariable {
			return nil
		}
		return err
	}
	return nil
}

// ShouldFormatVectorType return true if vector type should be converted to longtext.
func ShouldFormatVectorType(db *sql.DB, cfg *Config) bool {
	if !cfg.HasVectorType {
		log.Warn("please set `has-vector-type` to be true if a column is vector type when the downstream is not TiDB or TiDB version less than specify version",
			zap.Any("hasVectorType", cfg.HasVectorType), zap.Any("supportVectorVersion", defaultSupportVectorVersion))
		return false
	}
	versionInfo, err := export.SelectVersion(db)
	if err != nil {
		log.Warn("fail to get version", zap.Error(err), zap.Bool("isTiDB", cfg.IsTiDB))
		return false
	}
	serverInfo := version.ParseServerInfo(versionInfo)
	ver := semver.New(defaultSupportVectorVersion)
	if !cfg.IsTiDB || serverInfo.ServerVersion.LessThan(*ver) {
		log.Error("downstream unsupport vector type. it will be converted to longtext",
			zap.String("version", serverInfo.ServerVersion.String()), zap.String("supportVectorVersion", defaultSupportVectorVersion), zap.Bool("isTiDB", cfg.IsTiDB))
		return true
	}
	return false
}

func isRetryableDMLError(err error) bool {
	if !cerror.IsRetryableError(err) {
		return false
	}

	errCode, ok := getSQLErrCode(err)
	if !ok {
		return true
	}

	switch errCode {
	// when meet dup entry error, we don't retry and report the error directly to owner to restart the changefeed.
	case mysql.ErrNoSuchTable, mysql.ErrBadDB, mysql.ErrDupEntry:
		return false
	}
	return true
}

func getSQLErrCode(err error) (errors.ErrCode, bool) {
	mysqlErr, ok := errors.Cause(err).(*dmysql.MySQLError)
	if !ok {
		return -1, false
	}

	return errors.ErrCode(mysqlErr.Number), true
}

func getDDLCreateTime(ctx context.Context, db *sql.DB) string {
	ddlCreateTime := "" // default when scan failed
	row, err := db.QueryContext(ctx, "BEGIN; SET @ticdc_ts := TIDB_PARSE_TSO(@@tidb_current_ts); ROLLBACK; SELECT @ticdc_ts; SET @ticdc_ts=NULL;")
	if err != nil {
		log.Warn("selecting tidb current timestamp failed", zap.Error(err))
	} else {
		for row.Next() {
			err = row.Scan(&ddlCreateTime)
			if err != nil {
				log.Warn("getting ddlCreateTime failed", zap.Error(err))
			}
		}
		//nolint:sqlclosecheck
		_ = row.Close()
		_ = row.Err()
	}
	return ddlCreateTime
}

// getDDLStateFromTiDB retrieves the ddl job status of the ddl query from downstream tidb based on the ddl query and the approximate ddl create time.
func getDDLStateFromTiDB(ctx context.Context, db *sql.DB, ddl string, createTime string) (timodel.JobState, error) {
	// ddlCreateTime and createTime are both based on UTC timezone of downstream
	showJobs := fmt.Sprintf(checkRunningSQL, createTime, ddl)
	//nolint:rowserrcheck
	jobsRows, err := db.QueryContext(ctx, showJobs)
	if err != nil {
		return timodel.JobStateNone, err
	}

	var jobsResults [][]string
	jobsResults, err = export.GetSpecifiedColumnValuesAndClose(jobsRows, "QUERY", "STATE", "JOB_ID", "JOB_TYPE", "SCHEMA_STATE")
	if err != nil {
		return timodel.JobStateNone, err
	}
	if len(jobsResults) > 0 {
		result := jobsResults[0]
		state, jobID, jobType, schemaState := result[1], result[2], result[3], result[4]
		log.Debug("Find ddl state in downstream",
			zap.String("jobID", jobID),
			zap.String("jobType", jobType),
			zap.String("schemaState", schemaState),
			zap.String("ddl", ddl),
			zap.String("state", state),
			zap.Any("jobsResults", jobsResults),
		)
		return timodel.StrToJobState(result[1]), nil
	}
	return timodel.JobStateNone, nil
}
