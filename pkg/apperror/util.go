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

package apperror

import (
	gmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	dmretry "github.com/pingcap/tiflow/dm/pkg/retry"
)

// IsTableNotExistsErr is used to check if the error is a table not exists error.
func IsTableNotExistsErr(err error) bool {
	err = errors.Cause(err)
	mysqlErr, ok := err.(*gmysql.MySQLError)
	if !ok {
		return false
	}

	errCode := errors.ErrCode(mysqlErr.Number)
	return errCode == infoschema.ErrTableNotExists.Code() || errCode == infoschema.ErrDatabaseNotExists.Code()
}

// IsIgnorableMySQLDDLError is used to check what error can be ignored
// we can get error code from:
// infoschema's error definition: https://github.com/pingcap/tidb/blob/master/infoschema/infoschema.go
// DDL's error definition: https://github.com/pingcap/tidb/blob/master/ddl/ddl.go
// tidb/mysql error code definition: https://github.com/pingcap/tidb/blob/master/mysql/errcode.go
func IsIgnorableMySQLDDLError(err error) bool {
	err = errors.Cause(err)
	mysqlErr, ok := err.(*gmysql.MySQLError)
	if !ok {
		return false
	}

	errCode := errors.ErrCode(mysqlErr.Number)
	switch errCode {
	case infoschema.ErrDatabaseExists.Code(), infoschema.ErrDatabaseDropExists.Code(),
		infoschema.ErrTableExists.Code(), infoschema.ErrTableDropExists.Code(),
		infoschema.ErrColumnExists.Code(), infoschema.ErrIndexExists.Code(),
		infoschema.ErrKeyNotExists.Code(), dbterror.ErrCantDropFieldOrKey.Code(),
		infoschema.ErrColumnNotExists.Code(),
		mysql.ErrDupKeyName, mysql.ErrSameNamePartition,
		mysql.ErrDropPartitionNonExistent, mysql.ErrMultiplePriKey:
		return true
	default:
		return false
	}
}

// IsRetryableDMLError check if the error is a retryable dml error.
func IsRetryableDMLError(err error) bool {
	if !cerror.IsRetryableError(err) {
		return false
	}
	// Check if the error is connection errors that can retry safely.
	if dmretry.IsConnectionError(err) {
		return true
	}
	// Check if the error is a retriable TiDB error or MySQL error.
	return dbutil.IsRetryableError(err)
}

// IsRetryableDDLError check if the error is a retryable ddl error.
func IsRetryableDDLError(err error) bool {
	if IsRetryableDMLError(err) {
		return true
	}

	// All DDLs should be idempotent in theory.
	if dmretry.IsUnretryableConnectionError(err) {
		return true
	}

	err = errors.Cause(err)
	mysqlErr, ok := err.(*gmysql.MySQLError)
	if !ok {
		return false
	}

	// If the error is in the black list, return false.
	switch mysqlErr.Number {
	case mysql.ErrAccessDenied,
		mysql.ErrDBaccessDenied,
		mysql.ErrSyntax,
		mysql.ErrParse,
		mysql.ErrNoDB,
		mysql.ErrBadDB,
		mysql.ErrNoSuchTable,
		mysql.ErrNoSuchIndex,
		mysql.ErrKeyColumnDoesNotExits,
		mysql.ErrWrongColumnName,
		mysql.ErrPartitionMgmtOnNonpartitioned,
		mysql.ErrNonuniqTable,
		errno.ErrTableWithoutPrimaryKey:
		return false
	}
	return true
}

// IsSyncPointIgnoreError returns whether the error is ignorable for syncpoint.
func IsSyncPointIgnoreError(err error) bool {
	err = errors.Cause(err)
	mysqlErr, ok := err.(*gmysql.MySQLError)
	if !ok {
		return false
	}
	// We should ignore the error when the downstream has no
	// such system variable for compatibility.
	return mysqlErr.Number == mysql.ErrUnknownSystemVariable
}
