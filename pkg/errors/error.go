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

package errors

import (
	"fmt"

	"github.com/pingcap/errors"
)

// errors
var (
	// kv related errors
	ErrChangeFeedNotExists = errors.Normalize(
		"changefeed not exists, %s",
		errors.RFCCodeText("CDC:ErrChangeFeedNotExists"),
	)
	ErrChangeFeedAlreadyExists = errors.Normalize(
		"changefeed already exists, %s",
		errors.RFCCodeText("CDC:ErrChangeFeedAlreadyExists"),
	)
	ErrEtcdAPIError = errors.Normalize(
		"etcd api returns error",
		errors.RFCCodeText("CDC:ErrEtcdAPIError"),
	)
	ErrChangeFeedDeletionUnfinished = errors.Normalize(
		"changefeed exists after deletion, %s",
		errors.RFCCodeText("CDC:ErrChangeFeedDeletionUnfinished"),
	)
	ErrCaptureNotExist = errors.Normalize(
		"capture not exists, %s",
		errors.RFCCodeText("CDC:ErrCaptureNotExist"),
	)
	ErrSchedulerRequestFailed = errors.Normalize(
		"scheduler request failed, %s",
		errors.RFCCodeText("CDC:ErrSchedulerRequestFailed"),
	)
	ErrGetAllStoresFailed = errors.Normalize(
		"get stores from pd failed",
		errors.RFCCodeText("CDC:ErrGetAllStoresFailed"),
	)
	ErrMetaListDatabases = errors.Normalize(
		"meta store list databases",
		errors.RFCCodeText("CDC:ErrMetaListDatabases"),
	)
	ErrDDLSchemaNotFound = errors.Normalize(
		"cannot find schema: %s",
		errors.RFCCodeText("CDC:ErrDDLSchemaNotFound"),
	)
	ErrPDEtcdAPIError = errors.Normalize(
		"etcd api call error",
		errors.RFCCodeText("CDC:ErrPDEtcdAPIError"),
	)
	ErrNewStore = errors.Normalize(
		"new store failed",
		errors.RFCCodeText("CDC:ErrNewStore"),
	)

	// codec related errors
	ErrDDLUnsupportType = errors.Normalize(
		"unsupport ddl type %s, query %s",
		errors.RFCCodeText("CDC:ErrDDLUnsupportType"),
	)
	ErrEncodeFailed = errors.Normalize(
		"encode failed",
		errors.RFCCodeText("CDC:ErrEncodeFailed"),
	)
	ErrDecodeFailed = errors.Normalize(
		"decode failed: %s",
		errors.RFCCodeText("CDC:ErrDecodeFailed"),
	)
	ErrFilterRuleInvalid = errors.Normalize(
		"filter rule is invalid %v",
		errors.RFCCodeText("CDC:ErrFilterRuleInvalid"),
	)

	ErrDispatcherFailed = errors.Normalize(
		"dispatcher failed",
		errors.RFCCodeText("CDC:ErrDispatcherFailed"),
	)

	ErrColumnSelectorFailed = errors.Normalize(
		"column selector failed",
		errors.RFCCodeText("CDC:ErrColumnSelectorFailed"),
	)

	// Errors caused by unexpected behavior from external systems
	ErrTiDBUnexpectedJobMeta = errors.Normalize(
		"unexpected `job_meta` from tidb",
		errors.RFCCodeText("CDC:ErrTiDBUnexpectedJobMeta"),
	)

	// ErrVersionIncompatible is an error for running CDC on an incompatible Cluster.
	ErrVersionIncompatible = errors.Normalize(
		"version is incompatible: %s",
		errors.RFCCodeText("CDC:ErrVersionIncompatible"),
	)
	ErrClusterIDMismatch = errors.Normalize(
		"cluster ID mismatch, tikv cluster ID is %d and request cluster ID is %d",
		errors.RFCCodeText("CDC:ErrClusterIDMismatch"),
	)
	ErrMultipleCDCClustersExist = errors.Normalize(
		"multiple TiCDC clusters exist while using --pd",
		errors.RFCCodeText("CDC:ErrMultipleCDCClustersExist"),
	)

	ErrKafkaSendMessage = errors.Normalize(
		"kafka send message failed",
		errors.RFCCodeText("CDC:ErrKafkaSendMessage"),
	)
	ErrKafkaProducerClosed = errors.Normalize(
		"kafka producer closed",
		errors.RFCCodeText("CDC:ErrKafkaProducerClosed"),
	)
	ErrKafkaAsyncSendMessage = errors.Normalize(
		"kafka async send message failed",
		errors.RFCCodeText("CDC:ErrKafkaAsyncSendMessage"),
	)
	ErrKafkaInvalidPartitionNum = errors.Normalize(
		"invalid partition num %d",
		errors.RFCCodeText("CDC:ErrKafkaInvalidPartitionNum"),
	)
	ErrKafkaInvalidRequiredAcks = errors.Normalize(
		"invalid required acks %d, "+
			"only support these values: 0(NoResponse),1(WaitForLocal) and -1(WaitForAll)",
		errors.RFCCodeText("CDC:ErrKafkaInvalidRequiredAcks"),
	)
	ErrKafkaNewProducer = errors.Normalize(
		"new kafka producer",
		errors.RFCCodeText("CDC:ErrKafkaNewProducer"),
	)
	ErrKafkaInvalidClientID = errors.Normalize(
		"invalid kafka client ID '%s'",
		errors.RFCCodeText("CDC:ErrKafkaInvalidClientID"),
	)
	ErrKafkaInvalidVersion = errors.Normalize(
		"invalid kafka version",
		errors.RFCCodeText("CDC:ErrKafkaInvalidVersion"),
	)
	ErrKafkaInvalidConfig = errors.Normalize(
		"kafka config invalid",
		errors.RFCCodeText("CDC:ErrKafkaInvalidConfig"),
	)
	ErrKafkaCreateTopic = errors.Normalize(
		"kafka create topic failed",
		errors.RFCCodeText("CDC:ErrKafkaCreateTopic"),
	)
	ErrKafkaInvalidTopicExpression = errors.Normalize(
		"invalid topic expression: %s ",
		errors.RFCCodeText("CDC:ErrKafkaTopicExprInvalid"),
	)
	ErrKafkaConfigNotFound = errors.Normalize(
		"kafka config item not found",
		errors.RFCCodeText("CDC:ErrKafkaConfigNotFound"),
	)
	ErrPulsarInvalidTopicExpression = errors.Normalize(
		"invalid topic expression",
		errors.RFCCodeText("CDC:ErrPulsarTopicExprInvalid"),
	)
	ErrCodecInvalidConfig = errors.Normalize(
		"Codec invalid config",
		errors.RFCCodeText("CDC:ErrCodecInvalidConfig"),
	)
	ErrCompressionFailed = errors.Normalize(
		"Compression failed",
		errors.RFCCodeText("CDC:ErrCompressionFailed"),
	)
	ErrSinkURIInvalid = errors.Normalize(
		"sink uri invalid '%s'",
		errors.RFCCodeText("CDC:ErrSinkURIInvalid"),
	)
	ErrIncompatibleSinkConfig = errors.Normalize(
		"incompatible configuration in sink uri(%s) and config file(%s), "+
			"please try to update the configuration only through sink uri",
		errors.RFCCodeText("CDC:ErrIncompatibleSinkConfig"),
	)
	ErrSinkUnknownProtocol = errors.Normalize(
		"unknown '%s' message protocol for sink",
		errors.RFCCodeText("CDC:ErrSinkUnknownProtocol"),
	)
	ErrExecDDLFailed = errors.Normalize(
		"exec DDL failed %s",
		errors.RFCCodeText("CDC:ErrExecDDLFailed"),
	)
	ErrDDLStateNotFound = errors.Normalize(
		"DDL state not found %s",
		errors.RFCCodeText("CDC:ErrDDLStateNotFound"),
	)
	ErrMySQLTxnError = errors.Normalize(
		"MySQL txn error",
		errors.RFCCodeText("CDC:ErrMySQLTxnError"),
	)
	ErrMySQLDuplicateEntry = errors.Normalize(
		"MySQL duplicate entry error",
		errors.RFCCodeText("CDC:ErrMySQLDuplicateEntry"),
	)
	ErrMySQLQueryError = errors.Normalize(
		"MySQL query error",
		errors.RFCCodeText("CDC:ErrMySQLQueryError"),
	)
	ErrMySQLConnectionError = errors.Normalize(
		"MySQL connection error",
		errors.RFCCodeText("CDC:ErrMySQLConnectionError"),
	)
	ErrMySQLInvalidConfig = errors.Normalize(
		"MySQL config invalid",
		errors.RFCCodeText("CDC:ErrMySQLInvalidConfig"),
	)
	ErrAvroToEnvelopeError = errors.Normalize(
		"to envelope failed",
		errors.RFCCodeText("CDC:ErrAvroToEnvelopeError"),
	)
	ErrAvroMarshalFailed = errors.Normalize(
		"json marshal failed",
		errors.RFCCodeText("CDC:ErrAvroMarshalFailed"),
	)
	ErrAvroEncodeFailed = errors.Normalize(
		"encode to avro native data",
		errors.RFCCodeText("CDC:ErrAvroEncodeFailed"),
	)
	ErrAvroEncodeToBinary = errors.Normalize(
		"encode to binray from native",
		errors.RFCCodeText("CDC:ErrAvroEncodeToBinary"),
	)
	ErrAvroSchemaAPIError = errors.Normalize(
		"schema manager API error, %s",
		errors.RFCCodeText("CDC:ErrAvroSchemaAPIError"),
	)
	ErrAvroInvalidMessage = errors.Normalize(
		"avro invalid message format, %s",
		errors.RFCCodeText("CDC:ErrAvroInvalidMessage"),
	)
	ErrOpenProtocolCodecInvalidData = errors.Normalize(
		"open-protocol codec invalid data",
		errors.RFCCodeText("CDC:ErrOpenProtocolCodecInvalidData"),
	)
	ErrCanalEncodeFailed = errors.Normalize(
		"canal encode failed",
		errors.RFCCodeText("CDC:ErrCanalEncodeFailed"),
	)
	ErrSinkInvalidConfig = errors.Normalize(
		"sink config invalid",
		errors.RFCCodeText("CDC:ErrSinkInvalidConfig"),
	)
	ErrMessageTooLarge = errors.Normalize(
		"message is too large. table:%s, length:%d, maxMessageBytes:%d",
		errors.RFCCodeText("CDC:ErrMessageTooLarge"),
	)
	ErrStorageSinkInvalidDateSeparator = errors.Normalize(
		"date separator in storage sink is invalid",
		errors.RFCCodeText("CDC:ErrStorageSinkInvalidDateSeparator"),
	)
	ErrCSVEncodeFailed = errors.Normalize(
		"csv encode failed",
		errors.RFCCodeText("CDC:ErrCSVEncodeFailed"),
	)
	ErrCSVDecodeFailed = errors.Normalize(
		"csv decode failed",
		errors.RFCCodeText("CDC:ErrCSVDecodeFailed"),
	)
	ErrDebeziumEncodeFailed = errors.Normalize(
		"debezium encode failed",
		errors.RFCCodeText("CDC:ErrDebeziumEncodeFailed"),
	)
	ErrDebeziumInvalidMessage = errors.Normalize(
		"debezium invalid message format, %s",
		errors.RFCCodeText("CDC:ErrDebeziumInvalidMessage"),
	)
	ErrDebeziumEmptyValueMessage = errors.Normalize(
		"debezium value should not be empty",
		errors.RFCCodeText("CDC:ErrDebeziumEmptyValueMessage"),
	)
	ErrStorageSinkInvalidConfig = errors.Normalize(
		"storage sink config invalid",
		errors.RFCCodeText("CDC:ErrStorageSinkInvalidConfig"),
	)
	ErrStorageSinkInvalidFileName = errors.Normalize(
		"filename in storage sink is invalid",
		errors.RFCCodeText("CDC:ErrStorageSinkInvalidFileName"),
	)

	// utilities related errors
	ErrToTLSConfigFailed = errors.Normalize(
		"generate tls config failed",
		errors.RFCCodeText("CDC:ErrToTLSConfigFailed"),
	)
	ErrCheckClusterVersionFromPD = errors.Normalize(
		"failed to request PD %s, please try again later",
		errors.RFCCodeText("CDC:ErrCheckClusterVersionFromPD"),
	)
	ErrNewSemVersion = errors.Normalize(
		"create sem version",
		errors.RFCCodeText("CDC:ErrNewSemVersion"),
	)
	ErrCheckDirWritable = errors.Normalize(
		"check dir writable failed",
		errors.RFCCodeText("CDC:ErrCheckDirWritable"),
	)
	ErrCheckDirValid = errors.Normalize(
		"check dir valid failed",
		errors.RFCCodeText("CDC:ErrCheckDirValid"),
	)
	ErrURLFormatInvalid = errors.Normalize(
		"url format is invalid",
		errors.RFCCodeText("CDC:ErrURLFormatInvalid"),
	)
	ErrIntersectNoOverlap = errors.Normalize(
		"span doesn't overlap: %+v vs %+v",
		errors.RFCCodeText("CDC:ErrIntersectNoOverlap"),
	)
	ErrOperateOnClosedNotifier = errors.Normalize(
		"operate on a closed notifier",
		errors.RFCCodeText("CDC:ErrOperateOnClosedNotifier"),
	)
	ErrDiskFull = errors.Normalize(
		"failed to preallocate file because disk is full",
		errors.RFCCodeText("CDC:ErrDiskFull"))
	ErrWaitFreeMemoryTimeout = errors.Normalize(
		"wait free memory timeout",
		errors.RFCCodeText("CDC:ErrWaitFreeMemoryTimeout"),
	)

	// encode/decode, data format and data integrity errors
	ErrInvalidRecordKey = errors.Normalize(
		"invalid record key - %q",
		errors.RFCCodeText("CDC:ErrInvalidRecordKey"),
	)
	ErrCodecDecode = errors.Normalize(
		"codec decode error",
		errors.RFCCodeText("CDC:ErrCodecDecode"),
	)
	ErrDatumUnflatten = errors.Normalize(
		"unflatten datume data",
		errors.RFCCodeText("CDC:ErrDatumUnflatten"),
	)
	ErrDecodeRowToDatum = errors.Normalize(
		"decode row data to datum failed",
		errors.RFCCodeText("CDC:ErrDecodeRowToDatum"),
	)
	ErrMarshalFailed = errors.Normalize(
		"marshal failed",
		errors.RFCCodeText("CDC:ErrMarshalFailed"),
	)
	ErrUnmarshalFailed = errors.Normalize(
		"unmarshal failed",
		errors.RFCCodeText("CDC:ErrUnmarshalFailed"),
	)
	ErrInvalidChangefeedID = errors.Normalize(
		`bad changefeed id, please match the pattern "^[a-zA-Z0-9]+(\-[a-zA-Z0-9]+)*$", the length should no more than %d, eg, "simple-changefeed-task"`,
		errors.RFCCodeText("CDC:ErrInvalidChangefeedID"),
	)
	ErrInvalidKeyspace = errors.Normalize(
		`bad keyspace, please match the pattern "^[a-zA-Z0-9]+(\-[a-zA-Z0-9]+)*$", the length should no more than %d, eg, "simple-keyspace-test"`,
		errors.RFCCodeText("CDC:ErrInvalidKeyspace"),
	)
	ErrKeyspaceNotFound = errors.Normalize(
		"keyspace not found: %d",
		errors.RFCCodeText("CDC:ErrKeyspaceNotFound"),
	)
	ErrKeyspaceIDInvalid = errors.Normalize(
		"keyspace id is invalid",
		errors.RFCCodeText("CDC:ErrKeyspaceIDInvalid"),
	)
	ErrInvalidEtcdKey = errors.Normalize(
		"invalid key: %s",
		errors.RFCCodeText("CDC:ErrInvalidEtcdKey"),
	)

	ErrSchemaSnapshotNotFound = errors.Normalize(
		"can not found schema snapshot, ts: %d",
		errors.RFCCodeText("CDC:ErrSchemaSnapshotNotFound"),
	)
	ErrSchemaStorageTableMiss = errors.Normalize(
		"table %d not found",
		errors.RFCCodeText("CDC:ErrSchemaStorageTableMiss"),
	)
	ErrSnapshotSchemaNotFound = errors.Normalize(
		"schema %d not found in schema snapshot",
		errors.RFCCodeText("CDC:ErrSnapshotSchemaNotFound"),
	)
	ErrSnapshotTableNotFound = errors.Normalize(
		"table %d not found in schema snapshot",
		errors.RFCCodeText("CDC:ErrSnapshotTableNotFound"),
	)
	ErrSnapshotSchemaExists = errors.Normalize(
		"schema %s(%d) already exists",
		errors.RFCCodeText("CDC:ErrSnapshotSchemaExists"),
	)
	ErrSnapshotTableExists = errors.Normalize(
		"table %s.%s already exists",
		errors.RFCCodeText("CDC:ErrSnapshotTableExists"),
	)
	ErrInvalidDDLJob = errors.Normalize(
		"invalid ddl job(%d)",
		errors.RFCCodeText("CDC:ErrInvalidDDLJob"),
	)
	ErrExchangePartition = errors.Normalize(
		"exchange partition failed, %s",
		errors.RFCCodeText("CDC:ErrExchangePartition"),
	)

	ErrCorruptedDataMutation = errors.Normalize(
		"Changefeed %s.%s stopped due to corrupted data mutation received",
		errors.RFCCodeText("CDC:ErrCorruptedDataMutation"))

	// server related errors
	ErrCaptureSuicide = errors.Normalize(
		"capture suicide",
		errors.RFCCodeText("CDC:ErrCaptureSuicide"),
	)
	ErrCaptureRegister = errors.Normalize(
		"capture register to etcd failed",
		errors.RFCCodeText("CDC:ErrCaptureRegister"),
	)
	ErrCaptureNotInitialized = errors.Normalize(
		"capture has not been initialized yet",
		errors.RFCCodeText("CDC:ErrCaptureNotInitialized"),
	)
	ErrOwnerUnknown = errors.Normalize(
		"owner running unknown error",
		errors.RFCCodeText("CDC:ErrOwnerUnknown"),
	)
	ErrInvalidServerOption = errors.Normalize(
		"invalid server option",
		errors.RFCCodeText("CDC:ErrInvalidServerOption"),
	)
	ErrServeHTTP = errors.Normalize(
		"serve http error",
		errors.RFCCodeText("CDC:ErrServeHTTP"),
	)
	ErrCaptureResignOwner = errors.Normalize(
		"resign owner failed",
		errors.RFCCodeText("CDC:ErrCaptureResignOwner"),
	)
	ErrClusterIsUnhealthy = errors.Normalize(
		"TiCDC cluster is unhealthy",
		errors.RFCCodeText("CDC:ErrClusterIsUnhealthy"),
	)
	ErrAPIInvalidParam = errors.Normalize(
		"invalid api parameter",
		errors.RFCCodeText("CDC:ErrAPIInvalidParam"),
	)
	ErrAPIGetPDClientFailed = errors.Normalize(
		"failed to get PDClient to connect PD, please recheck",
		errors.RFCCodeText("CDC:ErrAPIGetPDClientFailed"),
	)
	ErrInternalServerError = errors.Normalize(
		"internal server error",
		errors.RFCCodeText("CDC:ErrInternalServerError"),
	)
	ErrChangefeedUpdateRefused = errors.Normalize(
		"changefeed update error: %s",
		errors.RFCCodeText("CDC:ErrChangefeedUpdateRefused"),
	)
	ErrStartTsBeforeGC = errors.Normalize(
		"fail to create or maintain changefeed because start-ts %d "+
			"is earlier than or equal to GC safepoint at %d",
		errors.RFCCodeText("CDC:ErrStartTsBeforeGC"),
	)
	ErrTargetTsBeforeStartTs = errors.Normalize(
		"fail to create changefeed because target-ts %d is earlier than start-ts %d",
		errors.RFCCodeText("CDC:ErrTargetTsBeforeStartTs"),
	)
	ErrSnapshotLostByGC = errors.Normalize(
		"fail to create or maintain changefeed due to snapshot loss"+
			" caused by GC. checkpoint-ts %d is earlier than or equal to GC safepoint at %d",
		errors.RFCCodeText("CDC:ErrSnapshotLostByGC"),
	)
	ErrGCTTLExceeded = errors.Normalize(
		"the checkpoint-ts(%d) lag of the changefeed(%s) has exceeded "+
			"the GC TTL and the changefeed is blocking global GC progression",
		errors.RFCCodeText("CDC:ErrGCTTLExceeded"),
	)
	ErrNotOwner = errors.Normalize(
		"this capture is not a owner",
		errors.RFCCodeText("CDC:ErrNotOwner"),
	)
	ErrOwnerNotFound = errors.Normalize(
		"owner not found",
		errors.RFCCodeText("CDC:ErrOwnerNotFound"),
	)
	ErrTableIneligible = errors.Normalize(
		"some tables are not eligible to replicate(%v), "+
			"if you want to ignore these tables, please set ignore_ineligible_table to true",
		errors.RFCCodeText("CDC:ErrTableIneligible"),
	)
	// EtcdWorker related errors. Internal use only.
	// ErrEtcdTryAgain is used by a PatchFunc to force a transaction abort.
	ErrEtcdTryAgain = errors.Normalize(
		"the etcd txn should be aborted and retried immediately",
		errors.RFCCodeText("CDC:ErrEtcdTryAgain"),
	)
	// ErrEtcdIgnore is used by a PatchFunc to signal that the reactor no longer wishes to update Etcd.
	ErrEtcdIgnore = errors.Normalize(
		"this patch should be excluded from the current etcd txn",
		errors.RFCCodeText("CDC:ErrEtcdIgnore"),
	)
	// ErrEtcdSessionDone is used by etcd worker to signal a session done
	ErrEtcdSessionDone = errors.Normalize(
		"the etcd session is done",
		errors.RFCCodeText("CDC:ErrEtcdSessionDone"),
	)
	// ErrReactorFinished is used by reactor to signal a **normal** exit.
	ErrReactorFinished = errors.Normalize(
		"the reactor has done its job and should no longer be executed",
		errors.RFCCodeText("CDC:ErrReactorFinished"),
	)
	ErrLeaseExpired = errors.Normalize(
		"owner lease expired ",
		errors.RFCCodeText("CDC:ErrLeaseExpired"),
	)
	ErrEtcdTxnSizeExceed = errors.Normalize(
		"patch size:%d of a single changefeed exceed etcd txn max size:%d",
		errors.RFCCodeText("CDC:ErrEtcdTxnSizeExceed"),
	)
	ErrEtcdTxnOpsExceed = errors.Normalize(
		"patch ops:%d of a single changefeed exceed etcd txn max ops:%d",
		errors.RFCCodeText("CDC:ErrEtcdTxnOpsExceed"),
	)
	ErrEtcdMigrateFailed = errors.Normalize(
		"etcd meta data migrate failed:%s",
		errors.RFCCodeText("CDC:ErrEtcdMigrateFailed"),
	)
	ErrChangefeedUnretryable = errors.Normalize(
		"changefeed is in unretryable state, please check the error message"+
			", and you should manually handle it",
		errors.RFCCodeText("CDC:ErrChangefeedUnretryable"),
	)

	// workerpool errors
	ErrWorkerPoolHandleCancelled = errors.Normalize(
		"workerpool handle is cancelled",
		errors.RFCCodeText("CDC:ErrWorkerPoolHandleCancelled"),
	)
	ErrAsyncPoolExited = errors.Normalize(
		"asyncPool has exited. Report a bug if seen externally.",
		errors.RFCCodeText("CDC:ErrAsyncPoolExited"),
	)
	ErrWorkerPoolGracefulUnregisterTimedOut = errors.Normalize(
		"workerpool handle graceful unregister timed out",
		errors.RFCCodeText("CDC:ErrWorkerPoolGracefulUnregisterTimedOut"),
	)

	// sorter errors
	ErrIllegalSorterParameter = errors.Normalize(
		"illegal parameter for sorter: %s",
		errors.RFCCodeText("CDC:ErrIllegalSorterParameter"),
	)
	ErrConflictingFileLocks = errors.Normalize(
		"file lock conflict: %s",
		errors.RFCCodeText("ErrConflictingFileLocks"),
	)
	// RESTful client error
	ErrRewindRequestBodyError = errors.Normalize(
		"failed to seek to the beginning of request body",
		errors.RFCCodeText("CDC:ErrRewindRequestBodyError"),
	)
	ErrZeroLengthResponseBody = errors.Normalize(
		"0-length response with status code: %d",
		errors.RFCCodeText("CDC:ErrZeroLengthResponseBody"),
	)
	ErrInvalidHost = errors.Normalize(
		"host must be a URL or a host:port pair: %q",
		errors.RFCCodeText("CDC:ErrInvalidHost"),
	)

	// Upstream error
	ErrUpstreamNotFound = errors.Normalize(
		"upstream not found, cluster-id: %d",
		errors.RFCCodeText("CDC:ErrUpstreamNotFound"),
	)
	ErrUpdateServiceSafepointFailed = errors.Normalize(
		"updating service safepoint failed",
		errors.RFCCodeText("CDC:ErrUpdateServiceSafepointFailed"),
	)
	ErrUpdateGCBarrierFailed = errors.Normalize(
		"updating gc barrier failed",
		errors.RFCCodeText("CDC:ErrUpdateGCBarrierFailed"),
	)
	ErrGetGCBarrierFailed = errors.Normalize(
		"get gc barrier failed",
		errors.RFCCodeText("CDC:ErrGetGCBarrierFailed"),
	)
	ErrLoadKeyspaceFailed = errors.Normalize(
		"loading keyspace failed",
		errors.RFCCodeText("CDC:ErrLoadKeyspaceFailed"),
	)
	ErrUpstreamMissMatch = errors.Normalize(
		"upstream missmatch,old: %d, new %d",
		errors.RFCCodeText("CDC:ErrUpstreamMissMatch"),
	)

	// cli error
	ErrCliInvalidCheckpointTs = errors.Normalize(
		"invalid overwrite-checkpoint-ts %s, "+
			"overwrite-checkpoint-ts only accept 'now' or a valid timestamp in integer",
		errors.RFCCodeText("CDC:ErrCliInvalidCheckpointTs"),
	)
	ErrCliCheckpointTsIsInFuture = errors.Normalize(
		"the overwrite-checkpoint-ts %d must be smaller than current TSO",
		errors.RFCCodeText("CDC:ErrCliCheckpointTsIsInFuture"),
	)
	ErrCliAborted = errors.Normalize(
		"command '%s' is aborted by user",
		errors.RFCCodeText("CDC:ErrCliAborted"),
	)
	// Filter error
	ErrFailedToFilterDML = errors.Normalize(
		"failed to filter dml event: %v, please report a bug",
		errors.RFCCodeText("CDC:ErrFailedToFilterDML"),
	)
	ErrExpressionParseFailed = errors.Normalize(
		"invalid filter expressions. There is a syntax error in: '%s'",
		errors.RFCCodeText("CDC:ErrInvalidFilterExpression"),
	)
	ErrExpressionColumnNotFound = errors.Normalize(
		"invalid filter expression(s). Cannot find column '%s' from table '%s' in: %s",
		errors.RFCCodeText("CDC:ErrExpressionColumnNotFound"),
	)
	ErrInvalidIgnoreEventType = errors.Normalize(
		"invalid ignore event type: '%s'",
		errors.RFCCodeText("CDC:ErrInvalidIgnoreEventType"),
	)
	ErrInvalidEventType = errors.Normalize(
		"sink doesn't support this type of block event: '%s'",
		errors.RFCCodeText("CDC:ErrInvalidEventType"),
	)
	ErrSyncRenameTableFailed = errors.Normalize(
		"table's old name is not in filter rule, and its new name in filter rule "+
			"table id '%d', ddl query: [%s], it's an unexpected behavior, "+
			"if you want to replicate this table, please add its old name to filter rule.",
		errors.RFCCodeText("CDC:ErrSyncRenameTableFailed"),
	)

	// changefeed config error
	ErrInvalidReplicaConfig = errors.Normalize(
		"invalid replica config, %s",
		errors.RFCCodeText("CDC:ErrInvalidReplicaConfig"),
	)
	ErrInternalCheckFailed = errors.Normalize(
		"internal check failed, %s",
		errors.RFCCodeText("CDC:ErrInternalCheckFailed"),
	)

	ErrInvalidGlueSchemaRegistryConfig = errors.Normalize(
		"invalid glue schema registry config, %s",
		errors.RFCCodeText("CDC:ErrInvalidGlueSchemaRegistryConfig"),
	)

	ErrMetaOpFailed = errors.Normalize(
		"unexpected meta operation failure: %s",
		errors.RFCCodeText("CDC:ErrMetaOpFailed"),
	)

	ErrUnexpected = errors.Normalize(
		"cdc met unexpected error: %s",
		errors.RFCCodeText("CDC:ErrUnexpected"),
	)
	ErrGetDiskInfo = errors.Normalize(
		"get dir disk info failed",
		errors.RFCCodeText("CDC:ErrGetDiskInfo"),
	)
	ErrLoadTimezone = errors.Normalize(
		"load timezone",
		errors.RFCCodeText("CDC:ErrLoadTimezone"),
	)
	// credential related errors
	ErrCredentialNotFound = errors.Normalize(
		"credential not found: %s",
		errors.RFCCodeText("CDC:ErrCredentialNotFound"),
	)
	ErrUnauthorized = errors.Normalize(
		"user %s unauthorized, error: %s",
		errors.RFCCodeText("CDC:ErrUnauthorized"),
	)

	ErrExternalStorageAPI = errors.Normalize(
		"external storage api",
		errors.RFCCodeText("CDC:ErrS3StorageAPI"),
	)
	ErrConsistentStorage = errors.Normalize(
		"consistent storage (%s) not support",
		errors.RFCCodeText("CDC:ErrConsistentStorage"),
	)
	ErrStorageInitialize = errors.Normalize(
		"fail to open storage for redo log",
		errors.RFCCodeText("CDC:ErrStorageInitialize"),
	)

	ErrRedoConfigInvalid = errors.Normalize(
		"redo log config invalid",
		errors.RFCCodeText("CDC:ErrRedoConfigInvalid"),
	)
	ErrRedoDownloadFailed = errors.Normalize(
		"redo log down load to local failed",
		errors.RFCCodeText("CDC:ErrRedoDownloadFailed"),
	)
	ErrRedoWriterStopped = errors.Normalize(
		"redo log writer stopped",
		errors.RFCCodeText("CDC:ErrRedoWriterStopped"),
	)
	ErrRedoFileOp = errors.Normalize(
		"redo file operation",
		errors.RFCCodeText("CDC:ErrRedoFileOp"),
	)
	ErrRedoFileSizeExceed = errors.Normalize(
		"redo file size %d exceeds maximum %d",
		errors.RFCCodeText("CDC:ErrRedoFileSizeExceed"),
	)
	ErrRedoMetaFileNotFound = errors.Normalize(
		"no redo meta file found in dir: %s",
		errors.RFCCodeText("CDC:ErrRedoMetaFileNotFound"),
	)
	ErrRedoMetaInitialize = errors.Normalize(
		"initialize meta for redo log",
		errors.RFCCodeText("CDC:ErrRedoMetaInitialize"),
	)
	ErrPulsarInvalidConfig = errors.Normalize(
		"pulsar config invalid %s",
		errors.RFCCodeText("CDC:ErrPulsarInvalidConfig"),
	)
	ErrPulsarNewProducer = errors.Normalize(
		"new pulsar producer",
		errors.RFCCodeText("CDC:ErrPulsarNewProducer"),
	)
	ErrPulsarProducerClosed = errors.Normalize(
		"pulsar producer closed",
		errors.RFCCodeText("CDC:ErrPulsarProducerClosed"),
	)
	ErrPulsarAsyncSendMessage = errors.Normalize(
		"pulsar async send message failed",
		errors.RFCCodeText("CDC:ErrPulsarAsyncSendMessage"),
	)
	ErrFailToCreateExternalStorage = errors.Normalize(
		"failed to create external storage",
		errors.RFCCodeText("CDC:ErrFailToCreateExternalStorage"),
	)
	// retry error
	ErrReachMaxTry = errors.Normalize("reach maximum try: %s, error: %s",
		errors.RFCCodeText("CDC:ErrReachMaxTry"),
	)
	// tcp server error
	ErrTCPServerClosed = errors.Normalize("The TCP server has been closed",
		errors.RFCCodeText("CDC:ErrTCPServerClosed"),
	)

	// puller related errors
	ErrAddRegionRequestRetryLimitExceeded = errors.Normalize(
		"add region request retry limit exceeded",
		errors.RFCCodeText("CDC:ErrAddRegionRequestRetryLimitExceeded"),
	)

	// Application specific errors from apperror package
	ErrChangefeedRetryable = errors.Normalize(
		"changefeed is in retryable state",
		errors.RFCCodeText("CDC:ErrChangefeedRetryable"),
	)
	ErrChangefeedInitTableTriggerEventDispatcherFailed = errors.Normalize(
		"failed to init table trigger event dispatcher",
		errors.RFCCodeText("CDC:ErrChangefeedInitTableTriggerEventDispatcherFailed"),
	)
	ErrDDLEventError = errors.Normalize(
		"ddl event meets error",
		errors.RFCCodeText("CDC:ErrDDLEventError"),
	)
	ErrTableIsNotFounded = errors.Normalize(
		"table is not found",
		errors.RFCCodeText("CDC:ErrTableIsNotFounded"),
	)
	ErrTableNotSupportMove = errors.Normalize(
		"table is not supported to move",
		errors.RFCCodeText("CDC:ErrTableNotSupportMove"),
	)
	ErrMaintainerNotFounded = errors.Normalize(
		"maintainer is not found",
		errors.RFCCodeText("CDC:ErrMaintainerNotFounded"),
	)
	ErrTimeout = errors.Normalize(
		"timeout",
		errors.RFCCodeText("CDC:ErrTimeout"),
	)
	ErrNodeIsNotFound = errors.Normalize(
		"node is not found",
		errors.RFCCodeText("CDC:ErrNodeIsNotFound"),
	)
	ErrOperatorIsNil = errors.Normalize(
		"operator created failed",
		errors.RFCCodeText("CDC:ErrOperatorIsNil"),
	)

	ErrTableAfterDDLNotSplitable = errors.Normalize(
		"the ddl event will break splitable of this table",
		errors.RFCCodeText("CDC:ErrTableAfterNotSplitable"),
	)

	ErrConfigInvalidTimezone = errors.Normalize(
		"invalid timezone string: %s",
		errors.RFCCodeText("CDC:ErrConfigInvalidTimezone"),
	)

	ErrUnimplementedIOType = errors.Normalize(
		"unimplemented IOType: %d",
		errors.RFCCodeText("CDC:ErrUnimplementedIOType"),
	)
)

// ErrorType defines the type of application errors
type ErrorType int

const (
	// ErrorTypeUnknown is the default error type.
	ErrorTypeUnknown ErrorType = 0

	ErrorTypeEpochMismatch  ErrorType = 1
	ErrorTypeEpochSmaller   ErrorType = 2
	ErrorTypeTaskIDMismatch ErrorType = 3

	ErrorTypeInvalid    ErrorType = 101
	ErrorTypeIncomplete ErrorType = 102
	ErrorTypeDecodeData ErrorType = 103
	ErrorTypeBufferFull ErrorType = 104
	ErrorTypeDuplicate  ErrorType = 105
	ErrorTypeNotExist   ErrorType = 106
	ErrorTypeClosed     ErrorType = 107

	ErrorTypeConnectionFailed     ErrorType = 201
	ErrorTypeConnectionNotFound   ErrorType = 202
	ErrorTypeMessageCongested     ErrorType = 204
	ErrorTypeMessageReceiveFailed ErrorType = 205
	ErrorTypeMessageSendFailed    ErrorType = 206
	ErrorTypeTargetNotFound       ErrorType = 207
	ErrorTypeInvalidMessage       ErrorType = 208
	ErrorTypeTargetMismatch       ErrorType = 209

	ErrorInvalidDDLEvent ErrorType = 301
)

func (t ErrorType) String() string {
	switch t {
	case ErrorTypeUnknown:
		return "Unknown"
	case ErrorTypeEpochMismatch:
		return "EpochMismatch"
	case ErrorTypeEpochSmaller:
		return "EpochSmaller"
	case ErrorTypeTaskIDMismatch:
		return "TaskIDMismatch"
	case ErrorTypeInvalid:
		return "Invalid"
	case ErrorTypeIncomplete:
		return "Incomplete"
	case ErrorTypeDecodeData:
		return "DecodeData"
	case ErrorTypeBufferFull:
		return "BufferFull"
	case ErrorTypeDuplicate:
		return "Duplicate"
	case ErrorTypeNotExist:
		return "NotExist"
	case ErrorTypeClosed:
		return "Closed"
	case ErrorTypeConnectionFailed:
		return "ConnectionFailed"
	case ErrorTypeConnectionNotFound:
		return "ConnectionNotFound"
	case ErrorTypeMessageCongested:
		return "MessageCongested"
	case ErrorTypeMessageReceiveFailed:
		return "MessageReceiveFailed"
	case ErrorTypeMessageSendFailed:
		return "MessageSendFailed"
	default:
		return "Unknown"
	}
}

// AppError represents an application-specific error
type AppError struct {
	Type   ErrorType
	Reason string
}

// NewAppErrorS creates a new AppError with only type
func NewAppErrorS(t ErrorType) *AppError {
	return &AppError{
		Type:   t,
		Reason: "",
	}
}

// NewAppError creates a new AppError with type and reason
func NewAppError(t ErrorType, reason string) *AppError {
	return &AppError{
		Type:   t,
		Reason: reason,
	}
}

func (e AppError) Error() string {
	return fmt.Sprintf("ErrorType: %s, Reason: %s", e.Type, e.Reason)
}

func (e AppError) GetType() ErrorType {
	return e.Type
}

func (e AppError) Equal(err AppError) bool {
	return e.Type == err.Type
}
