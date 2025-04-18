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
	"github.com/pingcap/errors"
)

// errors
var (
	// kv related errors
	ErrChangeFeedNotExists = errors.Normalize(
		"changefeed not exists, %s",
		errors.RFCCodeText("CDC:ErrChangeFeedNotExists"),
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
	ErrMetaListDatabases = errors.Normalize(
		"meta store list databases",
		errors.RFCCodeText("CDC:ErrMetaListDatabases"),
	)
	ErrDDLSchemaNotFound = errors.Normalize(
		"cannot find mysql.tidb_ddl_job schema",
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
		"message is too large",
		errors.RFCCodeText("CDC:ErrMessageTooLarge"),
	)
	ErrStorageSinkInvalidDateSeparator = errors.Normalize(
		"date separator in storage sink is invalid",
		errors.RFCCodeText("CDC:ErrStorageSinkInvalidDateSeparator"),
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
	ErrURLFormatInvalid = errors.Normalize(
		"url format is invalid",
		errors.RFCCodeText("CDC:ErrURLFormatInvalid"),
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
	ErrInvalidEtcdKey = errors.Normalize(
		"invalid key: %s",
		errors.RFCCodeText("CDC:ErrInvalidEtcdKey"),
	)

	ErrSchemaSnapshotNotFound = errors.Normalize(
		"can not found schema snapshot, ts: %d",
		errors.RFCCodeText("CDC:ErrSchemaSnapshotNotFound"),
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
	ErrChangefeedUnretryable = errors.Normalize(
		"changefeed is in unretryable state, please check the error message"+
			", and you should manually handle it",
		errors.RFCCodeText("CDC:ErrChangefeedUnretryable"),
	)

	// sorter errors
	ErrIllegalSorterParameter = errors.Normalize(
		"illegal parameter for sorter: %s",
		errors.RFCCodeText("CDC:ErrIllegalSorterParameter"),
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
)
