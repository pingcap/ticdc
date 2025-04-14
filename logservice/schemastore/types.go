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

package schemastore

import (
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/tidb/pkg/meta/model"
)

//go:generate msgp

// TODO: use msgp.Raw to do version management
type PersistedDDLEvent struct {
	ID   int64 `msg:"id"`
	Type byte  `msg:"type"`

	// the table name for the ddl job in the information_schema.ddl_jobs table(just ddl job.TableName)
	TableNameInDDLJob string `msg:"table_name_in_ddl_job"`
	// the database name for the ddl job in the information_schema.ddl_jobs table(just ddl job.dbName)
	DBNameInDDLJob string `msg:"db_name_in_ddl_job"`

	// SchemaID is from upstream Job.SchemaID, it corresponds to TableID
	// it is the DB id of the table after the ddl
	SchemaID int64 `msg:"schema_id"`
	// TableID is from upstream Job.TableID
	// - for most ddl types which just involve a single table id, it is the table id of the table
	// - for ExchangeTablePartition, it is the table id of the normal table before exchange
	//   and it is one of of the partition ids after exchange
	// - for TruncateTable, it the table ID of the old table
	TableID int64 `msg:"table_id"`
	// SchemaName corresponds to SchemaID
	SchemaName string `msg:"schema_name"`
	// TableName corresponds to TableID
	TableName string `msg:"table_name"`

	// ExtraSchemaID corresponds to ExtraTableID
	ExtraSchemaID int64 `msg:"extra_schema_id"`
	// - for ExchangeTablePartition, it is the table id of the partition table
	// - for TruncateTable, it the table ID of the new table
	ExtraTableID int64 `msg:"extra_table_id"`
	// ExtraSchemaName corresponds to ExtraSchemaID
	ExtraSchemaName string `msg:"extra_schema_name"`
	// ExtraTableName corresponds to ExtraTableID
	ExtraTableName string `msg:"extra_table_name"`

	// the following fields are only used for RenameTables
	SchemaIDs        []int64  `msg:"schema_ids"`
	SchemaNames      []string `msg:"schema_names"`
	ExtraSchemaIDs   []int64  `msg:"extra_schema_ids"`
	ExtraSchemaNames []string `msg:"extra_schema_names"`
	ExtraTableNames  []string `msg:"extra_table_names"`

	// the following fields are only set when the ddl job involves a partition table
	// it is the partition info of the table before this ddl
	PrevPartitions []int64 `msg:"prev_partitions"`

	Query         string `msg:"query"`
	SchemaVersion int64  `msg:"schema_version"`
	FinishedTs    uint64 `msg:"finished_ts"`

	DBInfo *model.DBInfo `msg:"-"`
	// it is from upstream job.TableInfo
	// - for most ddl types which just involve a single table id, it is the table info of the table after the ddl
	// - for ExchangeTablePartition, it is the table info of the partition table after exchange
	//   note: ExtraTableID is the partition table id. (it is a little tricky)
	TableInfo      *model.TableInfo `msg:"-"`
	TableInfoValue []byte           `msg:"table_info_value"`
	// - for ExchangeTablePartition, it is the the info of the normal table before exchange
	//   and we derive the normal table info after exchange from this field(by clone it with a different table id)
	ExtraTableInfo      *common.TableInfo `msg:"-"`
	ExtraTableInfoValue []byte            `msg:"extra_table_info_value"`
	// the following fields are just used for CreateTables and RenameTables
	MultipleTableInfos      []*model.TableInfo `msg:"-"`
	MultipleTableInfosValue [][]byte           `msg:"multi_table_info_value"`

	// TODO: do we need the following two fields?
	BDRRole        string `msg:"bdr_role"`
	CDCWriteSource uint64 `msg:"cdc_write_source"`
}

// TODO: use msgp.Raw to do version management
type PersistedTableInfoEntry struct {
	SchemaID       int64  `msg:"schema_id"`
	SchemaName     string `msg:"schema_name"`
	TableInfoValue []byte `msg:"table_info_value"`
}

type UpperBoundMeta struct {
	FinishedDDLTs uint64 `msg:"finished_ddl_ts"`
	SchemaVersion int64  `msg:"schema_version"`
	ResolvedTs    uint64 `msg:"resolved_ts"`
}

//msgp:ignore BasicDatabaseInfo
type BasicDatabaseInfo struct {
	Name   string
	Tables map[int64]bool
}

//msgp:ignore BasicTableInfo
type BasicTableInfo struct {
	SchemaID int64
	Name     string
}

type BasicPartitionInfo map[int64]interface{}

func (info BasicPartitionInfo) AddPartitionIDs(ids ...int64) {
	for _, id := range ids {
		info[id] = nil
	}
}

func (info BasicPartitionInfo) RemovePartitionIDs(ids ...int64) {
	for _, id := range ids {
		delete(info, id)
	}
}

//msgp:ignore DDLJobWithCommitTs
type DDLJobWithCommitTs struct {
	Job *model.Job
	// the commitTs of the rawKVEntry which contains the DDL job, which euqals to Job.BinlogInfo.FinishedTS
	CommitTs uint64
}
