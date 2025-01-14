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

	// for exchange partition, it is the info of the partition table
	CurrentSchemaID   int64  `msg:"current_schema_id"`
	CurrentTableID    int64  `msg:"current_table_id"`
	CurrentSchemaName string `msg:"current_schema_name"`
	CurrentTableName  string `msg:"current_table_name"`

	// The following fields are only set when the ddl job involves a prev table
	// for exchange partition, it is the info of the normal table before exchange
	PrevSchemaID   int64  `msg:"prev_schema_id"`
	PrevTableID    int64  `msg:"prev_table_id"`
	PrevSchemaName string `msg:"prev_schema_name"`
	PrevTableName  string `msg:"prev_table_name"`

	// only used for rename tables
	PrevSchemaIDs      []int64  `msg:"prev_schema_ids"`
	PrevSchemaNames    []string `msg:"prev_schema_names"`
	PrevTableNames     []string `msg:"prev_table_names"`
	CurrentSchemaIDs   []int64  `msg:"current_schema_ids"`
	CurrentSchemaNames []string `msg:"s"`

	// The following fields are only set when the ddl job involves a partition table
	PrevPartitions []int64 `msg:"prev_partitions"`

	Query         string        `msg:"query"`
	SchemaVersion int64         `msg:"schema_version"`
	DBInfo        *model.DBInfo `msg:"-"`
	// for exchange partition, it is the info of the partition table
	TableInfo *model.TableInfo `msg:"-"`
	// TODO: use a custom struct to store the table info?
	TableInfoValue []byte `msg:"table_info_value"`
	// for exchange partition, it is the info of the normal table
	PreTableInfo *common.TableInfo `msg:"-"`
	// TODO: is there a better way to store PreTableInfo?
	PreTableInfoValue []byte `msg:"pre_table_info_value"`
	FinishedTs        uint64 `msg:"finished_ts"`

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

//msgp:ignore DDLJobWithCommitTs
type DDLJobWithCommitTs struct {
	Job *model.Job
	// the commitTs of the rawKVEntry which contains the DDL job, which euqals to Job.BinlogInfo.FinishedTS
	CommitTs uint64
}
