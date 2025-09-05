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

package avro

import (
	"strings"

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

const (
	keySchemaSuffix   = "-key"
	valueSchemaSuffix = "-value"
)

const (
	tidbType         = "tidb_type"
	tidbOp           = "_tidb_op"
	tidbCommitTs     = "_tidb_commit_ts"
	tidbPhysicalTime = "_tidb_commit_physical_time"

	// row level checksum related fields
	tidbRowLevelChecksum = "_tidb_row_level_checksum"
	tidbChecksumVersion  = "_tidb_checksum_version"
	tidbCorrupted        = "_tidb_corrupted"
)

const (
	insertOperation = "c"
	updateOperation = "u"
)

const (
	// avro does not send ddl and checkpoint message, the following 2 field is used to distinguish
	// TiCDC DDL event and checkpoint event, only used for testing purpose, not for production
	ddlByte        = uint8(1)
	checkpointByte = uint8(2)
)

var type2TiDBType = map[byte]string{
	mysql.TypeTiny:              "INT",
	mysql.TypeShort:             "INT",
	mysql.TypeInt24:             "INT",
	mysql.TypeLong:              "INT",
	mysql.TypeLonglong:          "BIGINT",
	mysql.TypeFloat:             "FLOAT",
	mysql.TypeDouble:            "DOUBLE",
	mysql.TypeBit:               "BIT",
	mysql.TypeNewDecimal:        "DECIMAL",
	mysql.TypeTinyBlob:          "TEXT",
	mysql.TypeMediumBlob:        "TEXT",
	mysql.TypeBlob:              "TEXT",
	mysql.TypeLongBlob:          "TEXT",
	mysql.TypeVarchar:           "TEXT",
	mysql.TypeVarString:         "TEXT",
	mysql.TypeString:            "TEXT",
	mysql.TypeEnum:              "ENUM",
	mysql.TypeSet:               "SET",
	mysql.TypeJSON:              "JSON",
	mysql.TypeDate:              "DATE",
	mysql.TypeDatetime:          "DATETIME",
	mysql.TypeTimestamp:         "TIMESTAMP",
	mysql.TypeDuration:          "TIME",
	mysql.TypeYear:              "YEAR",
	mysql.TypeTiDBVectorFloat32: "TiDBVECTORFloat32",
}

type avroSchemaTop struct {
	Tp        string                   `json:"type"`
	Name      string                   `json:"name"`
	Namespace string                   `json:"namespace"`
	Fields    []map[string]interface{} `json:"fields"`
}

type ddlEvent struct {
	Query    string             `json:"query"`
	Type     timodel.ActionType `json:"type"`
	Schema   string             `json:"schema"`
	Table    string             `json:"table"`
	CommitTs uint64             `json:"commitTs"`
}

type avroEncodeInput struct {
	row            *chunk.Row
	index          []int
	colInfos       []*timodel.ColumnInfo
	columnselector commonEvent.Selector
}

type avroSchema struct {
	Type string `json:"type"`
	// connect.parameters is designated field extracted by schema registry
	Parameters map[string]string `json:"connect.parameters"`
}

type avroLogicalTypeSchema struct {
	avroSchema
	LogicalType string      `json:"logicalType"`
	Precision   interface{} `json:"precision,omitempty"`
	Scale       interface{} `json:"scale,omitempty"`
}

func (r *avroEncodeInput) Less(i, j int) bool {
	return r.colInfos[i].ID < r.colInfos[j].ID
}

func (r *avroEncodeInput) Len() int {
	return len(r.index)
}

func (r *avroEncodeInput) Swap(i, j int) {
	r.colInfos[i], r.colInfos[j] = r.colInfos[j], r.colInfos[i]
	r.index[i], r.index[j] = r.index[j], r.index[i]
}

func getTiDBTypeFromColumn(col *timodel.ColumnInfo) string {
	tt := type2TiDBType[col.GetType()]
	if mysql.HasUnsignedFlag(col.GetFlag()) && (tt == "INT" || tt == "BIGINT") {
		return tt + " UNSIGNED"
	}
	if mysql.HasBinaryFlag(col.GetFlag()) && tt == "TEXT" {
		return "BLOB"
	}
	return tt
}

func topicName2SchemaSubjects(topicName, subjectSuffix string) string {
	return topicName + subjectSuffix
}

func getOperation(e *commonEvent.RowEvent) string {
	if e.IsInsert() {
		return insertOperation
	} else if e.IsUpdate() {
		return updateOperation
	}
	return ""
}

// sanitizeTopic escapes ".", it may have special meanings for sink connectors
func sanitizeTopic(name string) string {
	return strings.ReplaceAll(name, ".", "_")
}

// <empty> | <name>[(<dot><name>)*]
func getAvroNamespace(namespace string, schema string) string {
	ns := common.SanitizeName(namespace)
	s := common.SanitizeName(schema)
	if s != "" {
		return ns + "." + s
	}
	return ns
}
