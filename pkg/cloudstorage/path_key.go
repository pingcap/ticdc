// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cloudstorage

import (
	"cmp"
	"path"
	"strconv"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"go.uber.org/zap"
)

const schemaFilePartitionNum int64 = -1

// SchemaPathKey identifies the schema path scope parsed from or used to build
// cloud storage paths.
type SchemaPathKey struct {
	// Schema is the first directory level in storage sink paths.
	// Example: <schema>/<table>/<tableVersion>/...
	Schema string
	// Table is the second directory level for table-level schema file and data paths.
	// For database-level schema files, this field is empty and the path is
	// <schema>/meta/schema_{tableVersion}_{checksum}.json.
	Table string
	// TableVersion is the schema version encoded in the path.
	// In CDC it is carried by tableInfoVersion, and for DDL-related versions it
	// is typically equal to the DDL finishedTs.
	TableVersion uint64
}

// GetKey returns the quoted schema/table key used by consumer maps.
// For database-level schema files, Table is empty.
func (s *SchemaPathKey) GetKey() string {
	return common.QuoteSchema(s.Schema, s.Table)
}

// Parse fills SchemaPathKey from a schema file path.
// Input:
//   - <schema>/meta/schema_<tableVersion>_<checksum>.json
//   - <schema>/<table>/meta/schema_<tableVersion>_<checksum>.json
//
// Output fields are Schema, Table, and TableVersion. Table is empty for
// database-level schema files. Invalid paths panic.
func (s *SchemaPathKey) Parse(path string) {
	// For <schema>/<table>/meta/schema_{tableVersion}_{checksum}.json, the parts
	// should be ["<schema>", "<table>", "meta", "schema_{tableVersion}_{checksum}.json"].
	matches := strings.Split(path, "/")

	var schema, table string
	schema = matches[0]
	switch len(matches) {
	case 3:
		table = ""
	case 4:
		table = matches[1]
	default:
		log.Panic("cannot match schema path pattern", zap.String("path", path))
	}

	if matches[len(matches)-2] != "meta" {
		log.Panic("cannot match schema path pattern", zap.String("path", path))
	}

	schemaFileName := matches[len(matches)-1]
	version, _ := mustParseSchemaFileName(schemaFileName)

	*s = SchemaPathKey{
		Schema:       schema,
		Table:        table,
		TableVersion: version,
	}
}

type FileIndexKey struct {
	// DispatcherID is used in file name only when table-across-nodes is enabled.
	// File pattern: CDC_{dispatcherID}_{index}.{ext}
	DispatcherID string
	// EnableTableAcrossNodes controls whether dispatcher ID is embedded in
	// data/index file names to avoid collisions across captures.
	EnableTableAcrossNodes bool
}

type FileIndex struct {
	FileIndexKey
	// Idx is the monotonically increasing file sequence number in one
	// directory scope (schema/table/version[/partition][/date] or
	// tableID/version[/date]).
	Idx uint64
}

// DMLPathKey is the key of dml path.
type DMLPathKey struct {
	SchemaPathKey
	// UseTableIDAsPath controls whether TableID is used as the first path
	// element instead of Schema/Table.
	UseTableIDAsPath bool
	// TableID is set when UseTableIDAsPath is true.
	TableID int64
	// PartitionNum is an optional path level for partition table output.
	// It is present only when partition-separator is enabled.
	PartitionNum int64
	// Date is an optional path level controlled by date-separator
	// (year/month/day/none).
	Date string
}

// NewSchemaFileDMLPathKey returns the synthetic DML path key used to order a
// schema file before data files with the same schema version.
func NewSchemaFileDMLPathKey(schemaKey SchemaPathKey) DMLPathKey {
	return DMLPathKey{
		SchemaPathKey: schemaKey,
		PartitionNum:  schemaFilePartitionNum,
	}
}

// IsSchemaFileDMLPathKey checks whether the key represents a schema file marker.
func (d DMLPathKey) IsSchemaFileDMLPathKey() bool {
	return d.PartitionNum == schemaFilePartitionNum && d.Date == ""
}

// CompareDMLPathKey compares DML path keys in cloud storage replay order.
func CompareDMLPathKey(x, y DMLPathKey) int {
	if r := cmp.Compare(x.TableVersion, y.TableVersion); r != 0 {
		return r
	}
	if r := cmp.Compare(x.PartitionNum, y.PartitionNum); r != 0 {
		return r
	}
	if r := cmp.Compare(x.Date, y.Date); r != 0 {
		return r
	}
	if x.UseTableIDAsPath != y.UseTableIDAsPath {
		if x.UseTableIDAsPath {
			return 1
		}
		return -1
	}
	if r := cmp.Compare(x.TableID, y.TableID); r != 0 {
		return r
	}
	if r := cmp.Compare(x.Schema, y.Schema); r != 0 {
		return r
	}
	return cmp.Compare(x.Table, y.Table)
}

// GenerateDMLFilePath returns the full data file path.
// The receiver supplies the data directory fields. fileIndex supplies the
// dispatcher ID and sequence number used in the file name. extension is the
// file suffix, for example ".json" or ".csv".
func (d *DMLPathKey) GenerateDMLFilePath(
	fileIndex *FileIndex, extension string, fileIndexWidth int,
) string {
	fileName := generateDataFileName(
		fileIndex.EnableTableAcrossNodes, fileIndex.DispatcherID,
		fileIndex.Idx, extension, fileIndexWidth)
	return path.Join(d.generateDMLDataDirPath(), fileName)
}

// GenerateIndexFilePath returns the index file path for this data directory.
// Output is <data-dir>/meta/CDC.index or <data-dir>/meta/CDC_<dispatcherID>.index.
func (d *DMLPathKey) GenerateIndexFilePath(fileIndexKey FileIndexKey) string {
	return path.Join(
		d.generateDMLDataDirPath(),
		generateIndexFileName(fileIndexKey.EnableTableAcrossNodes, fileIndexKey.DispatcherID),
	)
}

// generateDMLDataDirPath returns the canonical data directory path.
// Output is either <tableID>/<version>[/date] or
// <schema>/<table>/<version>[/partition][/date].
func (d DMLPathKey) generateDMLDataDirPath() string {
	elems := make([]string, 0, 5)
	if d.UseTableIDAsPath {
		elems = append(elems, strconv.FormatInt(d.TableID, 10))
		elems = append(elems, strconv.FormatUint(d.TableVersion, 10))
		if d.Date != "" {
			elems = append(elems, d.Date)
		}
		return path.Join(elems...)
	}
	elems = append(elems, d.Schema, d.Table)
	elems = append(elems, strconv.FormatUint(d.TableVersion, 10))
	if d.PartitionNum != 0 {
		elems = append(elems, strconv.FormatInt(d.PartitionNum, 10))
	}
	if d.Date != "" {
		elems = append(elems, d.Date)
	}
	return path.Join(elems...)
}

func (d *DMLPathKey) parseDMLDataDir(
	dateSeparator string, parts []string, filePath string,
) {
	var (
		key       DMLPathKey
		version   string
		tableID   string
		partition string
		hasDate   bool
	)
	switch dateSeparator {
	case config.DateSeparatorNone.String():
	case config.DateSeparatorYear.String(),
		config.DateSeparatorMonth.String(),
		config.DateSeparatorDay.String():
		hasDate = true
	default:
		log.Panic("invalid date separator", zap.String("dateSeparator", dateSeparator))
	}

	switch {
	case !hasDate && len(parts) == 2:
		key.Schema = parts[0]
		tableID = parts[0]
		version = parts[1]
	case !hasDate && len(parts) == 3:
		key.Schema, key.Table = parts[0], parts[1]
		version = parts[2]
	case !hasDate && len(parts) == 4:
		key.Schema, key.Table = parts[0], parts[1]
		partition = parts[3]
		version = parts[2]
	case hasDate && len(parts) == 3:
		key.Schema = parts[0]
		tableID = parts[0]
		key.Date = parts[2]
		version = parts[1]
	case hasDate && len(parts) == 4:
		key.Schema, key.Table = parts[0], parts[1]
		key.Date = parts[3]
		version = parts[2]
	case hasDate && len(parts) == 5:
		key.Schema, key.Table = parts[0], parts[1]
		partition = parts[3]
		key.Date = parts[4]
		version = parts[2]
	default:
		log.Panic("cannot match dml path pattern", zap.String("path", filePath))
	}

	if tableID != "" {
		tableIDNum, err := strconv.ParseInt(tableID, 10, 64)
		if err != nil {
			log.Panic("parse table id failed", zap.String("value", tableID), zap.Error(err))
		}
		key.UseTableIDAsPath = true
		key.TableID = tableIDNum
	}
	if partition != "" {
		partitionNum, err := strconv.ParseInt(partition, 10, 64)
		if err != nil {
			log.Panic("parse partition number failed", zap.String("value", partition), zap.Error(err))
		}
		key.PartitionNum = partitionNum
	}
	tableVersion, err := strconv.ParseUint(version, 10, 64)
	if err != nil {
		log.Panic("parse table version failed", zap.String("value", version), zap.Error(err))
	}
	key.TableVersion = tableVersion
	*d = key
}

// ParseIndexFilePath fills DMLPathKey from an index file path.
// Input is <data-dir>/meta/CDC.index or
// <data-dir>/meta/CDC_<dispatcherID>.index. Only the data directory portion is
// parsed here; the file index itself is stored in the index file content and is
// read by the caller. Invalid paths panic.
func (d *DMLPathKey) ParseIndexFilePath(dateSeparator, path string) {
	parts := strings.Split(path, "/")
	if len(parts) < 4 || parts[len(parts)-2] != "meta" {
		log.Panic("cannot match dml path pattern", zap.String("path", path))
	}
	d.parseDMLDataDir(dateSeparator, parts[:len(parts)-2], path)
}

// ParseDMLFilePath fills DMLPathKey from a data file path and returns the file
// index encoded in the file name.
// Input is <data-dir>/CDC<idx><extension> or
// <data-dir>/CDC_<dispatcherID>_<idx><extension>. Invalid paths panic.
func (d *DMLPathKey) ParseDMLFilePath(dateSeparator, filePath, extension string) FileIndex {
	parts := strings.Split(filePath, "/")
	fileIndex, err := ParseFileIndexFromFileName(parts[len(parts)-1], extension)
	if err != nil {
		log.Panic("parse file index from file name failed",
			zap.String("path", filePath), zap.Error(err))
	}
	d.parseDMLDataDir(dateSeparator, parts[:len(parts)-1], filePath)
	return fileIndex
}
