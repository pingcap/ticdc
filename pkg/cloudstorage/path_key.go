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
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
)

const schemaFilePartitionNum int64 = -1

// SchemaPathKey is the key of schema path.
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

// GetKey returns the key of schema path.
func (s *SchemaPathKey) GetKey() string {
	return common.QuoteSchema(s.Schema, s.Table)
}

// ParseSchemaFilePath parses the schema file path and returns the table version and checksum.
func (s *SchemaPathKey) ParseSchemaFilePath(path string) (uint32, error) {
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
		return 0, errors.Trace(fmt.Errorf("cannot match schema path pattern for %s", path))
	}

	if matches[len(matches)-2] != "meta" {
		return 0, errors.Trace(fmt.Errorf("cannot match schema path pattern for %s", path))
	}

	schemaFileName := matches[len(matches)-1]
	version, checksum := mustParseSchemaFileName(schemaFileName)

	*s = SchemaPathKey{
		Schema:       schema,
		Table:        table,
		TableVersion: version,
	}
	return checksum, nil
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

// DmlPathKey is the key of dml path.
type DmlPathKey struct {
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
func NewSchemaFileDMLPathKey(schemaKey SchemaPathKey) DmlPathKey {
	return DmlPathKey{
		SchemaPathKey: schemaKey,
		PartitionNum:  schemaFilePartitionNum,
	}
}

// IsSchemaFileDMLPathKey checks whether the key represents a schema file marker.
func (d DmlPathKey) IsSchemaFileDMLPathKey() bool {
	return d.PartitionNum == schemaFilePartitionNum && d.Date == ""
}

// CompareDMLPathKey compares DML path keys in cloud storage replay order.
func CompareDMLPathKey(x, y DmlPathKey) int {
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

// GenerateDMLFilePath generates the dml file path.
func (d *DmlPathKey) GenerateDMLFilePath(
	fileIndex *FileIndex, extension string, fileIndexWidth int,
) string {
	fileName := generateDataFileName(
		fileIndex.EnableTableAcrossNodes, fileIndex.DispatcherID,
		fileIndex.Idx, extension, fileIndexWidth)
	return path.Join(d.generateDMLDataDirPath(), fileName)
}

func (d DmlPathKey) generateDMLDataDirPath() string {
	elems := make([]string, 0, 5)
	if d.UseTableIDAsPath {
		elems = append(elems, strconv.FormatInt(d.TableID, 10))
	} else {
		elems = append(elems, d.Schema, d.Table)
	}
	elems = append(elems, strconv.FormatUint(d.TableVersion, 10))
	if d.PartitionNum != 0 && !d.UseTableIDAsPath {
		elems = append(elems, strconv.FormatInt(d.PartitionNum, 10))
	}
	if d.Date != "" {
		elems = append(elems, d.Date)
	}
	return path.Join(elems...)
}

// ParseDMLFilePath parses a cloud storage data file path.
func (d *DmlPathKey) ParseDMLFilePath(
	dateSeparator, filePath, extension string,
) (FileIndex, error) {
	parts := strings.Split(filePath, "/")
	fileIndex, err := ParseFileIndexFromFileName(parts[len(parts)-1], extension)
	if err != nil {
		return FileIndex{}, err
	}
	if err = d.parseDMLDataDir(dateSeparator, parts[:len(parts)-1], filePath); err != nil {
		return FileIndex{}, err
	}
	return fileIndex, nil
}

func (d *DmlPathKey) parseDMLDataDir(
	dateSeparator string, parts []string, filePath string,
) error {
	var (
		key        DmlPathKey
		versionIdx int
		dateIdx    int
		err        error
	)
	switch dateSeparator {
	case config.DateSeparatorNone.String():
		switch len(parts) {
		case 2:
			key.UseTableIDAsPath = true
			key.Schema = parts[0]
			key.TableID, err = parseTableIDPathPart(parts[0])
			versionIdx = 1
		case 3:
			key.Schema, key.Table, versionIdx = parts[0], parts[1], 2
		case 4:
			key.Schema, key.Table, versionIdx = parts[0], parts[1], 2
			key.PartitionNum, err = strconv.ParseInt(parts[3], 10, 64)
			if err != nil {
				return err
			}
		default:
			return errors.ErrStorageSinkInvalidFileName.GenWithStack(
				"cannot match dml path pattern for %s", filePath)
		}
	case config.DateSeparatorYear.String(),
		config.DateSeparatorMonth.String(),
		config.DateSeparatorDay.String():
		switch len(parts) {
		case 3:
			key.UseTableIDAsPath = true
			key.Schema = parts[0]
			key.TableID, err = parseTableIDPathPart(parts[0])
			versionIdx, dateIdx = 1, 2
		case 4:
			key.Schema, key.Table, versionIdx, dateIdx = parts[0], parts[1], 2, 3
		case 5:
			key.Schema, key.Table, versionIdx, dateIdx = parts[0], parts[1], 2, 4
			key.PartitionNum, err = strconv.ParseInt(parts[3], 10, 64)
			if err != nil {
				return err
			}
		default:
			return errors.ErrStorageSinkInvalidFileName.GenWithStack(
				"cannot match dml path pattern for %s", filePath)
		}
		if !matchDateSeparatorValue(dateSeparator, parts[dateIdx]) {
			return errors.ErrStorageSinkInvalidFileName.GenWithStack(
				"cannot match date separator %s for %s", dateSeparator, parts[dateIdx])
		}
		key.Date = parts[dateIdx]
	default:
		return errors.ErrStorageSinkInvalidDateSeparator.GenWithStackByArgs(dateSeparator)
	}
	if err != nil {
		return err
	}
	if !key.UseTableIDAsPath && (key.Schema == "" || key.Table == "") {
		return errors.ErrStorageSinkInvalidFileName.GenWithStack(
			"cannot match dml path pattern for %s", filePath)
	}
	version, err := strconv.ParseUint(parts[versionIdx], 10, 64)
	if err != nil {
		return err
	}
	key.TableVersion = version
	*d = key
	return nil
}

func parseTableIDPathPart(part string) (int64, error) {
	tableID, err := strconv.ParseInt(part, 10, 64)
	if err != nil {
		return 0, err
	}
	if tableID <= 0 {
		return 0, errors.ErrStorageSinkInvalidFileName.GenWithStack(
			"invalid table id path part %s", part)
	}
	return tableID, nil
}

func matchDateSeparatorValue(separator string, value string) bool {
	switch separator {
	case config.DateSeparatorYear.String():
		return len(value) == 4 && isDigits(value)
	case config.DateSeparatorMonth.String():
		return len(value) == 7 &&
			value[4] == '-' &&
			isDigits(value[:4]) &&
			isDigits(value[5:])
	case config.DateSeparatorDay.String():
		return len(value) == 10 &&
			value[4] == '-' &&
			value[7] == '-' &&
			isDigits(value[:4]) &&
			isDigits(value[5:7]) &&
			isDigits(value[8:])
	default:
		return false
	}
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// ParseIndexFilePath parses the index file path and returns the max file index.
// index file path pattern is as follows:
// {schema}/{table}/{table-version-separator}/{partition-separator}/{date-separator}/meta/, where
// partition-separator and date-separator could be empty.
// DML file name pattern is as follows: CDC_{dispatcherID}.index or CDC.index
func (d *DmlPathKey) ParseIndexFilePath(dateSeparator, path string) (string, error) {
	parts := strings.Split(path, "/")
	if len(parts) < 4 || parts[len(parts)-2] != "meta" {
		return "", errors.ErrStorageSinkInvalidFileName.GenWithStack(
			"cannot match dml path pattern for %s", path)
	}
	dispatcherID, err := parseIndexFileName(parts[len(parts)-1])
	if err != nil {
		return "", err
	}
	if err = d.parseDMLDataDir(dateSeparator, parts[:len(parts)-2], path); err != nil {
		return "", err
	}

	return dispatcherID, nil
}

func parseIndexFileName(fileName string) (string, error) {
	const indexFileExtension = ".index"
	if !strings.HasPrefix(fileName, "CDC") || !strings.HasSuffix(fileName, indexFileExtension) {
		return "", errors.ErrStorageSinkInvalidFileName.GenWithStack(
			"cannot match dml index file name pattern for %q", fileName)
	}
	dispatcherID := strings.TrimSuffix(strings.TrimPrefix(fileName, "CDC"), indexFileExtension)
	if dispatcherID == "" {
		return "", nil
	}
	if !strings.HasPrefix(dispatcherID, "_") || len(dispatcherID) == 1 {
		return "", errors.ErrStorageSinkInvalidFileName.GenWithStack(
			"cannot match dml index file name pattern for %q", fileName)
	}
	return dispatcherID[1:], nil
}
