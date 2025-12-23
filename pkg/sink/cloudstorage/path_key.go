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
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
)

// SchemaPathKey is the key of schema path.
type SchemaPathKey struct {
	Schema       string
	Table        string
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
	version, checksum := mustParseSchemaName(schemaFileName)

	*s = SchemaPathKey{
		Schema:       schema,
		Table:        table,
		TableVersion: version,
	}
	return checksum, nil
}

type FileIndexKey struct {
	DispatcherID           string
	EnableTableAcrossNodes bool
}

type FileIndex struct {
	FileIndexKey
	Idx uint64
}

// DmlPathKey is the key of dml path.
type DmlPathKey struct {
	SchemaPathKey
	PartitionNum int64
	Date         string
}

// GenerateDMLFilePath generates the dml file path.
func (d *DmlPathKey) GenerateDMLFilePath(
	fileIndex *FileIndex, extension string, fileIndexWidth int,
) string {
	var elems []string

	elems = append(elems, d.Schema)
	elems = append(elems, d.Table)
	elems = append(elems, fmt.Sprintf("%d", d.TableVersion))

	if d.PartitionNum != 0 {
		elems = append(elems, fmt.Sprintf("%d", d.PartitionNum))
	}
	if len(d.Date) != 0 {
		elems = append(elems, d.Date)
	}
	elems = append(elems, generateDataFileName(fileIndex.EnableTableAcrossNodes, fileIndex.DispatcherID, fileIndex.Idx, extension, fileIndexWidth))

	return strings.Join(elems, "/")
}

// ParseDMLFilePath parses the dml file path and returns the max file index.
// DML file path pattern is as follows:
// {schema}/{table}/{table-version-separator}/{partition-separator}/{date-separator}/, where
// partition-separator and date-separator could be empty.
// DML file name pattern is as follows: CDC_{dispatcherID}_{num}.extension or CDC{num}.extension
func (d *DmlPathKey) ParseDMLFilePath(dateSeparator, path string) (*FileIndex, error) {
	var partitionNum int64

	str := `(\w+)\/(\w+)\/(\d+)\/(\d+)?\/*`
	switch dateSeparator {
	case config.DateSeparatorNone.String():
		str += `(\d{4})*`
	case config.DateSeparatorYear.String():
		str += `(\d{4})\/`
	case config.DateSeparatorMonth.String():
		str += `(\d{4}-\d{2})\/`
	case config.DateSeparatorDay.String():
		str += `(\d{4}-\d{2}-\d{2})\/`
	}
	matchesLen := 8
	matchesFileIdx := 7
	// CDC[_{dispatcherID}_]{num}.extension
	str += `CDC(?:_(\w+)_)?(\d+).\w+`
	pathRE, err := regexp.Compile(str)
	if err != nil {
		return nil, err
	}

	matches := pathRE.FindStringSubmatch(path)
	if len(matches) != matchesLen {
		return nil, fmt.Errorf("cannot match dml path pattern for %s", path)
	}

	version, err := strconv.ParseUint(matches[3], 10, 64)
	if err != nil {
		return nil, err
	}

	if len(matches[4]) > 0 {
		partitionNum, err = strconv.ParseInt(matches[4], 10, 64)
		if err != nil {
			return nil, err
		}
	}

	fileIdx, err := strconv.ParseUint(strings.TrimLeft(matches[matchesFileIdx], "0"), 10, 64)
	if err != nil {
		return nil, err
	}

	*d = DmlPathKey{
		SchemaPathKey: SchemaPathKey{
			Schema:       matches[1],
			Table:        matches[2],
			TableVersion: version,
		},
		PartitionNum: partitionNum,
		Date:         matches[5],
	}

	return &FileIndex{
		FileIndexKey: FileIndexKey{
			DispatcherID:           matches[6],
			EnableTableAcrossNodes: matches[6] != "",
		},
		Idx: fileIdx,
	}, nil
}
