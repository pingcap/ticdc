// Copyright 2026 PingCAP, Inc.
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

package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
)

const minFileNamePrefixLen = 3 + config.MinFileIndexWidth

var schemaRE = regexp.MustCompile(`meta/schema_\d+_\d{10}\.json$`)

// SchemaPathKey is the key of schema path for storage consumer.
type SchemaPathKey struct {
	Schema       string
	Table        string
	TableVersion uint64
}

// GetKey returns the key of schema path.
func (s *SchemaPathKey) GetKey() string {
	return common.QuoteSchema(s.Schema, s.Table)
}

// ParseSchemaFilePath parses the schema file path and returns the checksum.
func (s *SchemaPathKey) ParseSchemaFilePath(filePath string) (uint32, error) {
	segments := strings.Split(filePath, "/")
	if len(segments) != 3 && len(segments) != 4 {
		return 0, fmt.Errorf("cannot match schema path pattern for %s", filePath)
	}
	if segments[len(segments)-2] != "meta" {
		return 0, fmt.Errorf("cannot match schema path pattern for %s", filePath)
	}

	tableVersion, checksum, err := parseSchemaFileName(segments[len(segments)-1])
	if err != nil {
		return 0, err
	}

	schema := segments[0]
	table := ""
	if len(segments) == 4 {
		table = segments[1]
	}

	*s = SchemaPathKey{
		Schema:       schema,
		Table:        table,
		TableVersion: tableVersion,
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

// DmlPathKey is the key of dml path for storage consumer.
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

// ParseIndexFilePath parses the index file path and returns dispatcher ID.
func (d *DmlPathKey) ParseIndexFilePath(dateSeparator, filePath string) (string, error) {
	segments := strings.Split(filePath, "/")
	if len(segments) < 5 || len(segments) > 7 {
		return "", fmt.Errorf("cannot match dml path pattern for %s", filePath)
	}
	if segments[len(segments)-2] != "meta" {
		return "", fmt.Errorf("cannot match dml path pattern for %s", filePath)
	}

	version, err := strconv.ParseUint(segments[2], 10, 64)
	if err != nil {
		return "", err
	}

	dispatcherID, err := parseDispatcherIDFromIndexFileName(segments[len(segments)-1])
	if err != nil {
		return "", err
	}

	extraSegments := segments[3 : len(segments)-2]
	partitionNum, date, err := parseDMLPathExtraSegments(dateSeparator, extraSegments, filePath)
	if err != nil {
		return "", err
	}

	*d = DmlPathKey{
		SchemaPathKey: SchemaPathKey{
			Schema:       segments[0],
			Table:        segments[1],
			TableVersion: version,
		},
		PartitionNum: partitionNum,
		Date:         date,
	}

	return dispatcherID, nil
}

func parseSchemaFileName(fileName string) (uint64, uint32, error) {
	parts := strings.Split(fileName, "_")
	if len(parts) < 3 {
		return 0, 0, fmt.Errorf("cannot match schema file pattern for %s", fileName)
	}

	checksumString := strings.TrimSuffix(parts[len(parts)-1], ".json")
	checksum, err := strconv.ParseUint(checksumString, 10, 32)
	if err != nil {
		return 0, 0, err
	}

	tableVersionString := parts[len(parts)-2]
	tableVersion, err := strconv.ParseUint(tableVersionString, 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return tableVersion, uint32(checksum), nil
}

func parseDMLPathExtraSegments(
	dateSeparator string,
	extraSegments []string,
	path string,
) (int64, string, error) {
	parsePartition := func(value string) (int64, error) {
		partitionNum, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("cannot match dml path pattern for %s", path)
		}
		return partitionNum, nil
	}

	validateDate := func(value string) error {
		if !isValidDateSegment(dateSeparator, value) {
			return fmt.Errorf("cannot match dml path pattern for %s", path)
		}
		return nil
	}

	switch len(extraSegments) {
	case 0:
		if dateSeparator != config.DateSeparatorNone.String() {
			return 0, "", fmt.Errorf("cannot match dml path pattern for %s", path)
		}
		return 0, "", nil
	case 1:
		segment := extraSegments[0]
		if dateSeparator != config.DateSeparatorNone.String() && isValidDateSegment(dateSeparator, segment) {
			return 0, segment, nil
		}
		partitionNum, err := parsePartition(segment)
		if err != nil {
			return 0, "", err
		}
		return partitionNum, "", nil
	case 2:
		partitionNum, err := parsePartition(extraSegments[0])
		if err != nil {
			return 0, "", err
		}
		if err := validateDate(extraSegments[1]); err != nil {
			return 0, "", err
		}
		return partitionNum, extraSegments[1], nil
	default:
		return 0, "", fmt.Errorf("cannot match dml path pattern for %s", path)
	}
}

func parseDispatcherIDFromIndexFileName(fileName string) (string, error) {
	if !strings.HasPrefix(fileName, "CDC") || !strings.HasSuffix(fileName, ".index") {
		return "", fmt.Errorf("cannot match dml path pattern for %s", fileName)
	}

	fileName = strings.TrimSuffix(fileName, ".index")
	if fileName == "CDC" {
		return "", nil
	}

	if !strings.HasPrefix(fileName, "CDC_") {
		return "", fmt.Errorf("cannot match dml path pattern for %s", fileName)
	}
	dispatcherID := strings.TrimPrefix(fileName, "CDC_")
	if dispatcherID == "" {
		return "", fmt.Errorf("cannot match dml path pattern for %s", fileName)
	}
	return dispatcherID, nil
}

func isValidDateSegment(dateSeparator, value string) bool {
	switch dateSeparator {
	case config.DateSeparatorYear.String():
		return len(value) == 4 && isNumberString(value)
	case config.DateSeparatorMonth.String():
		return len(value) == 7 &&
			value[4] == '-' &&
			isNumberString(value[:4]) &&
			isNumberString(value[5:])
	case config.DateSeparatorDay.String():
		return len(value) == 10 &&
			value[4] == '-' &&
			value[7] == '-' &&
			isNumberString(value[:4]) &&
			isNumberString(value[5:7]) &&
			isNumberString(value[8:])
	default:
		return false
	}
}

func isNumberString(value string) bool {
	if value == "" {
		return false
	}
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}

// IsSchemaFile checks whether the file is a schema file.
func IsSchemaFile(filePath string) bool {
	return schemaRE.MatchString(filePath)
}

func generateDataFileName(enableTableAcrossNodes bool, dispatcherID string, index uint64, extension string, fileIndexWidth int) string {
	indexFmt := "%0" + strconv.Itoa(fileIndexWidth) + "d"
	if enableTableAcrossNodes {
		return fmt.Sprintf("CDC_%s_"+indexFmt+"%s", dispatcherID, index, extension)
	}
	return fmt.Sprintf("CDC"+indexFmt+"%s", index, extension)
}

func FetchIndexFromFileName(fileName string, extension string) (uint64, error) {
	if len(fileName) < minFileNamePrefixLen+len(extension) ||
		!strings.HasPrefix(fileName, "CDC") ||
		!strings.HasSuffix(fileName, extension) {
		return 0, fmt.Errorf("filename in storage sink is invalid: %s", fileName)
	}

	fileName = strings.TrimSuffix(fileName, extension)
	indexPart := strings.TrimPrefix(fileName, "CDC")

	if strings.HasPrefix(indexPart, "_") {
		trimmed := strings.TrimPrefix(indexPart, "_")
		separatorIndex := strings.LastIndex(trimmed, "_")
		if separatorIndex <= 0 || separatorIndex >= len(trimmed)-1 {
			return 0, fmt.Errorf("filename in storage sink is invalid: %s", fileName)
		}
		indexPart = trimmed[separatorIndex+1:]
	}

	if len(indexPart) < config.MinFileIndexWidth || !isNumberString(indexPart) {
		return 0, fmt.Errorf("filename in storage sink is invalid: %s", fileName)
	}

	return strconv.ParseUint(indexPart, 10, 64)
}
