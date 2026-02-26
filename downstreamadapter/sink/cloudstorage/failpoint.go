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

package cloudstorage

import (
	"bytes"
	cryptorand "crypto/rand"
	"encoding/json"
	"math/big"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"go.uber.org/zap"
)

func applyFailpointsOnEncodedMessages(frag eventFragment) {
	rowRecordsByMsg := splitRowRecordsByMessages(frag.encodedMsgs, dmlEventToRowRecords(frag.event))
	for idx, msg := range frag.encodedMsgs {
		var rowRecords []RowRecord
		if idx < len(rowRecordsByMsg) {
			rowRecords = rowRecordsByMsg[idx]
		}
		failpoint.Inject("cloudStorageSinkDropMessage", func() {
			log.Warn("cloudStorageSinkDropMessage: dropping message to simulate data loss",
				zap.Any("rows", rowRecords))
			Write("cloudStorageSinkDropMessage", rowRecords)
			// Keep callback flow unchanged while dropping data payload.
			msg.Key = nil
			msg.Value = nil
			msg.SetRowsCount(0)
			failpoint.Continue()
		})
		failpoint.Inject("cloudStorageSinkMutateValue", func() {
			log.Warn("cloudStorageSinkMutateValue: mutating message value to simulate data inconsistency",
				zap.Any("rows", rowRecords))
			mutatedRows, originTsMutatedRows := mutateMessageValueForFailpoint(msg, rowRecords)
			if len(mutatedRows) > 0 {
				Write("cloudStorageSinkMutateValue", mutatedRows)
			}
			if len(originTsMutatedRows) > 0 {
				Write("cloudStorageSinkMutateValueTidbOriginTs", originTsMutatedRows)
			}
		})
	}
}

func splitRowRecordsByMessages(messages []*common.Message, rows []RowRecord) [][]RowRecord {
	if len(messages) == 0 {
		return nil
	}
	ret := make([][]RowRecord, 0, len(messages))
	rowIdx := 0
	for _, msg := range messages {
		rowsNeeded := msg.GetRowsCount()
		if rowsNeeded <= 0 || rowIdx >= len(rows) {
			ret = append(ret, nil)
			continue
		}
		end := rowIdx + rowsNeeded
		if end > len(rows) {
			end = len(rows)
		}
		ret = append(ret, rows[rowIdx:end])
		rowIdx = end
	}
	return ret
}

func dmlEventToRowRecords(event *commonEvent.DMLEvent) []RowRecord {
	if event == nil || event.TableInfo == nil {
		return nil
	}
	indexes, columns := (&commonEvent.RowEvent{TableInfo: event.TableInfo}).PrimaryKeyColumn()
	originTsCol, hasOriginTsCol := event.TableInfo.GetColumnInfoByName(commonEvent.OriginTsColumn)
	originTsOffset, hasOriginTsOffset := event.TableInfo.GetColumnOffsetByName(commonEvent.OriginTsColumn)
	rowRecords := make([]RowRecord, 0, event.Len())
	for {
		row, ok := event.GetNextRow()
		if !ok {
			event.Rewind()
			break
		}
		rowData := row.Row
		if row.RowType == commonType.RowTypeDelete {
			rowData = row.PreRow
		}

		pks := make(map[string]any, len(columns))
		for i, col := range columns {
			if col == nil {
				continue
			}
			pks[col.Name.String()] = commonType.ExtractColVal(&rowData, col, indexes[i])
		}
		originTs := uint64(0)
		if hasOriginTsCol && hasOriginTsOffset {
			originTs = NormalizeOriginTs(
				commonType.ExtractColVal(&rowData, originTsCol, originTsOffset),
			)
		}
		rowRecords = append(rowRecords, RowRecord{
			CommitTs:    event.CommitTs,
			OriginTs:    originTs,
			PrimaryKeys: pks,
		})
	}
	return rowRecords
}

// mutateMessageValueForFailpoint rewrites a non-primary-key column value in
// canal-json encoded messages so that the multi-cluster-consistency-checker
// sees the original row as "lost" and the mutated row as "redundant".
//
// canal-json messages in msg.Value are separated by CRLF ("\r\n"). For every
// message we:
//  1. Parse the JSON to extract "pkNames" and "data".
//  2. Pick the first non-PK column in data[0] and replace its value with nil.
//  3. Re-marshal the whole message.
//
// This function is only called from within a failpoint.Inject block.
// It returns mutated row records grouped by whether `_tidb_origin_ts` is mutated.
func mutateMessageValueForFailpoint(
	msg *common.Message,
	rowRecords []RowRecord,
) ([]RowRecord, []RowRecord) {
	if len(msg.Value) == 0 {
		return nil, nil
	}
	terminator := []byte("\r\n")
	parts := bytes.Split(msg.Value, terminator)
	mutatedOffsets := make([]int, 0)
	originTsMutatedOffsets := make([]int, 0)
	rowOffset := 0
	for i, part := range parts {
		if len(part) == 0 {
			continue
		}

		// Decode the full message preserving all fields.
		var m map[string]json.RawMessage
		if err := json.Unmarshal(part, &m); err != nil {
			continue
		}

		// Extract pkNames so we can skip PK columns.
		var pkNames []string
		if raw, ok := m["pkNames"]; ok {
			_ = json.Unmarshal(raw, &pkNames)
		}
		pkSet := make(map[string]struct{}, len(pkNames))
		for _, pk := range pkNames {
			pkSet[pk] = struct{}{}
		}

		// Extract the "data" array.
		rawData, ok := m["data"]
		if !ok {
			continue
		}
		var data []map[string]any
		if err := json.Unmarshal(rawData, &data); err != nil || len(data) == 0 {
			continue
		}

		// Find the first row that has a non-PK column and mutate it to nil.
		mutated := false
		mutatedRowOffset := 0
		mutatedColumn := ""
		for rowIdx, row := range data {
			col, ok := selectColumnToMutate(row, pkSet)
			if !ok {
				continue
			}
			if col == commonEvent.OriginTsColumn {
				nextValue, ok := incrementOriginTSValue(row[col])
				if !ok {
					continue
				}
				row[col] = nextValue
			} else {
				row[col] = nil
			}
			mutated = true
			mutatedRowOffset = rowIdx
			mutatedColumn = col
			if mutated {
				break
			}
		}
		if !mutated {
			rowOffset += len(data)
			continue
		}

		// Write the mutated data back.
		newData, err := json.Marshal(data)
		if err != nil {
			rowOffset += len(data)
			continue
		}
		m["data"] = json.RawMessage(newData)

		newPart, err := json.Marshal(m)
		if err != nil {
			rowOffset += len(data)
			continue
		}
		parts[i] = newPart

		if mutatedColumn == commonEvent.OriginTsColumn {
			originTsMutatedOffsets = append(originTsMutatedOffsets, rowOffset+mutatedRowOffset)
		} else {
			mutatedOffsets = append(mutatedOffsets, rowOffset+mutatedRowOffset)
		}
		rowOffset += len(data)
	}
	msg.Value = bytes.Join(parts, terminator)
	return extractMutatedRowRecordsByOffset(rowRecords, mutatedOffsets),
		extractMutatedRowRecordsByOffset(rowRecords, originTsMutatedOffsets)
}

func selectColumnToMutate(row map[string]any, pkSet map[string]struct{}) (string, bool) {
	// Prefer mutating _tidb_origin_ts when it exists and is non-NULL.
	// Otherwise, mutate other non-PK columns.
	if _, isPK := pkSet[commonEvent.OriginTsColumn]; !isPK {
		if originTs, ok := row[commonEvent.OriginTsColumn]; ok && originTs != nil {
			return commonEvent.OriginTsColumn, true
		}
	}

	columns := make([]string, 0, len(row))
	for col := range row {
		if _, isPK := pkSet[col]; isPK {
			continue
		}
		if col == commonEvent.OriginTsColumn {
			continue
		}
		// Keep the failpoint mutation meaningful: skip columns that are already NULL.
		if row[col] == nil {
			continue
		}
		columns = append(columns, col)
	}
	if len(columns) == 0 {
		return "", false
	}
	idx, err := cryptorand.Int(cryptorand.Reader, big.NewInt(int64(len(columns))))
	if err != nil {
		// Best-effort fallback in failpoint path.
		return columns[0], true
	}
	return columns[idx.Int64()], true
}

func extractMutatedRowRecordsByOffset(
	rowRecords []RowRecord,
	offsets []int,
) []RowRecord {
	if len(offsets) == 0 || len(rowRecords) == 0 {
		return nil
	}
	ret := make([]RowRecord, 0, len(offsets))
	for _, offset := range offsets {
		if offset < 0 || offset >= len(rowRecords) {
			continue
		}
		ret = append(ret, rowRecords[offset])
	}
	return ret
}

func incrementOriginTSValue(v any) (any, bool) {
	switch value := v.(type) {
	case string:
		originTS, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return nil, false
		}
		return strconv.FormatUint(originTS+1, 10), true
	case float64:
		return value + 1, true
	case json.Number:
		originTS, err := value.Int64()
		if err != nil {
			return nil, false
		}
		return json.Number(strconv.FormatInt(originTS+1, 10)), true
	case int:
		return value + 1, true
	case int8:
		return value + 1, true
	case int16:
		return value + 1, true
	case int32:
		return value + 1, true
	case int64:
		return value + 1, true
	case uint:
		return value + 1, true
	case uint8:
		return value + 1, true
	case uint16:
		return value + 1, true
	case uint32:
		return value + 1, true
	case uint64:
		return value + 1, true
	default:
		return nil, false
	}
}

// envKey is the environment variable that controls the output file path.
const envKey = "TICDC_FAILPOINT_RECORD_FILE"

// RowRecord captures the essential identity of a single affected row.
type RowRecord struct {
	CommitTs    uint64         `json:"commitTs"`
	OriginTs    uint64         `json:"originTs"`
	PrimaryKeys map[string]any `json:"primaryKeys"`
}

// NormalizeOriginTs converts `_tidb_origin_ts` values from row payloads into uint64.
// It returns 0 for nil/invalid values.
func NormalizeOriginTs(v any) uint64 {
	switch value := v.(type) {
	case nil:
		return 0
	case uint64:
		return value
	case uint:
		return uint64(value)
	case uint32:
		return uint64(value)
	case uint16:
		return uint64(value)
	case uint8:
		return uint64(value)
	case int64:
		if value < 0 {
			return 0
		}
		return uint64(value)
	case int:
		if value < 0 {
			return 0
		}
		return uint64(value)
	case int32:
		if value < 0 {
			return 0
		}
		return uint64(value)
	case int16:
		if value < 0 {
			return 0
		}
		return uint64(value)
	case int8:
		if value < 0 {
			return 0
		}
		return uint64(value)
	case float64:
		if value < 0 {
			return 0
		}
		return uint64(value)
	case json.Number:
		i, err := value.Int64()
		if err != nil || i < 0 {
			return 0
		}
		return uint64(i)
	case string:
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return 0
		}
		return parsed
	default:
		return 0
	}
}

// Record is one line written to the JSONL file.
type Record struct {
	Time      string      `json:"time"`
	Failpoint string      `json:"failpoint"`
	Rows      []RowRecord `json:"rows"`
}

var (
	initOnce sync.Once
	mu       sync.Mutex
	file     *os.File
	disabled bool
)

func ensureFile() {
	initOnce.Do(func() {
		path := os.Getenv(envKey)
		if path == "" {
			disabled = true
			return
		}
		var err error
		file, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Warn("failed to open failpoint record file, recording disabled",
				zap.String("path", path), zap.Error(err))
			disabled = true
			return
		}
		log.Info("failpoint record file opened", zap.String("path", path))
	})
}

// Write persists one failpoint event to the JSONL file.
// It is safe for concurrent use.
// If the env var is not set the call is a no-op (zero allocation).
func Write(failpoint string, rows []RowRecord) {
	if disabled {
		return
	}
	ensureFile()
	if file == nil {
		return
	}

	rec := Record{
		Time:      time.Now().UTC().Format(time.RFC3339Nano),
		Failpoint: failpoint,
		Rows:      rows,
	}
	data, err := json.Marshal(rec)
	if err != nil {
		log.Warn("failed to marshal failpoint record", zap.Error(err))
		return
	}
	data = append(data, '\n')

	mu.Lock()
	defer mu.Unlock()
	if _, err := file.Write(data); err != nil {
		log.Warn("failed to write failpoint record", zap.Error(err))
	}
}
