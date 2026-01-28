// Copyright 2026 PingCAP, Inc.
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

package parser

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/utils"
	"github.com/pingcap/ticdc/pkg/common"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"go.uber.org/zap"
)

func getPkColumnOffset(tableInfo *commonType.TableInfo) (map[int64]int, error) {
	if tableInfo.PKIsHandle() {
		pkColInfo := tableInfo.GetPkColInfo()
		if pkColInfo == nil {
			return nil, errors.Errorf("table %s has no primary key", tableInfo.GetTableName())
		}
		return map[int64]int{pkColInfo.ID: 0}, nil
	}

	pkColInfos := tableInfo.GetPrimaryKeyColumnInfos()
	if len(pkColInfos) == 0 {
		return nil, errors.Errorf("table %s has no primary key", tableInfo.GetTableName())
	}

	columns := tableInfo.GetColumns()
	pkColumnOffsets := make(map[int64]int)
	for i, pkColInfo := range pkColInfos {
		if pkColInfo.Offset < 0 || pkColInfo.Offset >= len(columns) {
			return nil, errors.Errorf("primary key column offset (%d) out of range for column (%d) in table %s", pkColInfo.Offset, len(columns), tableInfo.GetTableName())
		}
		pkColumnOffsets[columns[pkColInfo.Offset].ID] = i
	}
	return pkColumnOffsets, nil
}

type TableParser struct {
	tableKey        string
	tableInfo       *common.TableInfo
	pkColumnOffsets map[int64]int
	csvDecoder      *csvDecoder
}

func NewTableParser(tableKey string, content []byte) (*TableParser, error) {
	tableParser := &TableParser{}
	if err := tableParser.parseTableInfo(tableKey, content); err != nil {
		return nil, errors.Trace(err)
	}
	tableParser.csvDecoder = NewCsvDecoder()
	return tableParser, nil
}

func (pt *TableParser) parseTableInfo(tableKey string, content []byte) error {
	// Parse schema content to get tableInfo
	var tableDef cloudstorage.TableDefinition
	if err := json.Unmarshal(content, &tableDef); err != nil {
		log.Error("failed to unmarshal schema content",
			zap.String("tableKey", tableKey),
			zap.ByteString("content", content),
			zap.Error(err))
		return errors.Trace(err)
	}

	tableInfo, err := tableDef.ToTableInfo()
	if err != nil {
		log.Error("failed to convert table definition to table info",
			zap.String("tableKey", tableKey),
			zap.ByteString("content", content),
			zap.Error(err))
		return errors.Trace(err)
	}

	pkColumnOffsets, err := getPkColumnOffset(tableInfo)
	if err != nil {
		log.Error("failed to get primary key column offsets",
			zap.String("tableKey", tableKey),
			zap.ByteString("content", content),
			zap.Error(err))
		return errors.Annotate(err, "failed to get primary key column offsets")
	}

	pt.tableKey = tableKey
	pt.tableInfo = tableInfo
	pt.pkColumnOffsets = pkColumnOffsets
	return nil
}

func (pt *TableParser) parseRecord(row *chunk.Row, commitTs uint64) (*utils.Record, error) {
	originTs := uint64(0)
	pkCount := 0
	colInfos := pt.tableInfo.GetColInfosForRowChangedEvent()
	columnValues := make([]utils.ColumnValue, 0, len(colInfos))
	pkColumnValues := make([]types.Datum, len(pt.pkColumnOffsets))
	for _, colInfo := range colInfos {
		col, ok := pt.tableInfo.GetColumnInfo(colInfo.ID)
		if !ok {
			log.Error("column info not found",
				zap.String("tableKey", pt.tableKey),
				zap.Int64("colID", colInfo.ID))
			return nil, errors.Errorf("column info not found for column %d in table %s", colInfo.ID, pt.tableKey)
		}
		rowColOffset, ok := pt.tableInfo.GetRowColumnsOffset()[colInfo.ID]
		if !ok {
			log.Error("row column offset not found",
				zap.String("tableKey", pt.tableKey),
				zap.Int64("colID", colInfo.ID))
			return nil, errors.Errorf("row column offset not found for column %d in table %s", colInfo.ID, pt.tableKey)
		}
		if offset, ok := pt.pkColumnOffsets[colInfo.ID]; ok {
			dt := row.GetDatum(rowColOffset, &col.FieldType)
			if !pkColumnValues[offset].IsNull() {
				log.Error("duplicated primary key column value",
					zap.String("tableKey", pt.tableKey),
					zap.Int64("colID", colInfo.ID))
				return nil, errors.Errorf("duplicated primary key column value for column %d in table %s", colInfo.ID, pt.tableKey)
			}
			pkColumnValues[offset] = dt
			pkCount += 1
			continue
		}
		if col.Name.O == event.OriginTsColumn {
			if !row.IsNull(rowColOffset) {
				d := row.GetDatum(rowColOffset, &col.FieldType)
				if d.Kind() != types.KindInt64 && d.Kind() != types.KindUint64 {
					log.Error("origin ts column value is not int64 or uint64",
						zap.String("tableKey", pt.tableKey),
						zap.String("datum", d.String()))
					return nil, errors.Errorf("origin ts column value is not int64 or uint64 for column %d in table %s", colInfo.ID, pt.tableKey)
				}
				originTs = d.GetUint64()
			}
		} else {
			colValue := commonType.ExtractColVal(row, col, rowColOffset)
			columnValues = append(columnValues, utils.ColumnValue{
				ColumnID: colInfo.ID,
				Value:    colValue,
			})
		}
	}
	if pkCount != len(pt.pkColumnOffsets) {
		log.Error("primary key column value missing",
			zap.String("tableKey", pt.tableKey),
			zap.Int("pkCount", pkCount),
			zap.Int("len(pt.pkColumnOffsets)", len(pt.pkColumnOffsets)))
		return nil, errors.Errorf("primary key column value is null for table %s", pt.tableKey)
	}
	pkEncoded, err := codec.EncodeKey(time.UTC, nil, pkColumnValues...)
	if err != nil {
		return nil, errors.Annotate(err, "failed to encode primary key")
	}
	pk := hex.EncodeToString(pkEncoded)
	return &utils.Record{
		Pk:           utils.PkType(pk),
		ColumnValues: columnValues,
		CdcVersion: utils.CdcVersion{
			CommitTs: commitTs,
			OriginTs: originTs,
		},
	}, nil
}

func (pt *TableParser) DecodeFiles(ctx context.Context, content []byte) ([]*utils.Record, error) {
	records := make([]*utils.Record, 0)

	decoder, err := pt.csvDecoder.NewDecoder(ctx, pt.tableInfo, content)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for {
		msgType, hasNext := decoder.HasNext()
		if !hasNext {
			break
		}
		if msgType != codecCommon.MessageTypeRow {
			continue
		}
		dmlEvent := decoder.NextDMLEvent()
		if dmlEvent == nil || dmlEvent.Rows == nil || dmlEvent.Rows.NumRows() == 0 {
			continue
		}
		row := dmlEvent.Rows.GetRow(0)
		record, err := pt.parseRecord(&row, dmlEvent.CommitTs)
		if err != nil {
			return nil, errors.Trace(err)
		}
		records = append(records, record)
	}
	return records, nil
}
