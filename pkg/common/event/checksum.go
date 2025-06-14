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

package event

import (
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"go.uber.org/zap"
)

// return error when calculate the checksum.
// return false if the checksum is not matched
func (m *mounter) verifyChecksum(
	tableInfo *common.TableInfo, row chunk.Row, key kv.Key, handle kv.Handle, decoder *rowcodec.ChunkDecoder, isPreRow bool,
) (uint32, bool, error) {
	if !m.integrity.Enabled() {
		return 0, true, nil
	}
	columnInfos := tableInfo.GetColumns()
	version := decoder.ChecksumVersion()
	switch version {
	case 0:
		// skip old value checksum verification for the checksum v1, since it cannot handle
		// Update / Delete event correctly, after Add Column / Drop column DDL,
		// since the table schema does not contain complete column information.
		return m.verifyColumnChecksum(columnInfos, row, decoder, isPreRow)
	case 1, 2:
		expected, matched, err := verifyRawBytesChecksum(columnInfos, row, decoder, key, handle, m.tz)
		if err != nil {
			log.Error("calculate raw checksum failed",
				zap.Int("version", version), zap.Any("tz", m.tz), zap.Any("handle", handle.String()),
				zap.Any("key", key), zap.Any("columns", columnInfos), zap.Error(err))
			return 0, false, errors.Trace(err)
		}
		if !matched {
			return expected, matched, err
		}
		columnChecksum, err := calculateColumnChecksum(columnInfos, row, m.tz)
		if err != nil {
			log.Error("failed to calculate column-level checksum, after raw checksum verification passed",
				zap.Any("columnsInfo", columnInfos),
				zap.Any("tz", m.tz), zap.Error(err))
			return 0, false, errors.Trace(err)
		}
		return columnChecksum, true, nil
	default:
	}
	return 0, false, errors.Errorf("unknown checksum version %d", version)
}

func (m *mounter) verifyColumnChecksum(
	columnInfos []*model.ColumnInfo, row chunk.Row,
	decoder *rowcodec.ChunkDecoder, skipFail bool,
) (uint32, bool, error) {
	// if the checksum cannot be found, which means the upstream TiDB checksum is not enabled,
	// so return matched as true to skip check the event.
	first, ok := decoder.GetChecksum()
	if !ok {
		return 0, true, nil
	}

	checksum, err := calculateColumnChecksum(columnInfos, row, m.tz)
	if err != nil {
		log.Error("failed to calculate the checksum",
			zap.Uint32("first", first), zap.Any("columnInfos", columnInfos), zap.Error(err))
		return 0, false, err
	}

	// the first checksum matched, it hits in the most case.
	if checksum == first {
		return checksum, true, nil
	}

	extra, ok := decoder.GetExtraChecksum()
	if ok && checksum == extra {
		return checksum, true, nil
	}

	if !skipFail {
		log.Error("cannot found the extra checksum, the first checksum mismatched",
			zap.Uint32("checksum", checksum), zap.Uint32("first", first), zap.Uint32("extra", extra),
			zap.Any("columnInfos", columnInfos), zap.Any("tz", m.tz))
		return checksum, false, nil
	}

	if time.Since(m.lastSkipOldValueTime) > time.Minute {
		log.Warn("checksum mismatch on the old value, "+
			"this may caused by Add Column / Drop Column executed, skip verification",
			zap.Uint32("checksum", checksum), zap.Uint32("first", first), zap.Uint32("extra", extra),
			zap.Any("columnInfos", columnInfos), zap.Any("tz", m.tz))
		m.lastSkipOldValueTime = time.Now()
	}
	return checksum, true, nil
}

func calculateColumnChecksum(
	columnInfos []*model.ColumnInfo, row chunk.Row, tz *time.Location,
) (uint32, error) {
	columns := make([]rowcodec.ColData, 0, row.Len())
	for idx, col := range columnInfos {
		datum := row.GetDatum(idx, &col.FieldType)
		column := rowcodec.ColData{
			ColumnInfo: col,
			Datum:      &datum,
		}
		columns = append(columns, column)
	}

	calculator := rowcodec.RowData{
		Cols: columns,
		Data: make([]byte, 0),
	}

	checksum, err := calculator.Checksum(tz)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return checksum, nil
}

func verifyRawBytesChecksum(
	columnInfos []*model.ColumnInfo, row chunk.Row, decoder *rowcodec.ChunkDecoder, key kv.Key, handle kv.Handle, tz *time.Location,
) (uint32, bool, error) {
	expected, ok := decoder.GetChecksum()
	if !ok {
		return 0, true, nil
	}
	var (
		columnIDs []int64
		datums    []*types.Datum
	)
	for idx, col := range columnInfos {
		// TiDB does not encode null value into the bytes, so just ignore it.
		if row.IsNull(idx) {
			continue
		}
		columnID := col.ID
		datum := row.GetDatum(idx, &col.FieldType)
		datums = append(datums, &datum)
		columnIDs = append(columnIDs, columnID)
	}
	obtained, err := decoder.CalculateRawChecksum(tz, columnIDs, datums, key, handle, nil)
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	if obtained == expected {
		return expected, true, nil
	}

	log.Error("raw bytes checksum mismatch",
		zap.Int("version", decoder.ChecksumVersion()),
		zap.Uint32("expected", expected),
		zap.Uint32("obtained", obtained),
		zap.Any("columns", columnInfos),
		zap.Any("tz", tz))

	return expected, false, nil
}
