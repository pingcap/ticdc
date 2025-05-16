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

package common

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
)

// VerifyChecksum calculate the checksum value, and compare it with the expected one, return error if not identical.
func VerifyChecksum(event *commonEvent.DMLEvent, db *sql.DB) error {
	// if expected is 0, it means the checksum is not enabled, so we don't need to verify it.
	// the data maybe restored by br, and the checksum is not enabled, so no expected here.
	columns := event.TableInfo.GetColumns()
	event.Rewind()

	for {
		row, ok := event.GetNextRow()
		if !ok {
			break
		}
		if row.Checksum.Current != 0 {
			checksum, err := calculateChecksum(row.Row, columns)
			if err != nil {
				return errors.Trace(err)
			}
			if checksum != row.Checksum.Current {
				log.Error("current checksum mismatch",
					zap.Uint32("expected", row.Checksum.Current), zap.Uint32("actual", checksum), zap.Any("event", event))
				for _, col := range columns {
					log.Info("data corrupted, print each column for debugging",
						zap.String("name", col.Name.O), zap.Any("type", col.GetType()),
						zap.Any("charset", col.GetCharset()), zap.Any("flag", col.GetFlag()),
						zap.Any("default", col.GetDefaultValue()))
				}
				return fmt.Errorf("current checksum mismatch, current: %d, expected: %d", checksum, row.Checksum.Current)
			}
		}
		if row.Checksum.Previous != 0 {
			checksum, err := calculateChecksum(row.PreRow, columns)
			if err != nil {
				return errors.Trace(err)
			}
			if checksum != row.Checksum.Previous {
				log.Error("previous checksum mismatch",
					zap.Uint32("expected", row.Checksum.Previous),
					zap.Uint32("actual", checksum), zap.Any("event", event))
				for _, col := range columns {
					log.Info("data corrupted, print each column for debugging",
						zap.String("name", col.Name.O), zap.Any("type", col.GetType()),
						zap.Any("charset", col.GetCharset()), zap.Any("flag", col.GetFlag()),
						zap.Any("default", col.GetDefaultValue()))
				}
				return fmt.Errorf("previous checksum mismatch, current: %d, expected: %d", checksum, row.Checksum.Previous)
			}
		}
	}
	if db == nil {
		return nil
	}
	// also query the upstream TiDB to get the columns-level checksum
	return queryRowChecksum(context.Background(), db, event)
}

// calculate the checksum, caller should make sure all columns is ordered by the column's id.
// by follow: https://github.com/pingcap/tidb/blob/e3417913f58cdd5a136259b902bf177eaf3aa637/util/rowcodec/common.go#L294
func calculateChecksum(row chunk.Row, columnInfo []*model.ColumnInfo) (uint32, error) {
	var (
		checksum uint32
		err      error
	)
	buf := make([]byte, 0)
	for idx, col := range columnInfo {
		if len(buf) > 0 {
			buf = buf[:0]
		}
		// buf = row.GetRaw(idx)
		buf, err = buildChecksumBytes(buf, row, idx, col)
		if err != nil {
			return 0, errors.Trace(err)
		}
		checksum = crc32.Update(checksum, crc32.IEEETable, buf)
	}
	return checksum, nil
}

// buildChecksumBytes append value to the buf, mysqlType is used to convert value interface to concrete type.
// by follow: https://github.com/pingcap/tidb/blob/e3417913f58cdd5a136259b902bf177eaf3aa637/util/rowcodec/common.go#L308
func buildChecksumBytes(buf []byte, row chunk.Row, idx int, col *model.ColumnInfo) ([]byte, error) {
	if row.IsNull(idx) {
		return buf, nil
	}
	d := row.GetDatum(idx, &col.FieldType)
	switch col.GetType() {
	// TypeTiny, TypeShort, TypeInt32 is encoded as int32
	// TypeLong is encoded as int32 if signed, else int64.
	// TypeLongLong is encoded as int64 if signed, else uint64,
	// if bigintUnsignedHandlingMode set as string, encode as string.
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24, mysql.TypeYear:
		buf = binary.LittleEndian.AppendUint64(buf, d.GetUint64())
	case mysql.TypeFloat, mysql.TypeDouble:
		v := d.GetFloat64()
		if math.IsInf(v, 0) || math.IsNaN(v) {
			v = 0
		}
		buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(v))
	case mysql.TypeEnum:
		buf = binary.LittleEndian.AppendUint64(buf, d.GetMysqlEnum().Value)
	case mysql.TypeSet:
		buf = binary.LittleEndian.AppendUint64(buf, d.GetMysqlSet().Value)
	case mysql.TypeBit:
		number := MustBinaryLiteralToInt(row.GetBytes(idx))
		buf = binary.LittleEndian.AppendUint64(buf, number)
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob,
		mysql.TypeDatetime, mysql.TypeDate, mysql.TypeTimestamp, mysql.TypeNewDate, mysql.TypeDuration,
		mysql.TypeNewDecimal, mysql.TypeJSON, mysql.TypeTiDBVectorFloat32:
		buf = appendLengthValue(buf, UnsafeStringToBytes(fmt.Sprintf("%v", d.GetValue())))
	case mysql.TypeNull, mysql.TypeGeometry:
		// do nothing
	default:
		return buf, errors.New("invalid type for the checksum calculation")
	}
	return buf, nil
}

func appendLengthValue(buf []byte, val []byte) []byte {
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(val)))
	buf = append(buf, val...)
	return buf
}
