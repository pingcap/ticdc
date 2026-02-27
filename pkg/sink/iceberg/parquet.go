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

package iceberg

import (
	"bytes"
	"encoding/base64"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/pingcap/ticdc/pkg/common"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

func encodeParquetRows(tableInfo *common.TableInfo, emitMetadata bool, rows []ChangeRow) ([]byte, error) {
	if tableInfo == nil {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("table info is nil")
	}
	if len(rows) == 0 {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("rows are empty")
	}

	mem := memory.DefaultAllocator

	type encoder struct {
		name   string
		isMeta bool
		ft     *types.FieldType
		mapped mappedColumnType
		b      array.Builder
		append func(row ChangeRow) error
	}

	fields := make([]arrow.Field, 0, len(tableInfo.GetColumns())+3)
	encoders := make([]encoder, 0, len(tableInfo.GetColumns())+3)

	appendField := func(id int, name string, isMeta bool, ft *types.FieldType) error {
		var mapped mappedColumnType
		if isMeta {
			switch name {
			case "_tidb_op":
				mapped = mappedColumnType{icebergType: "string", arrowType: arrow.BinaryTypes.String}
			case "_tidb_commit_ts":
				mapped = mappedColumnType{icebergType: "long", arrowType: arrow.PrimitiveTypes.Int64}
			case "_tidb_commit_time":
				mapped = mappedColumnType{icebergType: "timestamp", arrowType: &arrow.TimestampType{Unit: arrow.Microsecond}}
			default:
				mapped = mappedColumnType{icebergType: "string", arrowType: arrow.BinaryTypes.String}
			}
		} else {
			mapped = mapTiDBFieldType(ft)
		}

		fields = append(fields, arrow.Field{
			Name:     name,
			Type:     mapped.arrowType,
			Nullable: true,
			Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{strconv.Itoa(id)}),
		})

		b := array.NewBuilder(mem, mapped.arrowType)

		var appendRow func(row ChangeRow) error
		if isMeta {
			switch name {
			case "_tidb_op":
				sb, ok := b.(*array.StringBuilder)
				if !ok {
					return cerror.ErrSinkURIInvalid.GenWithStackByArgs("unexpected builder type for _tidb_op")
				}
				appendRow = func(row ChangeRow) error {
					if strings.TrimSpace(row.Op) == "" {
						sb.AppendNull()
						return nil
					}
					sb.Append(row.Op)
					return nil
				}
			case "_tidb_commit_ts":
				ib, ok := b.(*array.Int64Builder)
				if !ok {
					return cerror.ErrSinkURIInvalid.GenWithStackByArgs("unexpected builder type for _tidb_commit_ts")
				}
				appendRow = func(row ChangeRow) error {
					return appendInt64FromUintString(ib, row.CommitTs)
				}
			case "_tidb_commit_time":
				tb, ok := b.(*array.TimestampBuilder)
				if !ok {
					return cerror.ErrSinkURIInvalid.GenWithStackByArgs("unexpected builder type for _tidb_commit_time")
				}
				appendRow = func(row ChangeRow) error {
					return appendTimestampMicrosFromString(tb, row.CommitTime)
				}
			default:
				sb, ok := b.(*array.StringBuilder)
				if !ok {
					return cerror.ErrSinkURIInvalid.GenWithStackByArgs("unexpected builder type for iceberg meta field")
				}
				appendRow = func(row ChangeRow) error {
					sb.AppendNull()
					return nil
				}
			}
		} else {
			appendVal, err := newValueAppender(b, ft, mapped)
			if err != nil {
				return err
			}
			appendRow = func(row ChangeRow) error {
				return appendVal(row.Columns[name])
			}
		}

		encoders = append(encoders, encoder{
			name:   name,
			isMeta: isMeta,
			ft:     ft,
			mapped: mapped,
			b:      b,
			append: appendRow,
		})
		return nil
	}

	if emitMetadata {
		if err := appendField(icebergMetaIDOp, "_tidb_op", true, nil); err != nil {
			return nil, err
		}
		if err := appendField(icebergMetaIDCommitTs, "_tidb_commit_ts", true, nil); err != nil {
			return nil, err
		}
		if err := appendField(icebergMetaIDCommitTime, "_tidb_commit_time", true, nil); err != nil {
			return nil, err
		}
	}

	for _, colInfo := range tableInfo.GetColumns() {
		if colInfo == nil || colInfo.IsVirtualGenerated() {
			continue
		}
		ft := &colInfo.FieldType
		if err := appendField(int(colInfo.ID), colInfo.Name.O, false, ft); err != nil {
			return nil, err
		}
	}

	schema := arrow.NewSchema(fields, nil)

	for _, row := range rows {
		for i := range encoders {
			if err := encoders[i].append(row); err != nil {
				for _, e := range encoders {
					e.b.Release()
				}
				return nil, err
			}
		}
	}

	cols := make([]arrow.Array, 0, len(encoders))
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()
	for _, e := range encoders {
		cols = append(cols, e.b.NewArray())
		e.b.Release()
	}

	rec := array.NewRecord(schema, cols, int64(len(rows)))
	defer rec.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer tbl.Release()

	var buf bytes.Buffer
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd))
	if err := pqarrow.WriteTable(tbl, &buf, int64(len(rows)), props, pqarrow.DefaultWriterProps()); err != nil {
		return nil, cerror.Trace(err)
	}
	return buf.Bytes(), nil
}

func newValueAppender(b array.Builder, ft *types.FieldType, mapped mappedColumnType) (func(v *string) error, error) {
	switch builder := b.(type) {
	case *array.StringBuilder:
		return func(v *string) error {
			if v == nil {
				builder.AppendNull()
				return nil
			}
			builder.Append(*v)
			return nil
		}, nil
	case *array.Int32Builder:
		return func(v *string) error {
			if v == nil {
				builder.AppendNull()
				return nil
			}
			n, err := strconv.ParseInt(*v, 10, 32)
			if err != nil {
				return err
			}
			builder.Append(int32(n))
			return nil
		}, nil
	case *array.Int64Builder:
		return func(v *string) error {
			if v == nil {
				builder.AppendNull()
				return nil
			}
			if ft != nil && ft.GetType() == mysql.TypeBit {
				u, err := strconv.ParseUint(*v, 10, 64)
				if err != nil {
					return err
				}
				if u > uint64(math.MaxInt64) {
					return cerror.ErrSinkURIInvalid.GenWithStackByArgs("bit value overflows int64")
				}
				builder.Append(int64(u))
				return nil
			}

			if ft != nil && mysql.HasUnsignedFlag(ft.GetFlag()) {
				u, err := strconv.ParseUint(*v, 10, 64)
				if err != nil {
					return err
				}
				if u > uint64(math.MaxInt64) {
					return cerror.ErrSinkURIInvalid.GenWithStackByArgs("unsigned value overflows int64")
				}
				builder.Append(int64(u))
				return nil
			}

			n, err := strconv.ParseInt(*v, 10, 64)
			if err != nil {
				return err
			}
			builder.Append(n)
			return nil
		}, nil
	case *array.Float32Builder:
		return func(v *string) error {
			if v == nil {
				builder.AppendNull()
				return nil
			}
			f, err := strconv.ParseFloat(*v, 32)
			if err != nil {
				return err
			}
			builder.Append(float32(f))
			return nil
		}, nil
	case *array.Float64Builder:
		return func(v *string) error {
			if v == nil {
				builder.AppendNull()
				return nil
			}
			f, err := strconv.ParseFloat(*v, 64)
			if err != nil {
				return err
			}
			builder.Append(f)
			return nil
		}, nil
	case *array.Decimal128Builder:
		decimalType, ok := mapped.arrowType.(*arrow.Decimal128Type)
		if !ok {
			return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("decimal builder has unexpected arrow type")
		}
		return func(v *string) error {
			if v == nil {
				builder.AppendNull()
				return nil
			}
			n, err := decimal128.FromString(*v, decimalType.Precision, decimalType.Scale)
			if err != nil {
				return err
			}
			builder.Append(n)
			return nil
		}, nil
	case *array.BinaryBuilder:
		return func(v *string) error {
			if v == nil {
				builder.AppendNull()
				return nil
			}
			decoded, err := base64.StdEncoding.DecodeString(*v)
			if err != nil {
				return err
			}
			builder.Append(decoded)
			return nil
		}, nil
	case *array.Date32Builder:
		return func(v *string) error {
			if v == nil {
				builder.AppendNull()
				return nil
			}
			t, err := time.ParseInLocation("2006-01-02", *v, time.UTC)
			if err != nil {
				return err
			}
			builder.Append(arrow.Date32(t.UTC().Unix() / 86400))
			return nil
		}, nil
	case *array.TimestampBuilder:
		return func(v *string) error {
			if v == nil {
				builder.AppendNull()
				return nil
			}

			parsed, err := parseMySQLTimestampString(*v)
			if err != nil {
				return err
			}
			builder.Append(arrow.Timestamp(parsed.UnixNano() / 1000))
			return nil
		}, nil
	default:
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("unsupported arrow builder type")
	}
}

func appendInt64FromUintString(b *array.Int64Builder, raw string) error {
	if b == nil {
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs("int64 builder is nil")
	}
	if strings.TrimSpace(raw) == "" {
		b.AppendNull()
		return nil
	}
	u, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return cerror.Trace(err)
	}
	if u > uint64(math.MaxInt64) {
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs("uint64 value overflows int64")
	}
	b.Append(int64(u))
	return nil
}

func appendTimestampMicrosFromString(b *array.TimestampBuilder, raw string) error {
	if b == nil {
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs("timestamp builder is nil")
	}
	if strings.TrimSpace(raw) == "" {
		b.AppendNull()
		return nil
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return cerror.Trace(err)
	}
	b.Append(arrow.Timestamp(t.UTC().UnixNano() / 1000))
	return nil
}

func parseMySQLTimestampString(raw string) (time.Time, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return time.Time{}, cerror.ErrSinkURIInvalid.GenWithStackByArgs("timestamp string is empty")
	}

	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC(), nil
	}

	layouts := []string{
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05.999",
		"2006-01-02 15:04:05",
	}
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, cerror.ErrSinkURIInvalid.GenWithStackByArgs("unsupported timestamp format")
}
