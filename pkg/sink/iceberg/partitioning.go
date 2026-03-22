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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	cerror "github.com/pingcap/ticdc/pkg/errors"
)

type resolvedPartitionSpec struct {
	spec                  partitionSpec
	lastPartitionID       int
	fields                []resolvedPartitionField
	partitionSpecJSON     []byte
	manifestEntrySchemaV2 string
}

type resolvedPartitionField struct {
	name          string
	fieldID       int
	sourceName    string
	sourceID      int
	sourceType    string
	transform     string
	resultType    string
	avroUnionType string
	numBuckets    int
	truncateWidth int
}

type partitionGroup struct {
	partition map[string]any
	rows      []ChangeRow
}

func resolvePartitionSpec(cfg *Config, schema *icebergSchema) (*resolvedPartitionSpec, error) {
	if cfg == nil {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("iceberg config is nil")
	}
	if schema == nil {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("iceberg schema is nil")
	}

	expr := strings.TrimSpace(cfg.Partitioning)
	if expr == "" || isUnpartitionedExpr(expr) {
		spec := partitionSpec{SpecID: icebergPartitionSpecID, Fields: []any{}}
		specJSON, err := json.Marshal(spec)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}
		return &resolvedPartitionSpec{
			spec:                  spec,
			lastPartitionID:       icebergLastPartitionIDUnpartitioned,
			fields:                nil,
			partitionSpecJSON:     specJSON,
			manifestEntrySchemaV2: manifestEntrySchemaV2Unpartitioned,
		}, nil
	}

	transforms, err := parsePartitionTransforms(expr)
	if err != nil {
		return nil, err
	}

	fieldsByName := make(map[string]icebergField, len(schema.Fields))
	for _, f := range schema.Fields {
		fieldsByName[f.Name] = f
	}

	resolvedFields := make([]resolvedPartitionField, 0, len(transforms))
	specFields := make([]any, 0, len(transforms))
	manifestFields := make([]manifestPartitionField, 0, len(transforms))
	usedNames := make(map[string]struct{}, len(transforms))

	nextFieldID := 1000
	for _, tr := range transforms {
		src, ok := fieldsByName[tr.sourceName]
		if !ok {
			return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs(fmt.Sprintf("partition source column not found: %s", tr.sourceName))
		}
		transform, transformLabel, resultType, err := resolvePartitionTransform(tr, src.Type)
		if err != nil {
			return nil, err
		}

		name := sanitizePartitionFieldName(tr.sourceName + "_" + transformLabel)
		if _, ok := usedNames[name]; ok {
			return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs(fmt.Sprintf("duplicate partition field name: %s", name))
		}
		usedNames[name] = struct{}{}

		fieldID := nextFieldID
		nextFieldID++

		avroUnionType := avroUnionTypeNameForIcebergType(resultType)
		if avroUnionType == "" {
			return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs(fmt.Sprintf("unsupported partition field type: %s", resultType))
		}

		resolvedFields = append(resolvedFields, resolvedPartitionField{
			name:          name,
			fieldID:       fieldID,
			sourceName:    tr.sourceName,
			sourceID:      src.ID,
			sourceType:    src.Type,
			transform:     transform,
			resultType:    resultType,
			avroUnionType: avroUnionType,
			numBuckets:    tr.numBuckets,
			truncateWidth: tr.truncateWidth,
		})
		specFields = append(specFields, map[string]any{
			"name":      name,
			"transform": transform,
			"source-id": src.ID,
			"field-id":  fieldID,
		})
		manifestFields = append(manifestFields, manifestPartitionField{
			Name:     name,
			FieldID:  fieldID,
			AvroType: icebergTypeToAvroType(resultType),
		})
	}

	spec := partitionSpec{SpecID: icebergPartitionSpecID, Fields: specFields}
	specJSON, err := json.Marshal(spec)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	manifestSchema, err := buildManifestEntrySchemaV2(manifestFields)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}

	lastPartitionID := icebergLastPartitionIDUnpartitioned
	if len(resolvedFields) > 0 {
		lastPartitionID = resolvedFields[len(resolvedFields)-1].fieldID
	}

	return &resolvedPartitionSpec{
		spec:                  spec,
		lastPartitionID:       lastPartitionID,
		fields:                resolvedFields,
		partitionSpecJSON:     specJSON,
		manifestEntrySchemaV2: manifestSchema,
	}, nil
}

func isUnpartitionedExpr(expr string) bool {
	switch strings.ToLower(strings.TrimSpace(expr)) {
	case "none", "unpartitioned", "false", "off":
		return true
	default:
		return false
	}
}

type parsedPartitionTransform struct {
	transformName string
	sourceName    string
	numBuckets    int
	truncateWidth int
}

func parsePartitionTransforms(expr string) ([]parsedPartitionTransform, error) {
	parts := splitPartitioningList(expr)
	out := make([]parsedPartitionTransform, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if !strings.Contains(part, "(") {
			out = append(out, parsedPartitionTransform{
				transformName: "identity",
				sourceName:    part,
			})
			continue
		}
		name, args, err := parseTransformCall(part)
		if err != nil {
			return nil, err
		}
		switch name {
		case "identity":
			if len(args) != 1 {
				return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("identity transform requires 1 argument")
			}
			out = append(out, parsedPartitionTransform{
				transformName: "identity",
				sourceName:    args[0],
			})
		case "day", "days":
			if len(args) != 1 {
				return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("days transform requires 1 argument")
			}
			out = append(out, parsedPartitionTransform{
				transformName: "days",
				sourceName:    args[0],
			})
		case "year", "years":
			if len(args) != 1 {
				return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("years transform requires 1 argument")
			}
			out = append(out, parsedPartitionTransform{
				transformName: "years",
				sourceName:    args[0],
			})
		case "month", "months":
			if len(args) != 1 {
				return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("months transform requires 1 argument")
			}
			out = append(out, parsedPartitionTransform{
				transformName: "months",
				sourceName:    args[0],
			})
		case "hour", "hours":
			if len(args) != 1 {
				return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("hours transform requires 1 argument")
			}
			out = append(out, parsedPartitionTransform{
				transformName: "hours",
				sourceName:    args[0],
			})
		case "bucket":
			if len(args) != 2 {
				return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("bucket transform requires 2 arguments")
			}
			n, err := strconv.Atoi(args[1])
			if err != nil {
				return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
			}
			if n <= 0 {
				return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("bucket transform requires a positive bucket count")
			}
			out = append(out, parsedPartitionTransform{
				transformName: "bucket",
				sourceName:    args[0],
				numBuckets:    n,
			})
		case "truncate":
			if len(args) != 2 {
				return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("truncate transform requires 2 arguments")
			}
			n, err := strconv.Atoi(args[1])
			if err != nil {
				return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
			}
			if n <= 0 {
				return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("truncate transform requires a positive width")
			}
			out = append(out, parsedPartitionTransform{
				transformName: "truncate",
				sourceName:    args[0],
				truncateWidth: n,
				numBuckets:    0,
			})
		default:
			return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs(fmt.Sprintf("unsupported partition transform: %s", name))
		}
	}
	return out, nil
}

func resolvePartitionTransform(tr parsedPartitionTransform, sourceIcebergType string) (transform string, transformLabel string, resultType string, _ error) {
	switch strings.ToLower(strings.TrimSpace(tr.transformName)) {
	case "identity":
		return "identity", "identity", sourceIcebergType, nil
	case "days":
		switch sourceIcebergType {
		case "timestamp", "date":
			return "day", "day", "int", nil
		default:
			return "", "", "", cerror.ErrSinkURIInvalid.GenWithStackByArgs("days transform requires a timestamp or date source column")
		}
	case "bucket":
		switch sourceIcebergType {
		case "int", "long", "string", "binary":
		default:
			if _, _, ok := parseDecimalType(sourceIcebergType); !ok {
				return "", "", "", cerror.ErrSinkURIInvalid.GenWithStackByArgs("bucket transform requires an int, long, string, binary, or decimal source column")
			}
		}
		if tr.numBuckets <= 0 {
			return "", "", "", cerror.ErrSinkURIInvalid.GenWithStackByArgs("bucket transform requires a positive bucket count")
		}
		return fmt.Sprintf("bucket[%d]", tr.numBuckets), fmt.Sprintf("bucket_%d", tr.numBuckets), "int", nil
	case "years":
		switch sourceIcebergType {
		case "timestamp", "date":
			return "year", "year", "int", nil
		default:
			return "", "", "", cerror.ErrSinkURIInvalid.GenWithStackByArgs("years transform requires a timestamp or date source column")
		}
	case "months":
		switch sourceIcebergType {
		case "timestamp", "date":
			return "month", "month", "int", nil
		default:
			return "", "", "", cerror.ErrSinkURIInvalid.GenWithStackByArgs("months transform requires a timestamp or date source column")
		}
	case "hours":
		switch sourceIcebergType {
		case "timestamp":
			return "hour", "hour", "int", nil
		default:
			return "", "", "", cerror.ErrSinkURIInvalid.GenWithStackByArgs("hours transform requires a timestamp source column")
		}
	case "truncate":
		switch sourceIcebergType {
		case "int", "long", "string", "binary":
		default:
			if _, _, ok := parseDecimalType(sourceIcebergType); !ok {
				return "", "", "", cerror.ErrSinkURIInvalid.GenWithStackByArgs("truncate transform requires an int, long, string, binary, or decimal source column")
			}
		}
		if tr.truncateWidth <= 0 {
			return "", "", "", cerror.ErrSinkURIInvalid.GenWithStackByArgs("truncate transform requires a positive width")
		}
		return fmt.Sprintf("truncate[%d]", tr.truncateWidth), fmt.Sprintf("truncate_%d", tr.truncateWidth), sourceIcebergType, nil
	default:
		return "", "", "", cerror.ErrSinkURIInvalid.GenWithStackByArgs(fmt.Sprintf("unsupported partition transform: %s", tr.transformName))
	}
}

func (s *resolvedPartitionSpec) emptyPartitionRecord() map[string]any {
	if s == nil || len(s.fields) == 0 {
		return map[string]any{}
	}
	m := make(map[string]any, len(s.fields))
	for _, f := range s.fields {
		m[f.name] = nil
	}
	return m
}

func (s *resolvedPartitionSpec) groupRows(rows []ChangeRow) ([]partitionGroup, error) {
	if s == nil || len(s.fields) == 0 {
		return []partitionGroup{{partition: map[string]any{}, rows: rows}}, nil
	}
	if len(rows) == 0 {
		return nil, nil
	}

	groups := make(map[string]*partitionGroup)
	order := make([]string, 0, 8)
	for _, row := range rows {
		var keyBuilder strings.Builder
		values := make(map[string]any, len(s.fields))
		for _, f := range s.fields {
			raw, err := computePartitionValue(f, row)
			if err != nil {
				return nil, err
			}
			values[f.name] = wrapUnion(f.avroUnionType, raw)
			keyBuilder.WriteString(encodePartitionKeyPart(raw))
			keyBuilder.WriteByte(0x1f)
		}
		key := keyBuilder.String()
		g := groups[key]
		if g == nil {
			record := make(map[string]any, len(s.fields))
			for _, f := range s.fields {
				record[f.name] = values[f.name]
			}
			g = &partitionGroup{partition: record}
			groups[key] = g
			order = append(order, key)
		}
		g.rows = append(g.rows, row)
	}

	out := make([]partitionGroup, 0, len(groups))
	for _, key := range order {
		out = append(out, *groups[key])
	}
	return out, nil
}

func encodePartitionKeyPart(raw any) string {
	switch v := raw.(type) {
	case nil:
		return "n"
	case string:
		return "s:" + strconv.Itoa(len(v)) + ":" + v
	case []byte:
		enc := base64.StdEncoding.EncodeToString(v)
		return "b:" + strconv.Itoa(len(enc)) + ":" + enc
	default:
		s := fmt.Sprint(v)
		return "t:" + strconv.Itoa(len(s)) + ":" + s
	}
}

func (s *resolvedPartitionSpec) isSafeForEqualityDeletes(equalityFieldIDs []int) bool {
	if s == nil || len(s.fields) == 0 {
		return true
	}
	if len(equalityFieldIDs) == 0 {
		return false
	}
	eq := make(map[int]struct{}, len(equalityFieldIDs))
	for _, id := range equalityFieldIDs {
		eq[id] = struct{}{}
	}
	for _, f := range s.fields {
		if _, ok := eq[f.sourceID]; !ok {
			return false
		}
	}
	return true
}

func computePartitionValue(field resolvedPartitionField, row ChangeRow) (any, error) {
	switch field.transform {
	case "identity":
		raw, ok, err := getPartitionSourceValue(field.sourceName, row)
		if err != nil {
			return nil, err
		}
		if !ok || strings.TrimSpace(raw) == "" {
			return nil, nil
		}
		return parseIdentityPartitionValue(field.sourceType, raw, field.truncateWidth)
	case "day":
		raw, ok, err := getPartitionSourceValue(field.sourceName, row)
		if err != nil {
			return nil, err
		}
		if !ok || strings.TrimSpace(raw) == "" {
			return nil, nil
		}

		t, err := parsePartitionTimeValue(field.sourceType, raw)
		if err != nil {
			return nil, err
		}
		return int32(t.UTC().Unix() / 86400), nil
	case "year":
		raw, ok, err := getPartitionSourceValue(field.sourceName, row)
		if err != nil {
			return nil, err
		}
		if !ok || strings.TrimSpace(raw) == "" {
			return nil, nil
		}
		t, err := parsePartitionTimeValue(field.sourceType, raw)
		if err != nil {
			return nil, err
		}
		return int32(t.UTC().Year() - 1970), nil
	case "month":
		raw, ok, err := getPartitionSourceValue(field.sourceName, row)
		if err != nil {
			return nil, err
		}
		if !ok || strings.TrimSpace(raw) == "" {
			return nil, nil
		}
		t, err := parsePartitionTimeValue(field.sourceType, raw)
		if err != nil {
			return nil, err
		}
		year := t.UTC().Year() - 1970
		month := int(t.UTC().Month()) - 1
		return int32(year*12 + month), nil
	case "hour":
		raw, ok, err := getPartitionSourceValue(field.sourceName, row)
		if err != nil {
			return nil, err
		}
		if !ok || strings.TrimSpace(raw) == "" {
			return nil, nil
		}
		t, err := parsePartitionTimeValue(field.sourceType, raw)
		if err != nil {
			return nil, err
		}
		return int32(t.UTC().Unix() / 3600), nil
	default:
		if strings.HasPrefix(field.transform, "bucket[") && strings.HasSuffix(field.transform, "]") {
			if field.numBuckets <= 0 {
				return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("bucket transform requires a positive bucket count")
			}
			raw, ok, err := getPartitionSourceValue(field.sourceName, row)
			if err != nil {
				return nil, err
			}
			if !ok || strings.TrimSpace(raw) == "" {
				return nil, nil
			}

			data, err := bucketHashBytes(field.sourceType, raw)
			if err != nil {
				return nil, err
			}
			hash := int32(murmur3Sum32(data))
			n := field.numBuckets
			return int32(((int(hash) % n) + n) % n), nil
		}
		if strings.HasPrefix(field.transform, "truncate[") && strings.HasSuffix(field.transform, "]") {
			if field.truncateWidth <= 0 {
				return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("truncate transform requires a positive width")
			}
			raw, ok, err := getPartitionSourceValue(field.sourceName, row)
			if err != nil {
				return nil, err
			}
			if !ok || strings.TrimSpace(raw) == "" {
				return nil, nil
			}
			return truncatePartitionValue(field.sourceType, raw, field.truncateWidth)
		}
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("unsupported partition transform")
	}
}

func parsePartitionTimeValue(icebergType string, raw string) (time.Time, error) {
	switch icebergType {
	case "timestamp":
		return parseMySQLTimestampString(raw)
	case "date":
		t, err := time.ParseInLocation("2006-01-02", raw, time.UTC)
		if err != nil {
			return time.Time{}, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}
		return t, nil
	default:
		return time.Time{}, cerror.ErrSinkURIInvalid.GenWithStackByArgs("unsupported source type for time partition transform")
	}
}

func parseIdentityPartitionValue(icebergType string, raw string, _ int) (any, error) {
	switch icebergType {
	case "int":
		n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 32)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}
		return int32(n), nil
	case "long":
		n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}
		return int64(n), nil
	case "float":
		f, err := strconv.ParseFloat(strings.TrimSpace(raw), 32)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}
		return float32(f), nil
	case "double":
		f, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}
		return f, nil
	case "boolean":
		s := strings.ToLower(strings.TrimSpace(raw))
		switch s {
		case "1", "true":
			return true, nil
		case "0", "false":
			return false, nil
		default:
			v, err := strconv.ParseBool(s)
			if err != nil {
				return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
			}
			return v, nil
		}
	case "string":
		return raw, nil
	case "binary":
		decoded, err := base64.StdEncoding.DecodeString(raw)
		if err != nil {
			return []byte(raw), nil
		}
		return decoded, nil
	case "date":
		t, err := time.ParseInLocation("2006-01-02", raw, time.UTC)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}
		return int32(t.UTC().Unix() / 86400), nil
	case "timestamp":
		t, err := parseMySQLTimestampString(raw)
		if err != nil {
			return nil, err
		}
		return int64(t.UTC().UnixNano() / 1000), nil
	default:
		if _, scale, ok := parseDecimalType(icebergType); ok {
			unscaled, err := parseDecimalUnscaled(raw, scale)
			if err != nil {
				return nil, err
			}
			return twosComplementBytes(unscaled), nil
		}
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("unsupported type for identity transform")
	}
}

func truncatePartitionValue(icebergType string, raw string, width int) (any, error) {
	if width <= 0 {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("truncate width is invalid")
	}
	switch icebergType {
	case "int":
		n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 32)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}
		w := int32(width)
		if w == 0 {
			return int32(n), nil
		}
		v := int32(n)
		r := v % w
		if r < 0 {
			r += w
		}
		return v - r, nil
	case "long":
		n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}
		w := int64(width)
		if w == 0 {
			return int64(n), nil
		}
		v := int64(n)
		r := v % w
		if r < 0 {
			r += w
		}
		return v - r, nil
	case "string":
		rs := []rune(raw)
		if len(rs) <= width {
			return raw, nil
		}
		return string(rs[:width]), nil
	case "binary":
		b, err := base64.StdEncoding.DecodeString(raw)
		if err != nil {
			b = []byte(raw)
		}
		if len(b) <= width {
			return b, nil
		}
		return b[:width], nil
	default:
		if _, scale, ok := parseDecimalType(icebergType); ok {
			unscaled, err := parseDecimalUnscaled(raw, scale)
			if err != nil {
				return nil, err
			}
			widthUnscaled := new(big.Int).SetInt64(int64(width))
			if scale > 0 {
				mult := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
				widthUnscaled.Mul(widthUnscaled, mult)
			}
			if widthUnscaled.Sign() == 0 {
				return twosComplementBytes(unscaled), nil
			}
			rem := new(big.Int).Mod(unscaled, widthUnscaled)
			trunc := new(big.Int).Sub(unscaled, rem)
			return twosComplementBytes(trunc), nil
		}
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("unsupported source type for truncate transform")
	}
}

func bucketHashBytes(icebergType string, raw string) ([]byte, error) {
	switch icebergType {
	case "int":
		n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 32)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, uint32(int32(n)))
		return b, nil
	case "long":
		n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(n))
		return b, nil
	case "string":
		return []byte(raw), nil
	case "binary":
		decoded, err := base64.StdEncoding.DecodeString(raw)
		if err != nil {
			return []byte(raw), nil
		}
		return decoded, nil
	default:
		if _, scale, ok := parseDecimalType(icebergType); ok {
			unscaled, err := parseDecimalUnscaled(raw, scale)
			if err != nil {
				return nil, err
			}
			return twosComplementBytes(unscaled), nil
		}
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("unsupported source type for bucket transform")
	}
}

func parseDecimalType(icebergType string) (precision int, scale int, ok bool) {
	s := strings.TrimSpace(icebergType)
	if !strings.HasPrefix(s, "decimal(") || !strings.HasSuffix(s, ")") {
		return 0, 0, false
	}
	inner := strings.TrimSuffix(strings.TrimPrefix(s, "decimal("), ")")
	parts := strings.Split(inner, ",")
	if len(parts) != 2 {
		return 0, 0, false
	}
	prec, err1 := strconv.Atoi(strings.TrimSpace(parts[0]))
	sc, err2 := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err1 != nil || err2 != nil || prec <= 0 || sc < 0 || sc > prec {
		return 0, 0, false
	}
	return prec, sc, true
}

func parseDecimalUnscaled(raw string, scale int) (*big.Int, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("decimal string is empty")
	}

	neg := false
	if s[0] == '+' || s[0] == '-' {
		neg = s[0] == '-'
		s = strings.TrimSpace(s[1:])
	}
	if s == "" {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("decimal string is empty")
	}

	intPart := s
	fracPart := ""
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		intPart = s[:dot]
		fracPart = s[dot+1:]
	}
	if intPart == "" {
		intPart = "0"
	}
	if intPart != "0" && strings.HasPrefix(intPart, "0") {
		intPart = strings.TrimLeft(intPart, "0")
		if intPart == "" {
			intPart = "0"
		}
	}
	if !isDecimalDigits(intPart) || !isDecimalDigits(fracPart) {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("decimal string contains non digits")
	}

	if scale == 0 {
		if strings.TrimSpace(fracPart) != "" {
			for _, r := range fracPart {
				if r != '0' {
					return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("decimal has fractional part but scale is 0")
				}
			}
		}
		fracPart = ""
	} else {
		if len(fracPart) > scale {
			return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("decimal exceeds scale")
		}
		if len(fracPart) < scale {
			fracPart = fracPart + strings.Repeat("0", scale-len(fracPart))
		}
	}

	combined := intPart + fracPart
	combined = strings.TrimLeft(combined, "0")
	if combined == "" {
		combined = "0"
	}

	n := new(big.Int)
	if _, ok := n.SetString(combined, 10); !ok {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("failed to parse decimal number")
	}
	if neg && n.Sign() != 0 {
		n.Neg(n)
	}
	return n, nil
}

func isDecimalDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func twosComplementBytes(n *big.Int) []byte {
	if n == nil || n.Sign() == 0 {
		return []byte{0}
	}
	if n.Sign() > 0 {
		b := n.Bytes()
		if len(b) == 0 {
			return []byte{0}
		}
		if b[0]&0x80 != 0 {
			out := make([]byte, len(b)+1)
			copy(out[1:], b)
			return out
		}
		return b
	}

	abs := new(big.Int).Abs(n)
	length := (abs.BitLen() + 7) / 8
	for {
		mod := new(big.Int).Lsh(big.NewInt(1), uint(length*8))
		tc := new(big.Int).Sub(mod, abs)
		b := tc.Bytes()
		if len(b) < length {
			out := make([]byte, length)
			copy(out[length-len(b):], b)
			b = out
		}
		if len(b) > 0 && b[0]&0x80 != 0 {
			return b
		}
		length++
	}
}

func getPartitionSourceValue(sourceName string, row ChangeRow) (string, bool, error) {
	switch sourceName {
	case "_tidb_commit_time":
		return row.CommitTime, true, nil
	case "_tidb_commit_ts":
		return row.CommitTs, true, nil
	case "_tidb_op":
		return row.Op, true, nil
	default:
		if row.Columns == nil {
			return "", false, nil
		}
		v, ok := row.Columns[sourceName]
		if !ok || v == nil {
			return "", false, nil
		}
		return *v, true, nil
	}
}

func avroUnionTypeNameForIcebergType(icebergType string) string {
	switch {
	case icebergType == "binary":
		return "bytes"
	case icebergType == "date":
		return "int"
	case icebergType == "timestamp":
		return "long"
	case strings.HasPrefix(icebergType, "decimal(") && strings.HasSuffix(icebergType, ")"):
		return "bytes"
	default:
		return icebergType
	}
}

func sanitizePartitionFieldName(name string) string {
	s := strings.TrimSpace(name)
	if s == "" {
		return "p"
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('_')
	}
	out := b.String()
	if out == "" {
		out = "p"
	}
	if out[0] >= '0' && out[0] <= '9' {
		out = "_" + out
	}
	return out
}

func splitPartitioningList(expr string) []string {
	var parts []string
	var b strings.Builder
	depth := 0
	for _, r := range expr {
		switch r {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				p := strings.TrimSpace(b.String())
				if p != "" {
					parts = append(parts, p)
				}
				b.Reset()
				continue
			}
		}
		b.WriteRune(r)
	}
	if p := strings.TrimSpace(b.String()); p != "" {
		parts = append(parts, p)
	}
	return parts
}

func parseTransformCall(expr string) (string, []string, error) {
	s := strings.TrimSpace(expr)
	open := strings.IndexByte(s, '(')
	close := strings.LastIndexByte(s, ')')
	if open <= 0 || close < open {
		return "", nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs(fmt.Sprintf("invalid partition transform: %s", expr))
	}

	name := strings.ToLower(strings.TrimSpace(s[:open]))
	if name == "" {
		return "", nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs(fmt.Sprintf("invalid partition transform: %s", expr))
	}

	argStr := strings.TrimSpace(s[open+1 : close])
	var args []string
	if argStr != "" {
		raw := strings.Split(argStr, ",")
		args = make([]string, 0, len(raw))
		for _, a := range raw {
			v := strings.TrimSpace(a)
			if v != "" {
				args = append(args, v)
			}
		}
	}
	return name, args, nil
}

func ensurePartitionSpecMatches(m *tableMetadata, desired *resolvedPartitionSpec) error {
	if m == nil || desired == nil {
		return nil
	}

	desiredJSON, err := json.Marshal(desired.spec)
	if err != nil {
		return cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}

	existing := m.currentPartitionSpec()
	if existing == nil {
		m.PartitionSpecs = []partitionSpec{desired.spec}
		m.DefaultSpecID = desired.spec.SpecID
		m.LastPartitionID = desired.lastPartitionID
		return nil
	}

	existingJSON, err := json.Marshal(*existing)
	if err != nil {
		return cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}

	if !bytes.Equal(existingJSON, desiredJSON) {
		if m.CurrentSnapshotID == nil && len(m.Snapshots) == 0 {
			// No visible data, safe to adopt the desired spec.
			m.PartitionSpecs = []partitionSpec{desired.spec}
			m.DefaultSpecID = desired.spec.SpecID
			m.LastPartitionID = desired.lastPartitionID
			return nil
		}
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs("iceberg partition spec mismatch")
	}

	if m.LastPartitionID < desired.lastPartitionID {
		m.LastPartitionID = desired.lastPartitionID
	}
	return nil
}

func (m *tableMetadata) currentPartitionSpec() *partitionSpec {
	if m == nil {
		return nil
	}
	for i := range m.PartitionSpecs {
		if m.PartitionSpecs[i].SpecID == m.DefaultSpecID {
			return &m.PartitionSpecs[i]
		}
	}
	if len(m.PartitionSpecs) > 0 {
		return &m.PartitionSpecs[0]
	}
	return nil
}

func parseEpochDaysString(raw string) (int32, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, cerror.ErrSinkURIInvalid.GenWithStackByArgs("epoch days is empty")
	}
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	return int32(n), nil
}

func murmur3Sum32(data []byte) uint32 {
	const (
		c1 = 0xcc9e2d51
		c2 = 0x1b873593
	)

	var h1 uint32
	nblocks := len(data) / 4
	for i := 0; i < nblocks; i++ {
		k1 := binary.LittleEndian.Uint32(data[i*4:])
		k1 *= c1
		k1 = (k1 << 15) | (k1 >> 17)
		k1 *= c2

		h1 ^= k1
		h1 = (h1 << 13) | (h1 >> 19)
		h1 = h1*5 + 0xe6546b64
	}

	tail := data[nblocks*4:]
	var k1 uint32
	switch len(tail) & 3 {
	case 3:
		k1 ^= uint32(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(tail[0])
		k1 *= c1
		k1 = (k1 << 15) | (k1 >> 17)
		k1 *= c2
		h1 ^= k1
	}

	h1 ^= uint32(len(data))
	h1 ^= h1 >> 16
	h1 *= 0x85ebca6b
	h1 ^= h1 >> 13
	h1 *= 0xc2b2ae35
	h1 ^= h1 >> 16
	return h1
}
