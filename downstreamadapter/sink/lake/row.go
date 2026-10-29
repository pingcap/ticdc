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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package lake owns the native Lake sink implementation. Its row and SQL
// layer is ported from bend-cdc's TiDB path, without the Canal JSON decoder.
package lake

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/pingcap/ticdc/pkg/errors"
)

type columnEncoding uint8

const (
	scalarColumn columnEncoding = iota
	binaryColumn
	jsonColumn
)

type column struct {
	name        string
	encoding    columnEncoding
	sourceID    int64
	typ         string
	nullable    bool
	defaultSQL  string
	defaultExpr bool
}

// tableSchema describes the target projection, not an inferred source schema.
type tableSchema struct {
	database   string
	source     string
	table      string
	columns    []column
	primaryKey []string
}

func (s tableSchema) validate() error {
	validName := func(name string) bool { return name != "" && !strings.ContainsRune(name, 0) }
	if !validName(s.database) || !validName(s.table) || len(s.columns) == 0 || len(s.primaryKey) == 0 {
		return errors.ErrLakeInvalidData.GenWithStackByArgs("table metadata and a primary key are required")
	}
	columns := make(map[string]struct{}, len(s.columns))
	folded := make(map[string]struct{}, len(s.columns))
	for _, col := range s.columns {
		if !validName(col.name) || col.encoding > jsonColumn {
			return errors.ErrLakeInvalidData.GenWithStackByArgs("invalid column metadata")
		}
		lower := strings.ToLower(col.name)
		if _, ok := folded[lower]; ok {
			return errors.ErrLakeInvalidData.GenWithStackByArgs("duplicate column name")
		}
		folded[lower] = struct{}{}
		columns[col.name] = struct{}{}
	}
	keys := make(map[string]struct{}, len(s.primaryKey))
	for _, key := range s.primaryKey {
		if _, ok := columns[key]; !ok {
			return errors.ErrLakeInvalidData.GenWithStackByArgs("primary key is missing from columns")
		}
		if _, ok := keys[key]; ok {
			return errors.ErrLakeInvalidData.GenWithStackByArgs("duplicate primary key column")
		}
		keys[key] = struct{}{}
	}
	return nil
}

func (s tableSchema) canonicalKeys() []string {
	keys := append([]string(nil), s.primaryKey...)
	sort.Slice(keys, func(i, j int) bool { return strings.ToLower(keys[i]) < strings.ToLower(keys[j]) })
	return keys
}

type rowAction string

const (
	insertAction rowAction = "insert"
	updateAction rowAction = "update"
	deleteAction rowAction = "delete"
)

// normalizedRow is an owned, immutable raw payload. The SQL writer cannot
// accidentally run Canal ordinal/Latin-1 decoding on this representation again.
type normalizedRow struct {
	id       string
	payload  string
	commitTs uint64
	action   rowAction
}

// normalizeNativeRow accepts labels for ENUM/SET, original bytes for BINARY and
// exact JSON documents. The DMLEvent adapter must extract these from typed datum
// values; this function deliberately does not infer ordinal encodings.
func normalizeNativeRow(schema tableSchema, action rowAction, commitTs uint64, values map[string]any) (normalizedRow, error) {
	if err := schema.validate(); err != nil {
		return normalizedRow{}, err
	}
	if commitTs == 0 || commitTs > math.MaxInt64 {
		return normalizedRow{}, errors.ErrLakeInvalidData.GenWithStackByArgs("commit TSO cannot be represented in raw BIGINT")
	}
	if action != insertAction && action != updateAction && action != deleteAction {
		return normalizedRow{}, errors.ErrLakeInvalidData.GenWithStackByArgs("unsupported row action")
	}
	data := make(map[string]any, len(values))
	for _, col := range schema.columns {
		value, exists := values[col.name]
		if !exists {
			if action == deleteAction {
				continue // A tombstone needs the old primary key, not an after image.
			}
			return normalizedRow{}, errors.ErrLakeInvalidData.GenWithStackByArgs("incomplete after image")
		}
		normalized, err := normalizeNativeValue(col.encoding, value)
		if err != nil {
			return normalizedRow{}, err
		}
		data[col.name] = normalized
	}
	for name := range values {
		if _, exists := data[name]; !exists {
			return normalizedRow{}, errors.ErrLakeInvalidData.GenWithStackByArgs("value has no column metadata")
		}
	}
	id, err := conflictIdentity(data, schema.canonicalKeys())
	if err != nil {
		return normalizedRow{}, err
	}
	payload, err := json.Marshal(data)
	if err != nil {
		return normalizedRow{}, errors.WrapError(errors.ErrLakeInvalidData, err, "cannot encode normalized row")
	}
	return normalizedRow{id: id, payload: string(payload), commitTs: commitTs, action: action}, nil
}

func normalizeNativeValue(encoding columnEncoding, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	if encoding == binaryColumn {
		switch v := value.(type) {
		case []byte:
			return hex.EncodeToString(v), nil
		case string:
			return hex.EncodeToString([]byte(v)), nil // Original bytes, not Canal Latin-1.
		default:
			return nil, errors.ErrLakeInvalidData.GenWithStackByArgs("binary datum must contain bytes")
		}
	}
	if encoding == jsonColumn {
		var document []byte
		switch v := value.(type) {
		case string:
			document = []byte(v)
		case []byte:
			document = v
		case json.RawMessage:
			document = v
		default:
			return nil, errors.ErrLakeInvalidData.GenWithStackByArgs("JSON datum must contain an exact document")
		}
		if !json.Valid(document) {
			return nil, errors.ErrLakeInvalidData.GenWithStackByArgs("invalid JSON document")
		}
		return string(document), nil // Includes JSON null, distinct from SQL NULL.
	}
	switch v := value.(type) {
	case string:
		if !utf8.ValidString(v) {
			return nil, errors.ErrLakeInvalidData.GenWithStackByArgs("invalid text encoding")
		}
		return strings.Clone(v), nil
	case []byte:
		if !utf8.Valid(v) {
			return nil, errors.ErrLakeInvalidData.GenWithStackByArgs("invalid text encoding")
		}
		return string(v), nil
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return value, nil
	case json.Number:
		text := string(v)
		if len(text) == 0 || !json.Valid([]byte(text)) || (text[0] != '-' && (text[0] < '0' || text[0] > '9')) {
			return nil, errors.ErrLakeInvalidData.GenWithStackByArgs("invalid exact JSON number")
		}
		return json.Number(strings.Clone(string(v))), nil
	case float32:
		if !math.IsNaN(float64(v)) && !math.IsInf(float64(v), 0) {
			return v, nil
		}
	case float64:
		if !math.IsNaN(v) && !math.IsInf(v, 0) {
			return v, nil
		}
	}
	return nil, errors.ErrLakeInvalidData.GenWithStackByArgs("unsupported native scalar datum")
}

// conflictIdentity preserves bend-cdc's single-key text and canonical composite
// JSON shape. It intentionally contains no arrival time, batch ID or local seq.
func conflictIdentity(values map[string]any, keys []string) (string, error) {
	type keyPart struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}
	parts := make([]keyPart, 0, len(keys))
	for _, key := range keys {
		value, ok := values[key]
		if !ok || value == nil {
			return "", errors.ErrLakeInvalidData.GenWithStackByArgs("primary key is missing or null")
		}
		text := fmt.Sprint(value)
		if len(keys) == 1 {
			if strings.TrimSpace(text) == "" {
				return "", errors.ErrLakeInvalidData.GenWithStackByArgs("empty primary key")
			}
			return text, nil
		}
		parts = append(parts, keyPart{Name: key, Value: text})
	}
	encoded, err := json.Marshal(parts)
	if err != nil {
		return "", errors.WrapError(errors.ErrLakeInvalidData, err, "cannot encode primary key")
	}
	return string(encoded), nil
}
