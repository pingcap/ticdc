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

package avro

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

// TestDecodedTableInfoLocatesRowByPrimaryKey checks the table info this decoder
// builds from an avro message schema: the key columns must stay the handle key,
// with the real column offsets, because that is what the MySQL sink locates rows
// by.
func TestDecodedTableInfoLocatesRowByPrimaryKey(t *testing.T) {
	fields := []any{
		map[string]any{"name": "a", "type": map[string]any{"connect.parameters": map[string]any{"tidb_type": "INT"}}},
		map[string]any{"name": "b", "type": map[string]any{"connect.parameters": map[string]any{"tidb_type": "INT"}}},
		map[string]any{"name": "c", "type": map[string]any{"connect.parameters": map[string]any{"tidb_type": "INT"}}},
	}
	columns, _, err := avroData2Columns(map[string]any{
		"a": int64(1), "b": int64(2), "c": int64(3),
	}, fields)
	require.NoError(t, err)

	tableInfo := newTableInfo("test", "t", columns, map[string]any{"a": nil, "b": nil})
	common.RequireRowLocatorByPrimaryKey(t, tableInfo, "a", "b")
}
