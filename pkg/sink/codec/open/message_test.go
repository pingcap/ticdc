// Copyright 2022 PingCAP, Inc.
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

package open

import (
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/stretchr/testify/require"
)

func TestFormatCol(t *testing.T) {
	t.Parallel()
	row := &messageRow{Update: map[string]column{"test": {
		Type:  mysql.TypeString,
		Value: "测",
	}}}
	rowEncode, err := row.encode()
	require.NoError(t, err)
	row2 := new(messageRow)
	row2.decode(rowEncode)
	require.Equal(t, row, row2)
	// toRowChangeColumn will change the Value from string to []byte
	row = &messageRow{Update: map[string]column{"test": {
		Type:  mysql.TypeBlob,
		Value: []byte("测"),
	}}}
	rowEncode, err = row.encode()
	require.NoError(t, err)
	row2 = new(messageRow)
	row2.decode(rowEncode)
	for colName, column := range row2.Update {
		row2.Update[colName] = formatColumn(column, *types.NewFieldType(mysql.TypeBlob))
	}
	require.Equal(t, row, row2)
}

func TestNonBinaryStringCol(t *testing.T) {
	t.Parallel()
	mqCol := column{
		Type:  mysql.TypeString,
		Value: "value",
	}
	row := &messageRow{Update: map[string]column{"test": mqCol}}
	rowEncode, err := row.encode()
	require.NoError(t, err)
	row2 := new(messageRow)
	row2.decode(rowEncode)
	require.Equal(t, row, row2)
}

func TestVarBinaryCol(t *testing.T) {
	t.Parallel()
	mqCol := column{
		Type: mysql.TypeString,
		Flag: binaryFlag,
	}
	value := []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}
	str := strconv.Quote(string(value))
	str = str[1 : len(str)-1]
	mqCol.Value = str
	row := &messageRow{Update: map[string]column{"test": mqCol}}
	rowEncode, err := row.encode()
	require.NoError(t, err)
	row2 := new(messageRow)
	row2.decode(rowEncode)
	require.Equal(t, row, row2)
}
