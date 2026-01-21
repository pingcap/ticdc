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

package canal

import (
	"testing"

	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestConvertDdlEventType(t *testing.T) {
	t.Parallel()

	require.Equal(t, "CREATE", convertDdlEventType(byte(timodel.ActionCreateTable)).String())
	require.Equal(t, "ERASE", convertDdlEventType(byte(timodel.ActionDropTable)).String())
	require.Equal(t, "TRUNCATE", convertDdlEventType(byte(timodel.ActionTruncateTable)).String())
	require.Equal(t, "ALTER", convertDdlEventType(byte(timodel.ActionAddColumn)).String())
}

func TestFormatColumnValueBlob(t *testing.T) {
	t.Parallel()

	plainBlob := newTestColumnInfo("a", mysql.TypeBlob, 0)
	row := makeOneColumnRow(plainBlob, func(chk *chunk.Chunk) {
		chk.AppendBytes(0, []byte("abc"))
	})
	s, javaType := formatColumnValue(&row, 0, plainBlob)
	require.False(t, s.isNull)
	require.Equal(t, "abc", s.value)
	require.Equal(t, codecCommon.JavaSQLTypeCLOB, javaType)

	binaryBlob := newTestColumnInfo("a", mysql.TypeBlob, mysql.BinaryFlag)
	row = makeOneColumnRow(binaryBlob, func(chk *chunk.Chunk) {
		chk.AppendBytes(0, []byte("abc"))
	})
	s, javaType = formatColumnValue(&row, 0, binaryBlob)
	require.False(t, s.isNull)
	require.Equal(t, "abc", s.value)
	require.Equal(t, codecCommon.JavaSQLTypeBLOB, javaType)
}
