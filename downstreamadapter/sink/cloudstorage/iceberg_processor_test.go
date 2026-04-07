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

package cloudstorage

import (
	"encoding/base64"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestFormatIcebergColumnValueKeepsTextPlain(t *testing.T) {
	ft := types.NewFieldType(mysql.TypeBlob)
	ft.SetCharset(charset.CharsetUTF8MB4)

	chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, 1)
	chk.AppendString(0, "hello iceberg")
	row := chk.GetRow(0)

	value, err := formatIcebergColumnValue(&row, 0, ft)
	require.NoError(t, err)
	require.NotNil(t, value)
	require.Equal(t, "hello iceberg", *value)
}

func TestFormatIcebergColumnValueBase64EncodesBinaryBlob(t *testing.T) {
	ft := types.NewFieldType(mysql.TypeBlob)
	ft.SetCharset(charset.CharsetBin)

	chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, 1)
	chk.AppendBytes(0, []byte("hello iceberg"))
	row := chk.GetRow(0)

	value, err := formatIcebergColumnValue(&row, 0, ft)
	require.NoError(t, err)
	require.NotNil(t, value)
	require.Equal(t, base64.StdEncoding.EncodeToString([]byte("hello iceberg")), *value)
}
