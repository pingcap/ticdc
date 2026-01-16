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
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/stretchr/testify/require"
)

func TestFormatColumnBinaryStringUnquote(t *testing.T) {
	t.Parallel()

	ft := *types.NewFieldType(mysql.TypeString)
	col := column{
		Type:  mysql.TypeString,
		Flag:  binaryFlag,
		Value: "a\\nb",
	}
	formatted := formatColumn(col, ft)

	b, ok := formatted.Value.([]byte)
	require.True(t, ok)
	require.Equal(t, []byte("a\nb"), b)
}

func TestFormatColumnBlobBase64Decode(t *testing.T) {
	t.Parallel()

	ft := *types.NewFieldType(mysql.TypeBlob)
	raw := []byte("blob-data")
	encoded := base64.StdEncoding.EncodeToString(raw)

	col := column{
		Type:  mysql.TypeBlob,
		Value: encoded,
	}
	formatted := formatColumn(col, ft)

	b, ok := formatted.Value.([]byte)
	require.True(t, ok)
	require.Equal(t, raw, b)
}

func TestFormatColumnFloatNumber(t *testing.T) {
	t.Parallel()

	ft := *types.NewFieldType(mysql.TypeFloat)
	col := column{
		Type:  mysql.TypeFloat,
		Value: json.Number("3.14"),
	}
	formatted := formatColumn(col, ft)

	v, ok := formatted.Value.(float32)
	require.True(t, ok)
	require.InDelta(t, 3.14, float64(v), 0.0001)
}
