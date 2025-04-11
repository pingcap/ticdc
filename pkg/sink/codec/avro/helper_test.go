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

package avro

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestGetAvroNamespace(t *testing.T) {
	t.Parallel()

	require.Equal(
		t,
		"normalNamespace.normalSchema",
		getAvroNamespace("normalNamespace", "normalSchema"),
	)
	require.Equal(
		t,
		"_1Namespace._1Schema",
		getAvroNamespace("1Namespace", "1Schema"),
	)
	require.Equal(
		t,
		"N_amespace.S_chema",
		getAvroNamespace("N-amespace", "S.chema"),
	)

	require.Equal(
		t,
		"normalNamespace",
		getAvroNamespace("normalNamespace", ""),
	)
}

func TestSanitizeName(t *testing.T) {
	t.Parallel()

	require.Equal(t, "normalColumnName123", common.SanitizeName("normalColumnName123"))
	require.Equal(
		t,
		"_1ColumnNameStartWithNumber",
		common.SanitizeName("1ColumnNameStartWithNumber"),
	)
	require.Equal(t, "A_B", common.SanitizeName("A.B"))
	require.Equal(t, "columnNameWith__", common.SanitizeName("columnNameWith中文"))
}
