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

package filter

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestDmlExprFilterInvalidConfig(t *testing.T) {
	t.Parallel()

	ti := mustNewCommonTableInfo("test", "t1",
		[]*model.ColumnInfo{
			newColumnInfo(1, "id", mysql.TypeLong, mysql.PriKeyFlag),
		}, nil)
	emptyRow := chunk.Row{}

	// Invalid SQL expression syntax
	cfg := &config.FilterConfig{
		EventFilters: []*config.EventFilterRule{
			{
				Matcher:               []string{"test.t1"},
				IgnoreInsertValueExpr: util.AddressOf("id > > 100"),
			},
		},
	}
	f, err := newExprFilter("UTC", cfg)
	require.NoError(t, err) // newExprFilter does not verify expressions
	row := types.MakeDatums(int64(101))
	chunkRow := datumsToChunkRow(row, ti)

	_, err = f.shouldSkipDML(common.RowTypeInsert, emptyRow, chunkRow, ti)
	require.Error(t, err)
	require.True(t, cerror.ErrExpressionParseFailed.Equal(err))
}
