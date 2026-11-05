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

package sqlname

import (
	"bytes"
	"testing"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/format"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/stretchr/testify/require"
)

func TestBindingsApplyAtomic(t *testing.T) {
	stmt, err := parser.New().ParseOneStmt("SELECT A.id, b.* FROM src.a JOIN src.b ON a.id = b.id", "", "")
	require.NoError(t, err)
	restore := func() string {
		var buf bytes.Buffer
		require.NoError(t, stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &buf)))
		return buf.String()
	}
	original := restore()
	bindings := Bind(stmt, "src")
	require.Equal(t, original, restore())
	failure := errors.ErrDDLEventError.GenWithStackByArgs()
	calls := 0
	changed, err := bindings.Apply(func(source Name) (Name, error) {
		calls++
		if source.Table == "b" {
			return Name{}, failure
		}
		return Name{Schema: "dst", Table: source.Table + "_r"}, nil
	})
	require.ErrorIs(t, err, failure)
	require.False(t, changed)
	require.Equal(t, 2, calls)
	require.Equal(t, original, restore())
	changed, err = bindings.Apply(func(source Name) (Name, error) {
		return Name{Schema: "dst", Table: source.Table + "_r"}, nil
	})
	require.NoError(t, err)
	require.True(t, changed)
	require.Contains(t, restore(), "SELECT `dst`.`a_r`.`id`,`dst`.`b_r`.*")
	require.Contains(t, restore(), "`dst`.`a_r`.`id`=`dst`.`b_r`.`id`")
}
