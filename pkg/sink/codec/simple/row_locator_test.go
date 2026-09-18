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

package simple

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/sink/codec/common"
)

// TestDecodedTableInfoLocatesRowByPrimaryKey checks the table info this decoder
// rebuilds from a table schema: a composite primary key must stay the handle key,
// with the real column offsets, because that is what the MySQL sink locates rows
// by.
func TestDecodedTableInfoLocatesRowByPrimaryKey(t *testing.T) {
	schema := &TableSchema{
		Schema: "test",
		Table:  "t",
		Columns: []*columnSchema{
			{Name: "a", DataType: dataType{MySQLType: "INT"}},
			{Name: "b", DataType: dataType{MySQLType: "INT"}},
			{Name: "c", DataType: dataType{MySQLType: "VARCHAR"}},
		},
		Indexes: []*IndexSchema{
			{Name: "PRIMARY", Unique: true, Primary: true, Columns: []string{"a", "b"}},
		},
	}

	common.RequireRowLocatorByPrimaryKey(t, newTableInfo(schema), "a", "b")
}
