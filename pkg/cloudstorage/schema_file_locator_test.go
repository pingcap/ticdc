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
	"testing"

	"github.com/pingcap/ticdc/pkg/sink/codec/common"
)

// TestTableInfoLocatesRowByPrimaryKey checks the table info rebuilt from a schema
// file: a composite primary key must stay the handle key, because that is what
// the MySQL sink locates rows by.
func TestTableInfoLocatesRowByPrimaryKey(t *testing.T) {
	schemaFile := &SchemaFile{
		Schema: "test",
		Table:  "t",
		Columns: []TableCol{
			{Name: "a", Tp: "int", IsPK: "true"},
			{Name: "b", Tp: "int", IsPK: "true"},
			{Name: "c", Tp: "varchar"},
		},
	}

	common.RequireRowLocatorByPrimaryKey(t, schemaFile.TableInfo(), "a", "b")
}
