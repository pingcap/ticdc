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

package open

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// TestDecodedTableInfoLocatesRowByPrimaryKey checks the table info this decoder
// builds from a message: a composite primary key must stay the handle key, with
// the real column offsets, because that is what the MySQL sink locates rows by.
func TestDecodedTableInfoLocatesRowByPrimaryKey(t *testing.T) {
	decoder := &decoder{}
	key := &messageKey{Schema: "test", Table: "t"}
	value := &messageRow{Update: map[string]column{
		"a": {Type: mysql.TypeLong, Flag: primaryKeyFlag | handleKeyFlag, Value: int64(1)},
		"b": {Type: mysql.TypeLong, Flag: primaryKeyFlag | handleKeyFlag, Value: int64(2)},
		"c": {Type: mysql.TypeLong, Value: int64(3)},
	}}

	common.RequireRowLocatorByPrimaryKey(t, decoder.newTableInfo(key, value), "a", "b")
}
