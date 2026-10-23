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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTableOf(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
		table string
	}{
		{"simple", `{"table":"probe","type":"INSERT"}`, "probe"},
		{"debezium insert", `{"payload":{"op":"c","source":{"table":"probe"}}}`, "probe"},
		{"debezium update", `{"payload":{"op":"u","source":{"table":"probe"}}}`, "probe"},
		{"debezium delete", `{"payload":{"op":"d","source":{"table":"probe"}}}`, "probe"},
		{"debezium snapshot", `{"payload":{"op":"r","source":{"table":"probe"}}}`, "probe"},
		{"debezium DDL", `{"payload":{"source":{"table":"probe"},"ddl":"CREATE TABLE probe (id INT)"}}`, ""},
		{"resolved", `{"payload":{"op":"m"}}`, ""},
		{"tombstone", `null`, ""},
		{"invalid", `invalid JSON`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.table, tableOf([]byte(tc.value)))
		})
	}
}
