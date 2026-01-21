// Copyright 2024 PingCAP, Inc.
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

package cli

import (
	"strings"

	v2 "github.com/pingcap/ticdc/api/v2"
)

func formatTableNames(tableNames []v2.TableName) string {
	if len(tableNames) == 0 {
		return "[]"
	}

	formatted := make([]string, 0, len(tableNames))
	for _, tableName := range tableNames {
		formatted = append(formatted, formatTableName(tableName))
	}
	return "[" + strings.Join(formatted, ", ") + "]"
}

func formatTableName(tableName v2.TableName) string {
	if tableName.Schema != "" {
		return tableName.Schema + "." + tableName.Table
	}
	return tableName.Table
}
