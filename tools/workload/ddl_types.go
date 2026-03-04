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

package main

type DDLType int

const (
	ddlAddColumn DDLType = iota
	ddlDropColumn
	ddlAddIndex
	ddlDropIndex
	ddlTruncateTable
)

func (t DDLType) String() string {
	switch t {
	case ddlAddColumn:
		return "add_column"
	case ddlDropColumn:
		return "drop_column"
	case ddlAddIndex:
		return "add_index"
	case ddlDropIndex:
		return "drop_index"
	case ddlTruncateTable:
		return "truncate_table"
	default:
		return "unknown"
	}
}
