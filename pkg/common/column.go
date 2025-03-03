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

package common

//go:generate msgp

// Column represents a column value and its schema info
type Column struct {
	Name      string         `msg:"name"`
	Type      byte           `msg:"type"`
	Charset   string         `msg:"charset"`
	Collation string         `msg:"collation"`
	Flag      ColumnFlagType `msg:"-"`
	Value     interface{}    `msg:"-"`
	Default   interface{}    `msg:"-"`

	// ApproximateBytes is approximate bytes consumed by the column.
	ApproximateBytes int `msg:"-"`
}
