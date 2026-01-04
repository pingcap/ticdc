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

//go:build !nextgen

package csv

import (
	lconfig "github.com/pingcap/tidb/pkg/lightning/config"
)

func newCSVConfig(delimiter string, quota string, terminator string, nullDefinedBy []string, backslashEscape bool, headerSchemaMatch bool, header bool) *lconfig.CSVConfig {
	return &lconfig.CSVConfig{
		Separator:         delimiter,
		Delimiter:         quota,
		Terminator:        terminator,
		Null:              nullDefinedBy,
		BackslashEscape:   backslashEscape,
		HeaderSchemaMatch: headerSchemaMatch,
		Header:            header,
	}
}
