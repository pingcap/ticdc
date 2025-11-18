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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"fmt"
	"strings"

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/redact"
)

func init() {
	// Initialize redact mode to OFF by default to prevent panics in tests
	// This ensures perrors.RedactLogEnabled.Load() always returns a valid mode
	perrors.RedactLogEnabled.Store(perrors.RedactLogDisable)
}

// ParseRedactMode converts string to redact mode string compatible with errors.RedactLogEnabled.
func ParseRedactMode(mode string) string {
	switch strings.ToUpper(strings.TrimSpace(mode)) {
	case "ON", "TRUE", "1", "ENABLE", "ENABLED":
		return perrors.RedactLogEnable
	case "MARKER", "MARK":
		return perrors.RedactLogMarker
	case "OFF", "FALSE", "0", "DISABLE", "DISABLED", "":
		return perrors.RedactLogDisable
	default:
		return perrors.RedactLogDisable
	}
}

// IsValidRedactMode checks if the given string is a valid redaction mode.
func IsValidRedactMode(input string, parsed string) bool {
	upperMode := strings.ToUpper(strings.TrimSpace(input))
	return upperMode == "OFF" || upperMode == "ON" || upperMode == "MARKER" ||
		(parsed == perrors.RedactLogEnable && (upperMode == "TRUE" || upperMode == "1" || upperMode == "ENABLE" || upperMode == "ENABLED")) ||
		(parsed == perrors.RedactLogDisable && (upperMode == "FALSE" || upperMode == "0" || upperMode == "DISABLE" || upperMode == "DISABLED" || upperMode == "")) ||
		(parsed == perrors.RedactLogMarker && upperMode == "MARK")
}

// RedactValue redacts a string value using the current redaction mode.
// This properly supports MARKER mode by using redact.String() instead of redact.Value().
// TiDB's redact.Value() treats MARKER the same as ON (returns "?"), but redact.String()
// properly wraps values with ‹› markers in MARKER mode.
func RedactValue(value string) string {
	mode := perrors.RedactLogEnabled.Load()
	return redact.String(mode, value)
}

// RedactAny redacts any value by converting to string and applying redaction.
// This extends TiDB's redact package with support for any type and proper MARKER mode.
func RedactAny(value any) string {
	if value == nil {
		return "NULL"
	}
	mode := perrors.RedactLogEnabled.Load()
	return redact.String(mode, fmt.Sprintf("%v", value))
}

// RedactArgs redacts SQL arguments/parameters for logging.
// This provides formatted argument redaction with proper MARKER mode support.
func RedactArgs(args []interface{}) string {
	if len(args) == 0 {
		return "()"
	}

	mode := perrors.RedactLogEnabled.Load()
	var b strings.Builder
	b.WriteString("(")
	for i, arg := range args {
		if i > 0 {
			b.WriteString(", ")
		}
		if arg == nil {
			b.WriteString("NULL")
		} else {
			b.WriteString(redact.String(mode, fmt.Sprintf("%v", arg)))
		}
	}
	b.WriteString(")")
	return b.String()
}
