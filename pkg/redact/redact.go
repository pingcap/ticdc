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

package redact

import (
	"encoding/hex"
	"fmt"
	"strings"
	"sync/atomic"
)

// RedactMode represents the redaction mode for sensitive information in logs.
// This is compatible with TiDB, TiKV, and PD redaction modes.
type RedactMode string

const (
	// Off disables redaction - sensitive information is logged as-is
	Off RedactMode = "OFF"
	// On enables redaction - sensitive information is replaced with "?"
	On RedactMode = "ON"
	// Marker enables marker mode - sensitive information is wrapped with ‹ and ›
	// This mode allows post-processing to recover the original data with proper authorization
	Marker RedactMode = "MARKER"
)

var enabledRedactMode atomic.Value

func init() {
	SetRedactMode(Off)
}

// SetRedactMode sets the global redaction mode
func SetRedactMode(mode RedactMode) {
	enabledRedactMode.Store(string(mode))
}

// GetRedactMode gets the current redaction mode
func GetRedactMode() RedactMode {
	if mode := enabledRedactMode.Load(); mode != nil {
		return RedactMode(mode.(string))
	}
	return Off
}

// NeedRedact returns whether redaction is enabled (any mode other than Off)
func NeedRedact() bool {
	return GetRedactMode() != Off
}

// String redacts the input string according to current redaction mode.
// This is the primary redaction function used throughout TiCDC.
//
// Usage examples:
//   - SQL queries: redact.String(sqlQuery)
//   - User data: redact.String(userData)
//   - Configuration values: redact.String(configValue)
func String(input string) string {
	switch GetRedactMode() {
	case On:
		return "?"
	case Marker:
		b := &strings.Builder{}
		b.Grow(len(input) + 2) // +2 for the markers
		b.WriteRune('‹')
		for _, c := range input {
			if c == '‹' || c == '›' {
				// Escape existing markers by doubling them
				b.WriteRune(c)
				b.WriteRune(c)
			} else {
				b.WriteRune(c)
			}
		}
		b.WriteRune('›')
		return b.String()
	default: // Off or unknown
		return input
	}
}

// Key redacts key data (typically byte slices) by converting to hex and redacting.
// Used for table keys, index keys, and other binary key data.
//
// Usage examples:
//   - Table keys: redact.Key(tableKey)
//   - Index keys: redact.Key(indexKey)
//   - Row keys: redact.Key(rowKey)
func Key(key []byte) string {
	if key == nil {
		return "NULL"
	}
	return String(strings.ToUpper(hex.EncodeToString(key)))
}

// Value redacts sensitive values.
// This is an alias for String but provides semantic clarity when redacting data values.
//
// Usage examples:
//   - Column values: redact.Value(columnValue)
//   - Row data: redact.Value(rowData)
//   - Arguments: redact.Value(argValue)
func Value(value string) string {
	return String(value)
}

// Bytes redacts byte slice data.
// Used for binary data that should be redacted as strings.
func Bytes(data []byte) string {
	if data == nil {
		return "NULL"
	}
	return String(string(data))
}

// SQL redacts SQL statements including queries and DDL.
// This is a semantic wrapper around String for SQL-specific redaction.
//
// Usage examples:
//   - INSERT statements: redact.SQL(insertStmt)
//   - UPDATE statements: redact.SQL(updateStmt)
//   - DDL statements: redact.SQL(ddlStmt)
func SQL(sql string) string {
	return String(sql)
}

// Args redacts SQL arguments/parameters.
// Formats and redacts argument values for logging.
func Args(args []interface{}) string {
	if len(args) == 0 {
		return "()"
	}

	var b strings.Builder
	b.WriteString("(")
	for i, arg := range args {
		if i > 0 {
			b.WriteString(", ")
		}
		if arg == nil {
			b.WriteString("NULL")
		} else {
			b.WriteString(Value(fmt.Sprintf("%v", arg)))
		}
	}
	b.WriteString(")")
	return b.String()
}

// Any redacts any/interface{} values.
// Converts the value to string using fmt.Sprintf and applies redaction.
//
// Usage examples:
//   - Generic values: redact.Any(anyValue)
//   - Column values from decoders: redact.Any(colValue)
//   - Dynamic data: redact.Any(data)
func Any(value any) string {
	if value == nil {
		return "NULL"
	}
	return String(fmt.Sprintf("%v", value))
}

// Anys redacts a slice of any/interface{} values.
// Returns a redacted slice of strings suitable for zap.Strings() or similar.
func Anys(values []any) []string {
	if len(values) == 0 {
		return []string{}
	}

	result := make([]string, len(values))
	for i, v := range values {
		if v == nil {
			result[i] = "NULL"
		} else {
			result[i] = String(fmt.Sprintf("%v", v))
		}
	}
	return result
}

// redactStringer implements fmt.Stringer with redaction support
type redactStringer struct {
	mode     RedactMode
	stringer fmt.Stringer
}

func (s redactStringer) String() string {
	if s.stringer == nil {
		return String("NULL")
	}
	originalMode := GetRedactMode()
	SetRedactMode(s.mode)
	defer SetRedactMode(originalMode)
	return String(s.stringer.String())
}

// Stringer wraps a fmt.Stringer to apply redaction using the specified mode.
// This is compatible with TiDB's redact.Stringer function.
func Stringer(mode RedactMode, input fmt.Stringer) fmt.Stringer {
	return redactStringer{mode, input}
}

// StringerWithCurrentMode wraps a fmt.Stringer to apply redaction using current global mode
func StringerWithCurrentMode(input fmt.Stringer) fmt.Stringer {
	return redactStringer{GetRedactMode(), input}
}

// InitRedact initializes redaction with a boolean flag (for backward compatibility).
// This matches TiDB's InitRedact function signature for easier integration.
func InitRedact(redactLog bool) {
	if redactLog {
		SetRedactMode(On)
	} else {
		SetRedactMode(Off)
	}
}

// ParseRedactMode converts string to RedactMode with case-insensitive matching.
// Supports multiple string representations for flexibility in configuration.
func ParseRedactMode(mode string) RedactMode {
	switch strings.ToUpper(strings.TrimSpace(mode)) {
	case "ON", "TRUE", "1", "ENABLE", "ENABLED":
		return On
	case "MARKER", "MARK":
		return Marker
	case "OFF", "FALSE", "0", "DISABLE", "DISABLED", "":
		return Off
	default:
		// For unknown values, default to Off for safety
		return Off
	}
}

// IsValidRedactMode checks if the given string is a valid redaction mode
func IsValidRedactMode(mode string) bool {
	parsed := ParseRedactMode(mode)
	upperMode := strings.ToUpper(strings.TrimSpace(mode))
	return upperMode == string(parsed) ||
		(parsed == On && (upperMode == "TRUE" || upperMode == "1" || upperMode == "ENABLE" || upperMode == "ENABLED")) ||
		(parsed == Off && (upperMode == "FALSE" || upperMode == "0" || upperMode == "DISABLE" || upperMode == "DISABLED" || upperMode == "")) ||
		(parsed == Marker && upperMode == "MARK")
}

// WriteRedact writes string with redaction into strings.Builder.
// This is compatible with TiDB's WriteRedact function.
func WriteRedact(build *strings.Builder, v string) {
	mode := GetRedactMode()
	switch mode {
	case Marker:
		build.WriteString("‹")
		build.WriteString(v)
		build.WriteString("›")
	case On:
		build.WriteString("?")
	default:
		build.WriteString(v)
	}
}
