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
	"encoding/hex"
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

// parseRedactMode converts string to redact mode string compatible with errors.RedactLogEnabled.
func parseRedactMode(mode string) string {
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

// isValidRedactMode checks if the given string is a valid redaction mode.
func isValidRedactMode(input string, parsed string) bool {
	upperMode := strings.ToUpper(strings.TrimSpace(input))
	return upperMode == "OFF" || upperMode == "ON" || upperMode == "MARKER" ||
		(parsed == perrors.RedactLogEnable && (upperMode == "TRUE" || upperMode == "1" || upperMode == "ENABLE" || upperMode == "ENABLED")) ||
		(parsed == perrors.RedactLogDisable && (upperMode == "FALSE" || upperMode == "0" || upperMode == "DISABLE" || upperMode == "DISABLED" || upperMode == "")) ||
		(parsed == perrors.RedactLogMarker && upperMode == "MARK")
}

// ParseRedactMode parses and validates the input string, returning the canonical redact mode.
// Returns empty string for empty input (means "not set", caller handles default).
// Returns an error for non-empty invalid modes.
// Valid inputs: "off", "on", "marker" (case-insensitive), plus aliases like "true"/"false", "enable"/"disable", "1"/"0".
func ParseRedactMode(input string) (string, error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return "", nil // Empty means "not set", caller handles default
	}
	parsed := parseRedactMode(trimmed)
	if !isValidRedactMode(trimmed, parsed) {
		return "", fmt.Errorf("invalid redact mode '%s': must be 'off', 'on', or 'marker'", input)
	}
	return parsed, nil
}

// RedactValue redacts a string value using the current redaction mode.
// Returns "?" for ON mode, wrapped value with ‹› for MARKER mode, original for OFF.
// In MARKER mode, embedded markers are escaped by doubling (‹ → ‹‹, › → ››)
// using TiDB's redact.String() to ensure log parsers can correctly identify redacted content.
func RedactValue(value string) string {
	mode := perrors.RedactLogEnabled.Load()
	switch mode {
	case perrors.RedactLogEnable:
		return "?" // ON mode: return "?" (not empty string like redact.String)
	case perrors.RedactLogMarker:
		return redact.String(mode, value) // MARKER mode: use TiDB's escaping
	default:
		return value // OFF mode: return as-is
	}
}

// RedactAny redacts any value by converting to string and applying redaction.
// Returns "?" for ON mode, wrapped value with ‹› for MARKER mode, original for OFF.
// In MARKER mode, embedded markers are escaped using TiDB's redact.String().
func RedactAny(value any) string {
	if value == nil {
		return "NULL"
	}
	return RedactValue(fmt.Sprintf("%v", value))
}

// RedactArgs redacts SQL arguments/parameters for logging.
// Returns "?" for each arg in ON mode, wrapped values for MARKER mode, originals for OFF.
// In MARKER mode, embedded markers in each argument are escaped using TiDB's redact.String().
func RedactArgs(args []interface{}) string {
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
			b.WriteString(RedactAny(arg))
		}
	}
	b.WriteString(")")
	return b.String()
}

// RedactKey redacts a byte slice key (like TiKV keys) using the current redaction mode.
// Returns "?" for ON mode (skips hex conversion), hex string wrapped with ‹› for MARKER mode,
// hex string for OFF. This properly handles MARKER mode unlike TiDB's redact.Key which only
// supports ON/OFF. Note: No marker escaping is needed because hex encoding eliminates any
// special characters including markers - the hex string will only contain 0-9 and A-F.
func RedactKey(key []byte) string {
	mode := perrors.RedactLogEnabled.Load()
	switch mode {
	case perrors.RedactLogEnable:
		return "?" // Skip hex conversion for ON mode - no need to process data we won't use
	case perrors.RedactLogMarker:
		return "‹" + strings.ToUpper(hex.EncodeToString(key)) + "›"
	default:
		return strings.ToUpper(hex.EncodeToString(key))
	}
}

// RedactBytes redacts a byte slice (like Kafka message content) using the current redaction mode.
// Returns "?" for ON mode, string value wrapped with ‹› for MARKER mode, string for OFF.
// In MARKER mode, embedded markers are escaped using TiDB's redact.String().
func RedactBytes(data []byte) string {
	return RedactValue(string(data))
}

// RedactStrings redacts each string in the slice using the current redaction mode.
// Returns a new slice with "?" for each element in ON mode, wrapped values for MARKER mode,
// original values for OFF. This preserves the slice structure for callers that need to
// process the redacted values further (e.g., dbutil.ReplacePlaceholder for logging).
// In MARKER mode, embedded markers in each element are escaped using TiDB's redact.String().
// Note: The Unicode markers ‹› (U+2039, U+203A) are distinct from SQL operators <> (U+003C, U+003E),
// so MARKER mode values work correctly with SQL placeholder substitution.
func RedactStrings(args []string) []string {
	if len(args) == 0 {
		return args
	}

	result := make([]string, len(args))
	for i, arg := range args {
		result[i] = RedactValue(arg)
	}
	return result
}
