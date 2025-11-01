#!/bin/bash
# Copyright 2024 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

# This script VETS/CHECKS for log statements that may need redaction.
# It's designed to be deterministic and run during build time.
#
# This script does NOT mutate code - it only verifies.
#
# Exception Mechanism:
# To skip redaction check on a specific log statement, add a comment:
#   // skip-redaction: <reason>
# or
#   // nolint:redact <reason>
#
# Example:
#   log.Info("processing", zap.String("query", metadataQuery)) // skip-redaction: metadata only, no user data

set -euo pipefail

# Parse command line arguments
INCLUDE_TESTS=false
SHOW_HELP=false

for arg in "$@"; do
    case $arg in
        --include-tests)
            INCLUDE_TESTS=true
            shift
            ;;
        --help|-h)
            SHOW_HELP=true
            shift
            ;;
        *)
            # Unknown option
            ;;
    esac
done

# Show help if requested
if [ "$SHOW_HELP" = true ]; then
    cat <<EOF
TiCDC Log Redaction Checker

Usage: $0 [OPTIONS]

OPTIONS:
    --help, -h          Show this help message
    --include-tests     Include test files in validation (by default, tests are excluded)

DESCRIPTION:
    This script checks for log statements that may need redaction but don't have it.
    By default, it excludes test files (_test.go and tests/ directory) since those are
    not production code.

EXCEPTION MECHANISM:
    To mark a log statement as intentionally unredacted, add a comment:
        // skip-redaction: <reason why no user data>
    or
        // nolint:redact <reason>

EXAMPLES:
    # Check production code only (default)
    $0

    # Check including test files
    $0 --include-tests

    # Valid exception comment
    log.Info("metadata query",
        zap.String("query", q)) // skip-redaction: SELECT schema_name FROM information_schema, no user data

EXIT CODES:
    0 - All checks passed
    1 - Violations found

EOF
    exit 0
fi

# Colors for output
RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$REPO_ROOT"

# Sensitive field names that should be redacted
SENSITIVE_FIELDS=(
    "query"
    "sql"
    "stmt"
    "ddl"
    "dml"
    "value"
    "data"
    "row"
    "column"
    "record"
    "key"
    "handle"
    "datum"
    "args"
    "params"
    "arguments"
)

# Build exclude patterns based on flags - separate dirs and file patterns
EXCLUDE_DIRS=(
    "pkg/redact"      # Redaction package itself
    "vendor"          # Vendor directory
    "tools"           # Testing/workload tools
)

EXCLUDE_FILES=(
    "README.md"       # Documentation
    "*.pb.go"         # Generated protobuf files
    "mock_*.go"       # Mock files
)

# Files that are known to only log non-sensitive configuration/infrastructure paths
# These are skipped entirely to avoid needing skip-redaction comments
EXCLUDED_FILES=(
    "cmd/cdc/server/server.go"           # TLS cert/key file paths only
    "api/middleware/middleware.go"        # HTTP query parameters, not SQL
    "cmd/cdc/cli/cli_changefeed_create.go"  # Flag values, not data
    "pkg/config/"                         # Configuration files, not user data
    "cmd/kafka-consumer/writer.go"        # Consumer infrastructure logs
    "cmd/pulsar-consumer/writer.go"       # Consumer infrastructure logs
    "cmd/storage-consumer/main.go"        # Consumer infrastructure logs
)

# Conditionally add test exclusions
if [ "$INCLUDE_TESTS" = false ]; then
    EXCLUDE_FILES+=("*_test.go")
    EXCLUDE_DIRS+=("tests")
fi

# Build grep exclude arguments
EXCLUDE_ARGS=""
for dir in "${EXCLUDE_DIRS[@]}"; do
    EXCLUDE_ARGS="$EXCLUDE_ARGS --exclude-dir=$dir"
done
for file in "${EXCLUDE_FILES[@]}"; do
    EXCLUDE_ARGS="$EXCLUDE_ARGS --exclude=$file"
done

echo "======================================"
echo "TiCDC Log Redaction Checker"
echo "======================================"
echo ""

VIOLATIONS=0
WARNINGS=0

# Function to check if a file should be excluded
is_file_excluded() {
    local filepath="$1"
    for excluded in "${EXCLUDED_FILES[@]}"; do
        if [[ "$filepath" == *"$excluded"* ]]; then
            return 0  # File is excluded
        fi
    done
    return 1  # File is not excluded
}

# Function to check if a line has a skip exception
has_skip_exception() {
    local line="$1"
    # Check for skip-redaction or nolint:redact comments
    if echo "$line" | grep -qE '//.*skip-redaction:|//.*nolint:redact'; then
        return 0  # Has exception
    fi
    return 1  # No exception
}

# Function to check for unredacted zap.String with sensitive fields
check_zap_string() {
    local field=$1
    echo "Checking zap.String(\"$field\", ...) without redaction..."

    # Find zap.String with this field that doesn't have redact
    # Pattern: zap.String("field", <not redact.>)
    local all_results=$(grep -rn --include="*.go" $EXCLUDE_ARGS \
        -E "zap\.String\(\"$field\"," . 2>/dev/null | \
        grep -v "redact\." || true)

    if [ -z "$all_results" ]; then
        echo -e "${GREEN}✓ OK${NC}"
        return 0
    fi

    # Filter out lines with skip exceptions and excluded files
    local violations=""
    while IFS= read -r line; do
        local filepath=$(echo "$line" | cut -d: -f1)
        if ! is_file_excluded "$filepath" && ! has_skip_exception "$line"; then
            if [ -n "$violations" ]; then
                violations="$violations"$'\n'"$line"
            else
                violations="$line"
            fi
        fi
    done <<< "$all_results"

    if [ -n "$violations" ]; then
        echo -e "${RED}✗ Found unredacted zap.String(\"$field\", ...)${NC}"
        echo "$violations" | head -10
        local count=$(echo "$violations" | wc -l)
        if [ $count -gt 10 ]; then
            echo "  ... and $((count - 10)) more"
        fi
        echo ""
        echo "To mark as exception, add comment: // skip-redaction: <reason>"
        echo ""
        VIOLATIONS=$((VIOLATIONS + 1))
        return 1
    else
        echo -e "${GREEN}✓ OK (all have redact or skip-redaction)${NC}"
        return 0
    fi
}

# Function to check for zap.Any with sensitive fields (warning only)
check_zap_any() {
    local field=$1
    echo "Checking zap.Any(\"$field\", ...) (should prefer zap.String + redact)..."

    local all_results=$(grep -rn --include="*.go" $EXCLUDE_ARGS \
        -E "zap\.Any\(\"$field\"," . 2>/dev/null || true)

    if [ -z "$all_results" ]; then
        echo -e "${GREEN}✓ OK${NC}"
        return 0
    fi

    # Filter out lines with skip exceptions and excluded files
    local warnings=""
    while IFS= read -r line; do
        local filepath=$(echo "$line" | cut -d: -f1)
        if ! is_file_excluded "$filepath" && ! has_skip_exception "$line"; then
            if [ -n "$warnings" ]; then
                warnings="$warnings"$'\n'"$line"
            else
                warnings="$line"
            fi
        fi
    done <<< "$all_results"

    if [ -n "$warnings" ]; then
        echo -e "${YELLOW}⚠ Found zap.Any(\"$field\", ...) - consider zap.String + redact${NC}"
        echo "$warnings" | head -5
        local count=$(echo "$warnings" | wc -l)
        if [ $count -gt 5 ]; then
            echo "  ... and $((count - 5)) more"
        fi
        echo "  (These are warnings, not failures)"
        echo ""
        WARNINGS=$((WARNINGS + 1))
        return 0
    else
        echo -e "${GREEN}✓ OK${NC}"
        return 0
    fi
}

# Function to check for direct string concatenation in logs
check_string_concat() {
    echo "Checking for string concatenation with queries in logs..."

    local all_results=$(grep -rn --include="*.go" $EXCLUDE_ARGS \
        -E 'log\.(Info|Warn|Error|Debug|Fatal).*\+.*query|sql' . 2>/dev/null | \
        grep -v "redact\." || true)

    if [ -z "$all_results" ]; then
        echo -e "${GREEN}✓ OK${NC}"
        return 0
    fi

    # Filter out lines with skip exceptions and excluded files
    local warnings=""
    while IFS= read -r line; do
        local filepath=$(echo "$line" | cut -d: -f1)
        if ! is_file_excluded "$filepath" && ! has_skip_exception "$line"; then
            if [ -n "$warnings" ]; then
                warnings="$warnings"$'\n'"$line"
            else
                warnings="$line"
            fi
        fi
    done <<< "$all_results"

    if [ -n "$warnings" ]; then
        echo -e "${YELLOW}⚠ Found potential string concatenation in logs${NC}"
        echo "$warnings" | head -5
        local count=$(echo "$warnings" | wc -l)
        if [ $count -gt 5 ]; then
            echo "  ... and $((count - 5)) more"
        fi
        echo ""
        WARNINGS=$((WARNINGS + 1))
        return 0
    else
        echo -e "${GREEN}✓ OK${NC}"
        return 0
    fi
}

echo "Running checks..."
echo ""

# Check each sensitive field for zap.String
for field in "${SENSITIVE_FIELDS[@]}"; do
    check_zap_string "$field"
done

echo ""
echo "Additional checks..."
echo ""

# Check for zap.Any with sensitive data (warnings)
for field in "query" "sql" "row" "value" "data"; do
    check_zap_any "$field"
done

echo ""
check_string_concat

echo ""
echo "======================================"
echo "Summary"
echo "======================================"
echo -e "Violations: ${RED}$VIOLATIONS${NC}"
echo -e "Warnings:   ${YELLOW}$WARNINGS${NC}"
echo ""

if [ $VIOLATIONS -gt 0 ]; then
    echo -e "${RED}✗ FAILED: Found $VIOLATIONS violation(s)${NC}"
    echo ""
    echo "To fix violations:"
    echo "1. Import the redact package: import \"github.com/pingcap/ticdc/pkg/redact\""
    echo "2. Wrap sensitive data with redact functions:"
    echo "   - SQL queries: redact.SQL(query)"
    echo "   - Values: redact.Value(value)"
    echo "   - Keys: redact.Key(key)"
    echo "   - Arguments: redact.Args(args)"
    echo ""
    echo "OR, if the log statement genuinely doesn't contain user data:"
    echo "3. Add an exception comment on the same line:"
    echo "   zap.String(\"query\", metadataQuery)) // skip-redaction: metadata only"
    echo "   zap.String(\"query\", systemQuery)) // nolint:redact infrastructure query"
    echo ""
    echo "See pkg/redact/README.md for more details."
    echo ""
    exit 1
fi

if [ $WARNINGS -gt 0 ]; then
    echo -e "${YELLOW}⚠ PASSED with $WARNINGS warning(s)${NC}"
    echo ""
    echo "Warnings are informational and don't fail the build."
    echo "Consider addressing them to improve consistency."
    echo ""
else
    echo -e "${GREEN}✓ PASSED: All checks successful!${NC}"
    echo ""
fi

exit 0
