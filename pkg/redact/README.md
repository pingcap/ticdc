# TiCDC Log Redaction Package

## Overview

The `redact` package provides comprehensive log redaction functionality for TiCDC to prevent sensitive user data from appearing in logs. This implementation is compatible with TiDB, TiKV, and PD redaction systems, ensuring consistent security practices across the TiDB ecosystem.

## Features

- **Three Redaction Modes**:
  - `OFF`: No redaction (default) - sensitive information is logged as-is
  - `ON`: Complete redaction - sensitive information is replaced with `"?"`
  - `MARKER`: Marker mode - sensitive information is wrapped with `‹` and `›` markers for post-processing

- **Thread-Safe**: All operations are safe for concurrent use across multiple goroutines

- **Performance Optimized**: Minimal overhead with efficient string building and atomic operations

- **Comprehensive Coverage**: Supports redacting:
  - SQL queries and DDL statements
  - Row data and column values
  - Keys (table keys, index keys, row keys)
  - Arguments and parameters
  - Byte slices and binary data

## Usage

### Basic Redaction

```go
import "github.com/pingcap/ticdc/pkg/redact"

// Set the redaction mode (typically done at initialization)
redact.SetRedactMode(redact.Marker)

// Redact sensitive data in logs
sql := "INSERT INTO users VALUES (1, 'john@email.com', 'password123')"
log.Info("Executing SQL", zap.String("query", redact.SQL(sql)))
// Output with MARKER mode: Executing SQL {"query": "‹INSERT INTO users VALUES (1, 'john@email.com', 'password123')›"}

// Redact row values
value := "sensitive_user_data"
log.Debug("Processing row", zap.String("value", redact.Value(value)))
// Output with ON mode: Processing row {"value": "?"}

// Redact keys
key := []byte{0x12, 0x34, 0xAB, 0xCD}
log.Debug("Processing key", zap.String("key", redact.Key(key)))
// Output with MARKER mode: Processing key {"key": "‹1234ABCD›"}

// Redact arguments
args := []interface{}{1, "john", "password123"}
log.Debug("SQL args", zap.String("args", redact.Args(args)))
// Output with MARKER mode: SQL args {"args": "(‹1›, ‹john›, ‹password123›)"}
```

### Configuration

```go
// Initialize from boolean (for backward compatibility)
redact.InitRedact(true)  // Enables ON mode
redact.InitRedact(false) // Disables (OFF mode)

// Parse from configuration string
mode := redact.ParseRedactMode("MARKER")  // Supports: ON, OFF, MARKER, TRUE, FALSE, etc.
redact.SetRedactMode(mode)

// Check if redaction is enabled
if redact.NeedRedact() {
    // Redaction is active
}

// Get current mode
currentMode := redact.GetRedactMode()
```

### Advanced Usage

```go
// Using fmt.Stringer with redaction
type MyType struct {
    Data string
}

func (m MyType) String() string {
    return m.Data
}

obj := MyType{Data: "sensitive"}
stringer := redact.StringerWithCurrentMode(obj)
log.Info("Object", zap.Stringer("obj", stringer))

// Writing redacted content to string builder
var builder strings.Builder
redact.WriteRedact(&builder, "sensitive data")
result := builder.String()
```

## Implementation in TiCDC

### MySQL Sink

The MySQL sink has been updated to redact:
- SQL statements in debug logs
- Row values and arguments
- Query parameters

Example from `pkg/sink/mysql/sql_builder.go`:
```go
// Before
log.Debug("Query:", sql)
log.Debug("Args:", args)

// After
log.Debug("Query:", redact.SQL(sql))
log.Debug("Args:", redact.Args(args))
```

### Syncpoint Operations

Syncpoint queries are redacted in `pkg/sink/mysql/mysql_writer_for_syncpoint.go`:
```go
log.Info("exec syncpoint ts query", zap.String("query", redact.SQL(query)))
```

## Integration Guide

To add redaction to new code:

1. **Import the package**:
   ```go
   import "github.com/pingcap/ticdc/pkg/redact"
   ```

2. **Identify sensitive data**:
   - SQL queries → use `redact.SQL()`
   - User data/values → use `redact.Value()`
   - Keys/binary data → use `redact.Key()`
   - Arguments lists → use `redact.Args()`

3. **Apply redaction**:
   ```go
   // Before
   log.Info("SQL query", zap.String("sql", sqlQuery))

   // After
   log.Info("SQL query", zap.String("sql", redact.SQL(sqlQuery)))
   ```

## Configuration in Production

The redaction mode should be configured based on your deployment environment:

### Development/Testing
```toml
[security]
  redact-info-log = "OFF"  # See all data for debugging
```

### Staging
```toml
[security]
  redact-info-log = "MARKER"  # Allow recovery with authorization
```

### Production
```toml
[security]
  redact-info-log = "MARKER"  # Recommended for production
  # Or use "ON" for maximum security
```

## Marker Mode and Post-Processing

When using `MARKER` mode, sensitive data is wrapped with `‹` and `›` markers:

```
Original: INSERT INTO users VALUES (1, 'john')
Redacted: ‹INSERT INTO users VALUES (1, 'john')›
```

This allows:
1. **Audit trails**: Know that sensitive data was logged without exposing it
2. **Authorized recovery**: Post-process logs to recover data with proper authorization
3. **Compliance**: Meet regulatory requirements while maintaining debuggability

### Marker Escaping

Existing markers in data are properly escaped by doubling them:
```
Input:  "data with ‹ and › markers"
Output: "‹data with ‹‹ and ›› markers›"
```

## Performance Considerations

- **Minimal Overhead**: String operations are optimized with `strings.Builder`
- **Atomic Operations**: Mode changes use atomic values for thread safety
- **Zero Allocation** (OFF mode): When redaction is disabled, strings pass through with no allocation

## Compatibility

This package is designed to be compatible with:
- **TiDB**: `tidb_redact_log` system variable (ON, OFF, MARKER)
- **TiKV**: `security.redact-info-log` configuration
- **PD**: `security.redact-info-log` configuration

## Testing

The package includes comprehensive tests covering:
- All redaction modes
- Concurrent access scenarios
- Edge cases (empty strings, nil values, special characters)
- Marker escaping
- Performance benchmarks

Run tests:
```bash
go test github.com/pingcap/ticdc/pkg/redact -v
```

Run benchmarks:
```bash
go test github.com/pingcap/ticdc/pkg/redact -bench=.
```

## Best Practices

1. **Always redact user data**: Any data that could identify users or contain sensitive information
2. **Use semantic functions**: Use `SQL()`, `Value()`, `Key()` instead of just `String()` for clarity
3. **Configure appropriately**: Use MARKER in production for balance of security and debuggability
4. **Test both modes**: Ensure your code works correctly with redaction ON and OFF
5. **Don't log unnecessarily**: Even with redaction, minimize logging of sensitive data

## Security Considerations

- **PII Protection**: Prevents personally identifiable information (PII) from appearing in logs
- **Compliance**: Helps meet GDPR, CCPA, and other privacy regulations
- **Audit Requirements**: Marker mode allows audit trails without data exposure
- **Defense in Depth**: Adds an extra layer of security even if logs are compromised

## Vetting Unredacted Log Statements

To help identify log statements that may need redaction, use these grep commands:

### Find Potentially Unredacted Logs

```bash
# Find zap.String with sensitive field names that lack redaction
# This searches for patterns like zap.String("query", ...) without redact.
grep -rn --include="*.go" \
  -E 'zap\.String\("(query|sql|stmt|value|data|row|column|record|key|handle|datum|arg)' \
  pkg/ | grep -v 'redact\.'

# Find zap.Any with sensitive data (should often use redact instead)
grep -rn --include="*.go" \
  -E 'zap\.Any\("(query|sql|stmt|row|value|data)' \
  pkg/ | grep -v '_test.go'

# Find fmt.Sprintf or string concatenation with queries in logs
grep -rn --include="*.go" \
  -E 'log\.(Info|Warn|Error|Debug).*fmt\.Sprintf.*query|sql|value' \
  pkg/

# Comprehensive check - finds all zap logging of sensitive fields
grep -rn --include="*.go" --exclude-dir=redact \
  -E 'zap\.(String|Any|Reflect)\("(query|sql|stmt|ddl|dml|value|data|row|column|record|key|handle|datum|insert|update|delete|select)"' \
  . | grep -v 'redact\.' | grep -v '_test\.go' | grep -v 'vendor/'
```

### Usage

Run these commands from the repository root to find log statements that may need redaction:

```bash
cd /path/to/ticdc
./scripts/check-redaction.sh --check
```

### What to Look For

Log statements needing redaction typically contain:
- **SQL/DDL queries**: `query`, `sql`, `stmt`, `ddl`
- **Row data**: `row`, `value`, `data`, `column`
- **Keys**: `key`, `handle`, `record`
- **Arguments**: `args`, `params`, `arguments`

**False Positives**: Some logs with these field names don't need redaction:
- Metadata: `table` name, `schema` name, `type`, `version`
- Metrics: `count`, `size`, `duration`
- Infrastructure: `node`, `region`, `store`

### Exception Mechanism

If a log statement genuinely doesn't contain user data (e.g., metadata, system queries, infrastructure info), you can add an exception:

```go
// Option 1: skip-redaction with reason
log.Info("system query",
    zap.String("query", metadataQuery)) // skip-redaction: metadata query, no user data

// Option 2: nolint:redact with reason
log.Debug("processing",
    zap.String("sql", systemSQL)) // nolint:redact infrastructure query only

// Both formats work - choose the one that fits your team's conventions
```

**Important**: Always provide a reason explaining why redaction isn't needed. This helps reviewers understand the exception is intentional.

## Build-Time Validation

TiCDC includes automated checks to catch missing redaction at build time.

### Automatic Validation

The build system automatically validates redaction before compilation:

```bash
# Run validation manually
make check-redaction

# Validation runs automatically during build
make build

# Skip validation (not recommended)
make build SKIP_REDACTION_CHECK=1
```

### Validation Rules

The checker enforces:
1. All `zap.String()` with sensitive field names must use `redact.*()` functions
2. All `zap.Any()` with SQL/row data should use `zap.String()` + `redact.*()`
3. No raw query/value concatenation in log statements

### CI Integration

In CI/CD pipelines:
```yaml
# .github/workflows/ci.yml example
- name: Check Log Redaction
  run: make check-redaction
```

### Adding to Pre-commit Hooks

```bash
# .git/hooks/pre-commit
#!/bin/bash
make check-redaction || exit 1
```

## Future Enhancements

Potential future additions:
- Integration with configuration system for runtime mode changes
- Metrics for redaction operations
- Custom redaction rules per changefeed
- Selective redaction based on data sensitivity levels
- IDE plugins for real-time redaction hints
