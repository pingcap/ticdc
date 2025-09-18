# MySQL Sink Encoding Test

## Overview

This integration test is designed to verify the encoding and decoding correctness of TiCDC's MySQL sink. It ensures data consistency between upstream (source) and downstream (MySQL sink) after data synchronization.

## Test Objectives

1. **Data Consistency Verification**: Ensure that data in upstream and downstream databases are identical after synchronization
2. **Encoding/Decoding Validation**: Test various SQL data types and their proper encoding/decoding
3. **Large Volume Testing**: Test with different data volumes (small, medium, large)
4. **DDL and DML Operations**: Test both schema changes and data manipulation operations

## Test Structure

### Files

- `run.sh`: Main test execution script
- `conf/diff_config.toml`: Configuration for data consistency checking
- `data/ddl_tests.sql`: DDL operations testing various data types and table structures
- `data/dml_tests.sql`: DML operations testing INSERT, UPDATE, DELETE with different data types
- `data/large_volume_tests.sql`: Large volume data testing with different row counts

### Test Categories

#### 1. DDL Tests (`ddl_tests.sql`)

Tests various table structures and data types:

- **Basic Data Types**: INT, VARCHAR, DECIMAL, DATETIME, BLOB, JSON, etc.
- **Complex Data Types**: ENUM, SET, etc.
- **Table Constraints**: Primary keys, unique constraints, foreign keys, indexes
- **Primary Key Types**: 
  - Single column primary keys
  - Multi-column composite primary keys (2-3 columns)
  - String-based composite primary keys
  - Mixed data type composite primary keys
- **Character Sets**: UTF8, UTF8MB4, Latin1, ASCII
- **Default Values**: Various default value scenarios
- **Check Constraints**: Data validation constraints

#### 2. DML Tests (`dml_tests.sql`)

Tests data manipulation operations:

- **INSERT Operations**: Various data types with edge cases
- **UPDATE Operations**: 
  - Single and multiple column updates
  - Updates on different primary key types
  - Business scenario updates (order status, inventory, etc.)
- **DELETE Operations**: 
  - Row deletion testing
  - Deletion on composite primary keys
  - Business scenario deletions (discontinued products, terminated employees, etc.)
- **Mixed Operations**: 
  - Real-world business workflow simulation
  - E-commerce order processing
  - Inventory management
  - Financial transactions
  - User and employee management
- **NULL Values**: Handling of NULL data
- **Special Characters**: Unicode and special character handling
- **Edge Cases**: Maximum/minimum values, empty strings, etc.

#### 3. Large Volume Tests (`large_volume_tests.sql`)

Tests with different data volumes:

- **Small Volume**: 10 rows
- **Medium Volume**: 100 rows  
- **Large Volume**: 1000 rows
- **Mixed Operations**: INSERT, UPDATE, DELETE on large datasets

## Key Features

### Primary Key Requirement

All tables created in this test include primary keys as required by the test specifications. No DDL operations remove existing primary keys.

### Data Type Coverage

The test covers a comprehensive range of SQL data types:

- **Numeric Types**: TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL
- **String Types**: CHAR, VARCHAR, TEXT, LONGTEXT
- **Date/Time Types**: DATE, TIME, DATETIME, TIMESTAMP, YEAR
- **Binary Types**: BLOB, TINYBLOB, MEDIUMBLOB, LONGBLOB, BINARY, VARBINARY
- **JSON Type**: JSON data with nested structures
- **Spatial Types**: GEOMETRY, POINT, LINESTRING, POLYGON, etc.
- **Other Types**: ENUM, SET, BIT, BOOLEAN

### Test Scenarios

1. **Basic Functionality**: Simple table creation and data insertion
2. **Complex Structures**: Multi-column primary keys, foreign keys, indexes
3. **Primary Key Variations**: 
   - Single column primary keys
   - Multi-column composite primary keys (2-3 columns)
   - String-based composite primary keys
   - Mixed data type composite primary keys
4. **Character Encoding**: Various character sets and collations
5. **Data Volume**: Different amounts of data to test performance and accuracy
6. **Edge Cases**: NULL values, maximum/minimum values, special characters
7. **Mixed Operations**: Combination of DDL and DML operations
8. **Real-world Business Scenarios**:
   - E-commerce order processing workflow
   - Inventory management operations
   - Financial transaction processing
   - User and employee management
   - Data cleanup operations

## Execution

The test follows the standard TiCDC integration test pattern:

1. Start TiDB cluster (upstream and downstream)
2. Start CDC server
3. Create changefeed with MySQL sink
4. Execute test SQL files
5. Verify data consistency using sync_diff_inspector
6. Clean up resources

## Expected Results

- All tables should be created successfully in downstream
- All data should be synchronized correctly
- Data consistency check should pass
- No encoding/decoding errors should occur

## Dependencies

- TiCDC binary (`cdc.test`)
- MySQL client
- sync_diff_inspector tool
- Standard TiCDC test utilities

## Notes

- This test is specifically designed for MySQL sink and will be skipped for other sink types
- The test uses the standard TiCDC test infrastructure and utilities
- All comments in the test files are written in English as per requirements
- The test follows existing TiCDC integration test patterns and conventions 