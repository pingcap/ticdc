# `table_info_sharing` workload

This workload creates many tables with the same column layout and index layout, while making selected default values differ by table index.

It is intended for cases that want to stress table-info reuse/sharing with:

- Multiple tables that look structurally the same
- A wide mix of column types
- Different default values on selected columns across tables

Covered column families include:

- Integer types: `TINYINT`, `SMALLINT`, `MEDIUMINT`, `INT`, `BIGINT`, `BIGINT UNSIGNED`
- Numeric types: `DECIMAL`, `FLOAT`, `DOUBLE`
- Logical/bit types: `BOOLEAN`, `BIT`
- String/binary types: `CHAR`, `VARCHAR`, `TEXT`, `BLOB`, `BINARY`, `VARBINARY`
- Temporal types: `DATE`, `DATETIME`, `TIMESTAMP`, `TIME`, `YEAR`
- Collection/document types: `ENUM`, `SET`, `JSON`

Example:

```bash
./workload -action write \
  -database-host 127.0.0.1 \
  -database-port 4000 \
  -database-db-name test \
  -workload-type table_info_sharing \
  -table-count 16 \
  -thread 32 \
  -batch-size 32 \
  -percentage-for-update 0.5 \
  -percentage-for-delete 0.1
```
