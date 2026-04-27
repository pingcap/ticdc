# `table_info_sharing` workload

This workload creates table groups for validating TableInfo/column-schema sharing. Tables in the same variant have identical table info except table name/table ID, so they should be shareable. Different variants intentionally differ in table-info fields and should not share column schema.

It is intended for cases that want to stress table-info reuse/sharing with:

- Multiple tables that are exactly shareable within the same variant
- Tables with the same visible column/index layout but different defaults
- Tables with different type attributes, nullability, index layouts, generated columns, or extra columns
- A wide mix of column types and insert/update/delete traffic

Variants are assigned by `table_index % 7`:

- `0`: base schema; repeated by table `7`, `14`, ... to verify positive sharing
- `1`: same columns and indexes as variant `0`, but different defaults to verify defaults prevent sharing
- `2`: different type attributes, such as wider `DECIMAL`, `VARCHAR`, `ENUM`, `SET`, `BINARY`, and `VARBINARY`
- `3`: different nullability flags on selected columns
- `4`: different index layout and a prefix index on `c_varchar`
- `5`: extra stored generated column
- `6`: extra normal columns with defaults

Covered column families include:

- Integer types: `TINYINT`, `SMALLINT`, `MEDIUMINT`, `INT`, `BIGINT`, `BIGINT UNSIGNED`
- Numeric types: `DECIMAL`, `FLOAT`, `DOUBLE`
- Logical/bit types: `BOOLEAN`, `BIT`
- String/binary types: `CHAR`, `VARCHAR`, `TEXT`, `BLOB`, `BINARY`, `VARBINARY`
- Temporal types: `DATE`, `DATETIME`, `TIMESTAMP`, `TIME`, `YEAR`
- Collection/document types: `ENUM`, `SET`, `JSON`
- Generated columns: stored generated column variant

Example:

```bash
./workload -action write \
  -database-host 127.0.0.1 \
  -database-port 4000 \
  -database-db-name table_info_sharing \
  -workload-type table_info_sharing \
  -table-count 14 \
  -thread 32 \
  -batch-size 32 \
  -percentage-for-update 0.5 \
  -percentage-for-delete 0.1
```

Use `-table-count 14` or higher to get at least two tables per variant. Validate upstream and downstream consistency with sync-diff-inspector after TiCDC catches up:

```bash
sync_diff_inspector --config tools/workload/examples/table_info_sharing_diff_config.toml
```
