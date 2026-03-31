# Table Route Integration Test

This test verifies that table route works correctly for MySQL sinks.

## What it tests

1. **Schema rewrite**: Routes `source_db.*` to `target_db.*`
2. **Table rewrite**: Appends `_routed` suffix to table names
3. **DML output**: INSERT, UPDATE, DELETE operations use the routed schema/table
4. **DDL output**: CREATE TABLE, ALTER TABLE use the routed schema/table

## Configuration

The test uses the following routing rules in `conf/changefeed.toml`:

```toml
[[sink.dispatchers]]
matcher = ['source_db.*']
target-schema = 'target_db'
target-table = '{table}_routed'
```

This routes:
- `source_db.users` -> `target_db.users_routed`
- `source_db.orders` -> `target_db.orders_routed`
- etc.

## Test flow

1. Create source database and tables in upstream
2. Create target database in downstream
3. Create changefeed with table route rules
4. Execute DML and DDL operations on source tables
5. Verify data appears in routed target tables
6. Verify data does NOT appear in source schema in downstream
