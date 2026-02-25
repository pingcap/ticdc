# Sink Routing Integration Test

This test verifies that schema and table routing works correctly for MySQL sinks.

## What it tests

1. **Schema routing**: Routes `source_db.*` to `target_db.*`
2. **Table routing**: Appends `_routed` suffix to table names
3. **DML routing**: INSERT, UPDATE, DELETE operations use the routed schema/table
4. **DDL routing**: CREATE TABLE, ALTER TABLE use the routed schema/table

## Configuration

The test uses the following routing rules in `conf/changefeed.toml`:

```toml
[[sink.dispatchers]]
matcher = ['source_db.*']
schema = 'target_db'
table = '{table}_routed'
```

This routes:
- `source_db.users` -> `target_db.users_routed`
- `source_db.orders` -> `target_db.orders_routed`
- etc.

## Test flow

1. Create source database and tables in upstream
2. Create target database in downstream
3. Create changefeed with routing rules
4. Execute DML and DDL operations on source tables
5. Verify data appears in routed target tables
6. Verify data does NOT appear in source schema in downstream
