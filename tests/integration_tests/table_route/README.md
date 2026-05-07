# Table Route Integration Test

This test verifies that table route works correctly for MySQL sinks.

## What it tests

1. **Schema rewrite**: Routes `source_db.*` to `target_db.*`
2. **Table rewrite**: Appends `_routed` suffix to table names
3. **DML output**: INSERT, UPDATE, DELETE operations use the routed schema/table
4. **DDL output**: CREATE DATABASE, CREATE TABLE, ALTER TABLE, RENAME TABLE, view DDL, partition DDL, TRUNCATE TABLE, DROP TABLE, and DROP DATABASE use the routed schema/table

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

1. Create changefeed with table route rules
2. Execute one SQL workload on the upstream source schema
3. Wait until the routed finish marker appears downstream
4. Use sync diff with table route rules to compare upstream source tables and downstream routed target tables
5. Verify dropped and renamed-away target tables do not remain downstream
6. Drop the upstream source database and verify the routed downstream database is gone
