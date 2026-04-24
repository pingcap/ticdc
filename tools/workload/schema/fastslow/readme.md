# `fast_slow` workload

This workload is intended for fast-table / slow-table testing.

It creates two table groups in one run:

- Fast tables: narrow rows, light updates, insert-heavy traffic.
- Slow tables: wide rows, more secondary indexes, heavy updates.

When `-table-count` is greater than 1, the first half of tables are fast tables and
the remaining tables are slow tables.

With `-table-count 4 -table-start-index 0`, the created tables are:

- `fast_table_0`
- `fast_table_1`
- `slow_table_2`
- `slow_table_3`

The workload also skews DML dispatch:

- Inserts mostly hit fast tables.
- Updates and deletes mostly hit slow tables.

This means you can reproduce two effects at the same time:

- Fast tables keep advancing with steady DML.
- Slow tables accumulate heavier writes and can be targeted by extra DDL.

Recommended usage:

```bash
./bin/workload -action write \
  -database-host 127.0.0.1 \
  -database-port 4000 \
  -database-db-name test \
  -workload-type fast_slow \
  -table-count 4 \
  -thread 16 \
  -batch-size 16 \
  -row-size 4096 \
  -percentage-for-update 0.4
```

If you also want schema-change pressure only on slow tables, use fixed-mode DDL:

```bash
./bin/workload -action write \
  -database-host 127.0.0.1 \
  -database-port 4000 \
  -database-db-name test \
  -workload-type fast_slow \
  -table-count 4 \
  -thread 16 \
  -batch-size 16 \
  -row-size 4096 \
  -percentage-for-update 0.4 \
  -enable-ddl \
  -ddl-worker 1 \
  -ddl-timeout 2m
```

`-enable-ddl` always targets only `slow_table_*` in this workload. If you want to
override the default DDL rates, you can still pass:

- `-ddl-rate-add-column`
- `-ddl-rate-drop-column`
- `-ddl-rate-add-index`
- `-ddl-rate-drop-index`
- `-ddl-rate-truncate-table`
