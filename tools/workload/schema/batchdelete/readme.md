# Batch Delete Workload

This workload keeps the schema intentionally small and focuses on single-statement batch deletes:

```sql
DELETE FROM batch_deleteN ORDER BY id LIMIT <delete-batch-size>
```

Use `write` mode to keep inserting rows while delete workers continuously remove rows in batches:

```bash
./workload -action write \
  -database-host 127.0.0.1 \
  -database-port 4000 \
  -database-db-name test \
  -table-count 1 \
  -workload-type batch_delete \
  -thread 64 \
  -batch-size 1000 \
  -delete-batch-size 10000 \
  -row-size 8192 \
  -percentage-for-delete 0.5 \
  -total-row-count 100000000000
```

To drain already prepared rows with delete workers only, use:

```bash
./workload -action delete \
  -database-host 127.0.0.1 \
  -database-port 4000 \
  -database-db-name test \
  -table-count 1 \
  -workload-type batch_delete \
  -thread 16 \
  -delete-batch-size 10000 \
  -percentage-for-delete 1.0
```

`write` mode is recommended when the upstream needs to keep producing real delete events for a long-running test.

`-batch-size` controls rows per insert statement. `-delete-batch-size` controls rows per delete statement. `-row-size` controls payload bytes per inserted row.
