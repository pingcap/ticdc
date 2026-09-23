# Opt-in local normal/redo subscription sharing

TiCDC schedules normal and redo dispatchers independently. With general EventStore sharing disabled, co-located dispatchers subscribe to the same upstream span twice and write two copies into EventStore. This experiment removes that duplicate ingestion for compatible local pairs without changing dispatcher placement or the normal/redo durability protocol.

## Configuration

On every TiCDC server, set:

```toml
[debug.event-store]
enable-redo-data-sharing = true
```

The default is `false`. For an isolated A/B test, leave `enable-data-sharing = false` in both runs. Restart TiCDC with the configuration; this is not a changefeed configuration or a live toggle. Existing duplicate subscriptions are not merged retroactively.

## Eligibility and correctness

- Both consumers belong to the same changefeed identity and have opposite normal/redo modes.
- They use exactly the same table span, keyspace ID, BDR filtering mode and low-latency mode.
- The incoming start timestamp is inside the subscription's retained checkpoint/resolved range. An uninitialized subscription is reusable at its original start timestamp, allowing a newly created table's two consumers to share its initial scan.
- Registration lookup and creation are serialized when the feature is enabled. Checkpoint advancement and detachment cannot race with checking the retained range and attaching the new consumer.
- Each dispatcher retains its own checkpoint, resolved notification and iterator. The existing minimum-subscriber checkpoint controls garbage collection. Removing one dispatcher does not unsubscribe the other; the last removal follows the existing EventStore cleanup policy.
- Incompatible registrations fall back to the original subscription creation path. Remote `onlyReuse` probes retain their original behavior, controlled by general data sharing.

This shares upstream delivery and EventStore writes, not dispatcher scans, decoding or downstream writes. It does not pair schedulers, move dispatchers, introduce cross-node sharing, or merge existing subscriptions after a reset. Two dispatchers on different EventStore nodes still ingest separate copies. General data sharing remains an independent, broader mechanism; its existing rules take precedence when the local pair is ineligible and general sharing is enabled.

## Validation procedure

1. Run the same binary and load with the new flag off, then on. Keep capture count, workload, redo and other settings fixed; use a fresh changefeed for each run.
2. First use one capture to make pair co-location deterministic. Then repeat with the intended multi-capture topology to measure the benefit achievable without scheduler changes.
3. Look for `reuse subscription for redo pair` logs. Correlate the dispatcher IDs with normal/redo registrations and compare live subscription count. In an exact-span, retained-range-compatible single-capture case, N physical spans need approximately N upstream subscriptions rather than 2N after registration settles. Table churn, retained-range misses and remote placement can prevent this ratio.
4. Compare upstream CDC event bytes, initial-scan bytes/tasks, EventStore write bytes, write stalls, CPU/RSS and checkpoint lag. Do not expect dispatcher scan bytes or sink output bytes to halve.
5. Verify row counts/checksums and redo recovery under the same DDL/table-churn workload. Also exercise a slow consumer, dispatcher removal/reset and capture failover; confirm that checkpoints catch up and no data is lost. These cluster-level checks are required before production use and are not replaced by unit tests.
6. For rollback, restart with the flag off and use a fresh changefeed for a clean comparison. No stored key format or RPC format changes are introduced.

The expected result is reduced ingestion work for eligible local pairs. A reduction in multi-node replication lag is a hypothesis to benchmark, not a measured result of this patch.
