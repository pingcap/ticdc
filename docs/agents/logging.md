# Logging Guidelines

Read this before adding, removing, or rewriting logs.

- Use structured logs via `github.com/pingcap/log` with `zap` fields.
- Treat logs as operational signals, not control-flow comments. Keep normal paths quiet.
- Default `INFO`/`WARN` logs should record high-value lifecycle events, state changes, external dependency abnormalities, or invariant violations.
- Choose log levels by required action:
  - `ERROR`: correctness, availability, or key progress is affected and needs attention.
  - `WARN`: the system is abnormal but can continue through recovery, retry, fallback, or degraded behavior.
  - `INFO`: key lifecycle events, important state changes, important configuration, or summary information.
  - `DEBUG`: bounded, low-frequency diagnostics with clear troubleshooting value.
- Do not add `DEBUG` logs by default. Delete low-value logs instead of moving them to `DEBUG`.
- Keep `message` stable and concise: summarize what happened, why it happened, and what the system will do next.
- Put object details in stable camelCase `zap` fields. For changefeed-scoped logs always use two fields: `keyspace` (keyspace name) and `changefeedID` (changefeed name). Never encode `keyspace/name` as a single value, and never name the changefeed field `changefeed`.
- One concept has one field name. Identifiers end with `ID`; timestamps end with `Ts`. Use `tableID` for the numeric table ID and `tableSpan` for the formatted key range, never `table` for either. Use `ddlQuery` for DDL text and `zap.Error(err)` for errors. Do not put a formatted range in a field named `span`, `subSpan`, or `dispatcherSpan`; when one log carries both the dispatcher's and the subscription's range, name them `tableSpan` and `subscriptionTableSpan`. Summarize other structured values in explicit fields instead of passing objects to `zap.Any`.
- Order fields the same way everywhere: `keyspace`, `changefeedID`, `dispatcherID`, `subscriptionID`, `tableID`, `tableSpan`, `regionID`, then `startTs`, `commitTs`, `checkpointTs`, `resolvedTs`, then payload fields such as `ddlQuery`, `state`, `result`, `duration`, `count`, with `error` last.
- Message strings should not include function names and should avoid `-` (use spaces instead).
- Do not unit-test log messages, fields, or formatting. Tests may only assert that sensitive values are masked.
- Avoid per-object or per-iteration logs, duplicated logs on the same error path, large objects, raw payloads, and long error dumps in default logs.
- Use metrics for counts, scale, frequency, and trends; use windowed summaries or representative samples for high-cardinality events.
- Before adding, keeping, or rewriting a log, verify that it answers a real diagnostic question, identifies the object, reason, action, and impact, and will not grow linearly with object count or retry/loop frequency.
