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
- Put object details in stable camelCase `zap` fields such as `changefeedID`, `nodeID`, `dispatcherID`, `regionID`, `subscriptionID`, and `requestID`.
- Message strings should not include function names and should avoid `-` (use spaces instead).
- Avoid per-object or per-iteration logs, duplicated logs on the same error path, large objects, raw payloads, and long error dumps in default logs.
- Use metrics for counts, scale, frequency, and trends; use windowed summaries or representative samples for high-cardinality events.
- Before adding, keeping, or rewriting a log, verify that it answers a real diagnostic question, identifies the object, reason, action, and impact, and will not grow linearly with object count or retry/loop frequency.
