# Create a paused changefeed

To validate and save a changefeed without starting replication, set `pause` to
`true` in the `POST /api/v2/changefeeds` JSON request:

```json
{
  "changefeed_id": "example",
  "sink_uri": "mysql://user:password@downstream:4000/",
  "pause": true
}
```

For CLI creation, put `pause = true` at the top level of the TOML configuration,
before any section headers:

```toml
pause = true

[filter]
rules = ["test.*"]
```

```shell
cdc cli changefeed create --changefeed-id example --sink-uri 'mysql://user:password@downstream:4000/' --config changefeed.toml
cdc cli changefeed resume --changefeed-id example
```

Omitting `pause`, or setting it to `false`, keeps the existing automatic startup
behavior. The option only affects creation; changing a configuration file does
not pause or resume an existing changefeed. Use the pause/resume commands for
existing tasks.

Paused creation still performs the normal configuration, table, sink, timestamp,
and GC checks. It may contact the downstream during validation. The saved state
and API response are `stopped`, with the checkpoint initialized to `start_ts`.
If `start_ts` is omitted, creation chooses the current TSO as usual. A restart
preserves the stopped state, and resume without a checkpoint override continues
from the saved checkpoint. Existing paused-changefeed GC protection and GC TTL
limits apply; this option does not provide indefinite data retention.

The first resume retains fresh-changefeed initialization, including cleanup of
stale MySQL `ddl_ts_v1` records for a reused changefeed name. An internal pending
bootstrap marker survives coordinator restarts and is cleared after the current
maintainer reports successful bootstrap. Subsequent resumes use normal recovery.
