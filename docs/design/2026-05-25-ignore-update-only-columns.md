# Ignore Update Only Columns Design

## Background

TiCDC already supports table and event filters under `filter.event-filters`.
The new requirement adds `ignore-update-only-columns` to the same section:

```toml
[[filter.event-filters]]
matcher = ["test.user"]
ignore-update-only-columns = ["version", "updated_at"]
```

For Kafka downstream only, an UPDATE row should be filtered out when every changed
column is included in the configured column list. INSERT and DELETE rows are not
affected.

The current DML filter is executed in EventService while raw KV entries are
decoded:

```text
DispatcherManager config
  -> EventCollector DispatcherRequest
  -> EventService SharedFilterStorage
  -> eventScanner / dmlProcessor
  -> DMLEvent.AppendRow
  -> filter.ShouldIgnoreDML
  -> Dispatcher
  -> Sink
```

Because this filter path is upstream of all sinks, the Kafka-only scope must be
represented explicitly. Otherwise adding the rule to `pkg/filter` would also
drop events for MySQL, Pulsar, storage, blackhole, and redo.

## Goals

1. Add `ignore-update-only-columns` under `[[filter.event-filters]]`.
2. Filter UPDATE rows when all changed columns are configured ignored columns.
3. Keep UPDATE rows when any changed column is not configured.
4. Keep UPDATE rows when a primary key or unique key column changes.
5. Log a warning and ignore configured columns that do not exist in the table
   schema.
6. Honor the feature only for Kafka dispatchers.
7. Keep existing table filters, event filters, expression filters, and non-Kafka
   downstream behavior unchanged.

## Non-Goals

1. Do not support MySQL, Pulsar, storage, blackhole, or redo in this iteration.
2. Do not change the existing `ignore-update-new-value-expr` or
   `ignore-update-old-value-expr` semantics.
3. Do not reject a non-Kafka changefeed that contains this config. The config is
   accepted but not evaluated unless the dispatcher writes to Kafka.
4. Do not add a new shared abstraction outside the filter package unless another
   component needs it later.

## Configuration And Wire Format

Add a new field to the repository config model:

```go
// pkg/config/filter.go
type EventFilterRule struct {
    Matcher                 []string       `toml:"matcher" json:"matcher"`
    IgnoreEvent             []bf.EventType `toml:"ignore-event" json:"ignore-event"`
    IgnoreSQL               []string       `toml:"ignore-sql" json:"ignore-sql"`
    IgnoreInsertValueExpr   *string        `toml:"ignore-insert-value-expr" json:"ignore-insert-value-expr,omitempty"`
    IgnoreUpdateNewValueExpr *string       `toml:"ignore-update-new-value-expr" json:"ignore-update-new-value-expr,omitempty"`
    IgnoreUpdateOldValueExpr *string       `toml:"ignore-update-old-value-expr" json:"ignore-update-old-value-expr,omitempty"`
    IgnoreDeleteValueExpr   *string        `toml:"ignore-delete-value-expr" json:"ignore-delete-value-expr,omitempty"`
    IgnoreUpdateOnlyColumns []string       `toml:"ignore-update-only-columns" json:"ignore-update-only-columns,omitempty"`
}
```

Add the corresponding OpenAPI model field:

```go
// api/v2/model.go
IgnoreUpdateOnlyColumns []string `json:"ignore_update_only_columns,omitempty"`
```

Add the protobuf fields used by dispatcher registration:

```proto
message EventFilterRule {
    repeated string matcher = 1;
    repeated string ignore_event = 2;
    repeated string ignore_sql = 3;
    string ignore_insert_value_expr = 4;
    string ignore_update_new_value_expr = 5;
    string ignore_update_old_value_expr = 6;
    string ignore_delete_value_expr = 7;
    repeated string ignore_update_only_columns = 8;
}

message DispatcherRequest {
    ...
    string txn_atomicity = 19;
    bool enable_ignore_update_only_columns = 20;
}
```

`enable_ignore_update_only_columns` is set by downstream dispatcher registration.
It is true only when the dispatcher writes to Kafka. This keeps the shared
changefeed filter config intact while making this rule conditional per
dispatcher.

After changing `eventpb/event.proto`, regenerate `eventpb/event.pb.go` with:

```sh
make generate-protobuf
```

Update conversion points:

1. `api/v2/model.go`
   - `EventFilterRule.ToInternalEventFilterRule`
   - `ToAPIEventFilterRule`
2. `downstreamadapter/dispatchermanager/helper.go`
   - `toEventFilterRulePB`
3. `pkg/filter/filter.go`
   - `SharedFilterStorage.GetOrSetFilter`
4. `pkg/messaging/message.go` if the dispatcher request wrapper needs an
   accessor for the new protobuf field.

## Kafka Scope Control

Add a method to the downstream dispatcher service interface:

```go
type DispatcherService interface {
    ...
    EnableIgnoreUpdateOnlyColumns() bool
}
```

Implement it on `BasicDispatcher`:

```go
func (d *BasicDispatcher) EnableIgnoreUpdateOnlyColumns() bool {
    return d.sink.SinkType() == common.KafkaSinkType
}
```

Set the protobuf field in both register and reset requests:

```go
// downstreamadapter/eventcollector/dispatcher_session.go
EnableIgnoreUpdateOnlyColumns: s.target.EnableIgnoreUpdateOnlyColumns(),
```

Expose the field through `eventservice.DispatcherInfo`, then pass it into
`dmlProcessor`:

```go
processor := newDMLProcessor(
    s.mounter,
    s.schemaGetter,
    dispatcher.filter,
    dispatcher.info.IsOutputRawChangeEvent(),
    dispatcher.info.EnableIgnoreUpdateOnlyColumns(),
    s.mode,
)
```

This is intentionally a runtime option, not a config rewrite. It avoids a
problem where the same changefeed can have normal dispatchers and redo
dispatchers at the same time. The filter object may be shared by changefeed, but
the Kafka-only decision is per dispatcher.

## Filter Implementation

Add a small filter component under `pkg/filter`, for example
`update_only_columns_filter.go`.

Suggested structs:

```go
type updateOnlyColumnsFilter struct {
    rules []*updateOnlyColumnsRule
}

type updateOnlyColumnsRule struct {
    mu sync.Mutex

    tableMatcher  tfilter.Filter
    configured    []string
    caseSensitive bool

    // tableID -> cached resolution for a table schema version.
    tables map[int64]resolvedUpdateOnlyColumns
}

type resolvedUpdateOnlyColumns struct {
    updateTS     uint64
    ignoredColID map[int64]struct{}
}
```

`newUpdateOnlyColumnsFilter(cfg, caseSensitive)` should:

1. Skip rules where `len(IgnoreUpdateOnlyColumns) == 0`.
2. Parse `Matcher` with `table-filter`.
3. Wrap the matcher with `tfilter.CaseInsensitive` when `caseSensitive` is
   false.
4. Keep the raw configured column names for per-table schema resolution.

Add it to the main filter:

```go
type filter struct {
    tableFilter             tfilter.Filter
    dmlExprFilter           *dmlExprFilter
    sqlEventFilter          *sqlEventFilter
    updateOnlyColumnsFilter *updateOnlyColumnsFilter
    ignoreTxnStartTs        []uint64
    forceReplicate          bool
}
```

Change the DML filter call to accept an option:

```go
type DMLFilterContext struct {
    EnableIgnoreUpdateOnlyColumns bool
}

ShouldIgnoreDML(
    dmlType common.RowType,
    preRow, row chunk.Row,
    tableInfo *common.TableInfo,
    startTs uint64,
    ctx DMLFilterContext,
) (bool, error)
```

`DMLEvent.AppendRow`, `TxnEvent.AppendRow`, and `dmlProcessor` need to pass the
context. Unit tests and helper callers can pass the zero value, which disables
the new Kafka-only rule and preserves current behavior.

The evaluation order should remain compatible with existing filters:

1. Ignore by start ts.
2. Ignore by table filter.
3. Ignore by event type or SQL event filter.
4. If enabled, ignore by `ignore-update-only-columns`.
5. Ignore by expression filter.

Step 4 and step 5 both use "any matching rule can skip the row" semantics,
matching existing event filter behavior.

## Column Resolution

Resolve configured names lazily for each matching table schema. Cache by table
ID and `tableInfo.GetUpdateTS()`. When the table schema changes, recompute.

Resolution rules:

1. If `case-sensitive = true`, match against the original schema name
   `ColumnInfo.Name.O`.
2. If `case-sensitive = false`, match with `strings.EqualFold`.
3. Only columns present in `tableInfo.GetRowColumnsOffset()` are evaluable. If a
   configured column is missing from the schema or not present in the row-change
   column set, warn and ignore that configured entry.
4. Duplicate configured columns are deduplicated by column ID.

Warn at most once per rule and table schema version:

```go
log.Warn("ignore update only column not found, skip it",
    zap.String("schema", tableInfo.GetSchemaName()),
    zap.String("table", tableInfo.GetTableName()),
    zap.String("column", columnName),
    zap.Bool("caseSensitive", caseSensitive))
```

This warning is an operational signal but must not be emitted per row.

## Changed Column Detection

Evaluate only UPDATE rows. INSERT and DELETE return `false`.

Pseudocode:

```go
func (r *updateOnlyColumnsRule) shouldSkipUpdate(preRow, row chunk.Row, tableInfo *common.TableInfo) bool {
    resolved := r.resolve(tableInfo)
    if len(resolved.ignoredColID) == 0 {
        return false
    }

    changedCount := 0
    keyColIDs := makeSetFromIndexes(tableInfo.GetIndexColumns())

    for _, col := range tableInfo.GetColumns() {
        offset, ok := tableInfo.GetRowColumnsOffset()[col.ID]
        if !ok {
            continue
        }

        changed := !columnValueEqual(preRow, row, offset, &col.FieldType)
        if !changed {
            continue
        }
        changedCount++

        if _, isKey := keyColIDs[col.ID]; isKey {
            return false
        }
        if _, ignored := resolved.ignoredColID[col.ID]; !ignored {
            return false
        }
    }

    return true
}
```

`columnValueEqual` must implement normal UPDATE comparison semantics:

1. NULL and NULL are equal.
2. NULL and non-NULL are different.
3. non-NULL and NULL are different.
4. non-NULL values are compared by column type.

Use `chunk.Row.GetDatum(offset, fieldType)` and type-aware comparison in the
filter package. Avoid importing sink codec packages into `pkg/filter`. The
comparison helper should cover the types already handled in codec tests:
decimal, enum, set, bit, time/date/datetime/timestamp, duration, JSON, bytes,
strings, signed and unsigned numbers, floats, and vectors if the current TiDB
type exposes them through the datum.

If a row has zero changed columns and the matching rule has at least one valid
ignored column, filter it out. This follows the product rule that the event is
ignored when all changed columns are included in the ignored set.

## Primary Key And Unique Key Updates

Primary key and unique key updates must always be kept. Use
`tableInfo.GetIndexColumns()` to cover both primary key and unique key columns.
Do not use `tableInfo.GetOrderedHandleKeyColumnIDs()`, because it only returns
the selected handle key and would miss other unique keys.

In normal non-raw mode, TiCDC may split some key-changing updates into
DELETE plus INSERT before filtering. Those rows are unaffected because this
feature applies only to UPDATE. In raw-change-event mode, the UPDATE remains an
UPDATE, so the changed key-column check keeps it.

## Multi-Row Transactions

`DMLEvent.AppendRow` filters one raw KV entry at a time. For a transaction with
multiple UPDATE rows, each row is evaluated independently:

1. Rows that match `ignore-update-only-columns` are removed from the chunk.
2. Rows that do not match remain in the same DML event.
3. If all rows are removed, the resulting DML event has `Len() == 0`, and the
   dispatcher already skips empty DML events.

This is consistent with existing expression filter behavior.

## Behavior Matrix

| Scenario | Kafka | Non-Kafka |
| --- | --- | --- |
| `ignore-update-only-columns` omitted | existing behavior | existing behavior |
| empty list | existing behavior | existing behavior |
| UPDATE changes only ignored columns | drop row | keep row |
| UPDATE changes ignored and non-ignored columns | keep row | keep row |
| UPDATE changes primary key or unique key | keep row | keep row |
| INSERT or DELETE | keep row unless existing filters drop it | keep row unless existing filters drop it |
| configured column does not exist | warn once per schema version, ignore entry | no evaluation, no warning |

## Tests

Add focused unit tests first.

`pkg/filter`:

1. UPDATE changes only ignored columns, returns ignored.
2. UPDATE changes ignored plus non-ignored columns, returns kept.
3. UPDATE changes primary key or unique key, returns kept even when the key
   column is configured.
4. INSERT and DELETE are unaffected.
5. Empty `ignore-update-only-columns` disables the feature.
6. Missing configured column is ignored; valid remaining columns still work.
7. Case-sensitive true and false matching.
8. NULL comparison cases:
   - NULL to NULL unchanged
   - NULL to non-NULL changed
   - non-NULL to NULL changed
9. Multiple matching rules use OR semantics.
10. The rule is inactive when `DMLFilterContext.EnableIgnoreUpdateOnlyColumns`
    is false.

`pkg/eventservice`:

1. `dmlProcessor` passes the enable flag to `DMLEvent.AppendRow`.
2. Same filter config behaves differently when the dispatcher flag is true vs
   false.

`downstreamadapter/eventcollector` or request conversion tests:

1. Kafka dispatcher registration sets `enable_ignore_update_only_columns = true`.
2. Non-Kafka and redo dispatcher registration set it to false.

API and conversion tests:

1. API model to internal config preserves `ignore_update_only_columns`.
2. Internal config to API model preserves it.
3. Internal config to eventpb preserves it.

Integration test:

Add a Kafka integration case if runtime cost is acceptable. The case should
create a Kafka changefeed with:

```toml
[[filter.event-filters]]
matcher = ["ignore_update_only_columns.user"]
ignore-update-only-columns = ["version", "updated_at"]
```

Then verify:

1. `UPDATE user SET version = version + 1 WHERE id = 1` is not emitted.
2. `UPDATE user SET name = 'new', version = version + 1 WHERE id = 1` is
   emitted.
3. `INSERT` and `DELETE` are emitted.
4. A primary key or unique key update is emitted.

## Validation

Recommended commands after implementation:

```sh
make generate-protobuf
make unit_test_pkg PKG=./pkg/filter/...
go test ./pkg/eventservice/...
go test ./downstreamadapter/eventcollector/... ./downstreamadapter/dispatchermanager/...
make integration_test_kafka CASE=ignore_update_only_columns
```

If no integration case is added in the first patch, document that coverage is
limited to unit and conversion tests.

## Compatibility And Rollout

1. Existing configs are unchanged because the new repeated field defaults to an
   empty list.
2. Existing OpenAPI requests are unchanged because the new JSON field is
   optional.
3. Protobuf compatibility is additive. Old senders omit the new fields and the
   feature stays disabled.
4. Non-Kafka downstreams can carry the config in metadata but never enable the
   runtime evaluation flag.
5. Invalid configured columns do not fail the changefeed. They are ignored with
   a warning so valid columns in the same rule can still work.

## Open Points

1. Whether generated virtual columns should be supported as configured columns.
   The proposed implementation evaluates only columns present in the row-change
   column set. Supporting virtual columns would require computing virtual values
   like expression filters do and should be a separate decision.
