# Active-Active Feature Notes

## Repository Context
- Repo: ticdc (TiCDC downstream adapter + maintainer stack).
- All conversations must stay in Chinese, but code/config/log files remain ASCII and avoid Chinese characters.
- Formatting/tests mandate: run `make fmt` and targeted unit tests after code edits.

## Feature Overview
- Goal: introduce changefeed-level Active-Active mode with downstream TiDB (via MySQL sink) and enhance soft-delete handling across sinks.
- Key components touched: config parsing (`pkg/config`), dispatcher validation (`downstreamadapter/dispatcher*`), sink logic (especially MySQL), table metadata (`pkg/common/table_info` + schema store), progress tracking (MySQL sink progress table), unit tests.

## Confirmed Requirements
1. **Config Switch**
   - Name: follow existing style (likely `EnableActiveActive` in Go, serialized `enable-active-active`).
   - Default false.
   - Enabling requires: `bdr-mode == true`, sink type is TiDB/MySQL sink targeting TiDB cluster. Changefeed creation must reject other sinks.
   - Usage similar to `bdr-mode`; surfaced in JSON/YAML/CLI consistently.

2. **Table Metadata Flags**
   - Extend `model.TableInfo` / `common.TableInfo` with booleans: `IsActiveActiveTable`, `IsSoftDeleteTable` (exact names TBD).
   - Schema store will populate these fields (future change ensured by user). Current work must assume they exist and enforce checks.

3. **Dispatcher Validation**
   - When `enable-active-active` true, dispatcher creation must ensure each table handled is marked Active-Active. Otherwise raise error via `handleError` flow to maintainer and block dispatcher progress.

4. **Sink Behavior**
   - All sinks must support soft-delete/active-active table handling when `enable-active-active` is false (i.e., normal mode) but table metadata indicates special behavior:
     - Drop delete events for these tables.
     - Convert update events changing `_tidb_softdelete_time` from null/0 to non-null into delete events.
     - Other events pass through normally.
   - When `enable-active-active` true:
     - Only MySQL sink path is valid.
     - Delete events are ignored.
     - Insert/Update must emit LWW `INSERT ... ON DUPLICATE KEY UPDATE` SQL using business columns plus `_tidb_origin_ts`, `_tidb_softdelete_time`; `_tidb_commit_ts` is read-only (used inside `IFNULL` but not updated). SQL template uses condition `@cond := (IFNULL(target._tidb_origin_ts, target._tidb_commit_ts) <= VALUES(_tidb_origin_ts))` and per-column `IF(@cond, VALUES(col), target.col)` updates.
     - Table must contain `_tidb_origin_ts`, `_tidb_softdelete_time`, `_tidb_commit_ts`; absence should raise error and stop pipeline.

5. **Progress Table**
   - Database `tidb_cdc` auto-created in downstream TiDB when Active-Active enabled.
   - Ensure table `tidb_cdc.ticdcProgressTable` exists with schema:
     ```sql
     CREATE TABLE ticdcProgressTable (
       changefeed_id VARCHAR(255) NOT NULL,
       upstreamID VARCHAR(255) NOT NULL,
       database_name VARCHAR(255) NOT NULL,
       table_name VARCHAR(255) NOT NULL,
       checkpoint_ts BIGINT UNSIGNED NOT NULL,
       PRIMARY KEY (changefeed_id, upstreamID, database_name, table_name)
     ) COMMENT='TiCDC synchronization progress table for HardDelete safety check';
     ```
   - MySQL sink listens for checkpointTs updates (from etcd) and every configurable interval (default 30 minutes) performs sparse updates for all tracked tables using `INSERT ... ON DUPLICATE KEY UPDATE`.
   - `upstreamID` equals cluster ID.

6. **Event Filtering Rules**
   - Soft delete detection: `_tidb_softdelete_time` transition from null/0 to non-null triggers conversion to delete event (when not in Active-Active mode). Delete events otherwise dropped for such tables.
   - In Active-Active mode delete events simply ignored; progress table updates handle safety.

7. **Testing & Tooling**
   - Add/adjust unit tests covering config validation, dispatcher enforcement, event rewriting, SQL generation, progress table management.
   - No integration tests for now.
   - After modifications run `make fmt` and targeted unit tests (likely packages touched plus any new ones).

## Implementation Plan (High-Level)
1. **Config Layer**
   - Add `EnableActiveActive` field to `ReplicaConfig`, CLI, serialization.
   - Validate mutual constraints (BDR mode, sink type) during config creation/verification.

2. **Metadata Updates**
   - Extend `common.TableInfo` structs and schema store serialization with new flags.
   - Provide helper methods (`IsActiveActiveTable`, `IsSoftDeleteTable`).

3. **Dispatcher Manager**
   - During dispatcher creation, when active-active enabled, check associated table info flag and error via `handleError` if mismatch.

4. **Sink Adjustments**
   - Define shared utilities to classify events and transform delete/update per table flags.
   - For MySQL sink:
     - Introduce SQL builder for LWW `INSERT ... ON DUPLICATE KEY` statements.
     - Integrate hidden column presence checks.
     - Ensure event encoding path uses new builder when mode enabled.

5. **Progress Table Maintenance**
   - On sink init ensure DB/table exist.
   - Track checkpointTs from dispatcher manager; add timer to update `ticdcProgressTable` every configurable interval (default 30m, config parameter for changefeed).

6. **Testing & Formatting**
   - Expand unit tests for each new module.
   - Run `make fmt` and focused unit tests.

## Open Questions / Assumptions
- Precise field names for table metadata (pending TableInfo schema update by user). For now plan to add bool flags and update serialization accordingly.
- Where the 30-minute interval parameter should live (likely under sink config or changefeed config). Need to confirm best placement before coding.
- Mechanism to receive checkpointTs update: hook into dispatcher manager's existing AddCheckpointTs -> sink path or subscribe to etcd update notifications? Need to inspect sink API.

## Current Status (2025-XX-XX)
- Requirements clarified with user; waiting to start implementation.
- Next step: finalize design for config additions and sink constraints, then begin coding per plan.


## 2025-??-?? Progress
- Added replica config + API plumbing for `enable-active-active` and progress interval defaults.
- Extended `common.TableInfo` with active-active/soft-delete flags and helper methods; added schema-store validation for required hidden columns when the feature is enabled.
- Bootstrapped row-policy helper (`pkg/sink/util/active_active.go`) to evaluate/convert rows; pending integration with concrete sink implementations.
- Next focus: propagate `enableActiveActive` through sink constructors, hook row-policy helper across sinks (kafka/pulsar/cloudstorage/redo/mysql), then implement MySQL-specific LWW SQL + progress table updates.
- 新增校验：`enable-active-active` 开启时必须关闭 redo/一致性日志，否则直接报错，防止创建 redo dispatcher。
- 已在各类 sink（kafka/pulsar/cloudstorage/mysql）接入行策略过滤，统一调用 `FilterDMLEvent`；MySQL sink 额外落盘 LWW SQL 与 progress table 更新逻辑开发中，当前已具备 LWW upsert 及 progress 表周期更新、定期 upsert `tidb_cdc.ticdcProgressTable` 的能力，后续需根据实际验证完善。
