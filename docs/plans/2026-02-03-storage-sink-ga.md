# Storage Sink (CloudStorage) GA Plan (Superseded)

> **Status:** Superseded. The GA source of truth is `docs/plans/2026-02-04-cloudstorage-sink-ga-requirements.md`, `docs/plans/2026-02-04-cloudstorage-sink-ga-design.md`, and `docs/plans/2026-02-04-cloudstorage-sink-ga-implementation.md`.
>
> This file is intentionally kept minimal to avoid confusing readers with plans and options that are not part of the 2026-02-04 GA scope.
>
> Key divergences include (not exhaustive): DDL ordering relies on per-dispatcher drain via `PassBlockEvent` (called before entering maintainer barrier); `enable-table-across-nodes` default is treated as false; early-wake is a must-have and has no rollback switch.
