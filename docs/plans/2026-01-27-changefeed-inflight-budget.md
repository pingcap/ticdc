# Changefeed Global Inflight Budget (Option A) Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task.

**Goal:** Add a changefeed-level global in-flight bytes budget for `cloudstorage/file` and `redo` sinks, layered on top of the existing per-dispatcher budget, to cap total sink in-flight memory and avoid starvation.

**Architecture:** Keep the existing per-dispatcher inflight budget. Introduce a shared per-changefeed `changefeedInflightBudget` that tracks global in-flight bytes and maintains a FIFO blocked queue; dispatchers register their wake callbacks when blocked by the global budget, and any flush that drops global in-flight to the low watermark wakes a bounded number of blocked dispatchers.

**Tech Stack:** Go, `sync/atomic`, `sync.Mutex`, Prometheus metrics in `pkg/metrics`.

---

### Task 1: Add unit tests for global budget

**Files:**
- Create: `downstreamadapter/dispatcher/changefeed_inflight_budget_test.go`

**Step 1: Write failing tests**
- Cover: block when `globalInFlightBytes >= globalHigh`, dedup, wake when `<= globalLow`, cleanup removes dispatcher.
- Cover integration: dispatcher A blocked by global while its own inflight drained, later awakened by another dispatcher's flush.

**Step 2: Run tests**

Run: `go test ./downstreamadapter/dispatcher -run TestChangefeedInflightBudget -count=1`

Expected: FAIL (missing types / wiring).

---

### Task 2: Implement `changefeedInflightBudget`

**Files:**
- Create: `downstreamadapter/dispatcher/changefeed_inflight_budget.go`
- Modify: `pkg/metrics/dispatcher.go`

**Step 1: Implement minimal API**
- `OnEnqueue(bytes int64)`
- `TryBlock(dispatcherID, wake func()) bool`
- `OnFlush(bytes int64)`
- `CleanupDispatcher(dispatcherID)`

**Step 2: Add metrics**
- `ticdc_dispatcher_inflight_budget_global_bytes`
- `ticdc_dispatcher_inflight_budget_global_blocked_dispatcher_count`
- `ticdc_dispatcher_inflight_budget_global_blocked_duration`

**Step 3: Run tests**

Run: `go test ./downstreamadapter/dispatcher -run TestChangefeedInflightBudget -count=1`

Expected: PASS.

---

### Task 3: Wire global budget into dispatcher inflight budget

**Files:**
- Modify: `downstreamadapter/dispatcher/inflight_budget.go`
- Modify: `downstreamadapter/dispatcher/basic_dispatcher.go`
- Modify: `downstreamadapter/dispatcher/basic_dispatcher_info.go`
- Modify: `downstreamadapter/dispatchermanager/dispatcher_manager.go`
- Modify: `downstreamadapter/dispatchermanager/dispatcher_manager_redo.go`

**Step 1: Add SharedInfo hooks**
- Store per-changefeed global budget (separately for sink/redo) and expose an init method for DispatcherManager.

**Step 2: Update per-dispatcher budget**
- On enqueue/flush, update both per-dispatcher and global counters.
- When global blocks, register wake callback in the global blocked queue.
- Ensure wake only happens when both per and global blocks are cleared.
- Cleanup path removes dispatcher from global blocked set.

**Step 3: Run focused tests**

Run: `go test ./downstreamadapter/dispatcher -run TestDispatcherInflightBudget -count=1`

Expected: PASS.

---

### Task 4: Full unit test and formatting pass

**Step 1: Run package tests**

Run: `go test ./...`

Expected: PASS.

**Step 2: Format**

Run: `make fmt`

Expected: no diffs.

