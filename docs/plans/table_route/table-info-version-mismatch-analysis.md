# GetUpdateTS Version Mismatch Analysis

- 状态：草稿，待审阅
- 最后更新：2026-05-09
- 范围：只分析 `TableInfo.GetUpdateTS()` 导致的 table route 本地 DML rows mismatch panic。

## 结论

当前导致 panic 的直接原因是 `BatchDMLEvent.AssembleRows` 里比较两个 `TableInfo.GetUpdateTS()` 的返回值不一致。

相关代码逻辑是：

```go
originVersion := b.TableInfo.GetUpdateTS()
routedVersion := tableInfo.GetUpdateTS()
if originVersion != routedVersion {
    log.Panic("table version mismatch when set routed table info", ...)
}
```

这里的 `originVersion` 来自本地 DML rows decode 时使用的 source `TableInfo`，`routedVersion` 来自 event collector 传入的 dispatcher cached routed `TableInfo`。

## `GetUpdateTS` 的含义

`common.TableInfo.GetUpdateTS()` 返回 `TableInfo.UpdateTS`。

它更接近“行 schema / column schema 版本”，用于判断当前 `TableInfo` 是否适合 decode 或解释 row 数据。

当前注释说明它不覆盖所有 DDL：

```go
// These changing schema operations don't include 'truncate table', 'rename table',
// 'rename tables', 'truncate partition' and 'exchange partition'.
func (ti *TableInfo) GetUpdateTS() uint64
```

它主要用于：

- raw rows decode 前确认 `RawRows` 和传入的 `TableInfo` schema 匹配。
- event scanner 判断 DML batch 是否需要在 schema 变化处切开。
- codec schema、mysql writer 相关路径识别 row schema 版本。

因此，`GetUpdateTS()` 不应该被理解成“所有 DDL 的事件版本”。

## routed `TableInfo` 是否必然来自 DML 原始 `TableInfo`

如果 routed `TableInfo` 是直接从 DML 自带的 `batch.TableInfo` 调用 `CloneWithRouting` 生成的，那么它们的 `UpdateTS` 必然一致，因为 `CloneWithRouting` 会原样复制 `UpdateTS`。

但当前链路不是这样：

1. DML 自带的 `batch.TableInfo` 来自 event scanner。
   - event scanner 按事务时间从 schema store 读取：

   ```go
   schemaStore.GetTableInfo(tableID, rawEvent.CRTs-1)
   ```

2. dispatcher 缓存的 routed `tableInfo` 来自 handshake 或 DDL。
   - handshake 路径会对 start table info 做 routing 后缓存。
   - DDL 路径会对 DDL event 做 routing 后，用 DDL event 的 `TableInfo` 更新缓存。

3. `BatchDMLEvent.AssembleRows(tableInfo)` 传入的是 dispatcher 缓存的 routed `tableInfo`。
   - 它不一定是从当前 batch 的 source `TableInfo` clone 出来的。

所以“routed tableInfo 应该派生自本地 rows decode 用的 tableInfo”这个假设在当前实现里并不成立。

## partition DDL 下 mismatch 的可能路径

以 `DROP PARTITION p1` 后继续写入 `p2` 为例：

1. DDL event 携带 logical partition table 的新 `TableInfo`。
2. dispatcher cache 可能被更新成这个 routed DDL `TableInfo`。
3. 对还存在的 physical partition `p2`，schema store 的 versioned table info store 未必追加新版本。
4. 后续 `p2` DML rows 仍然使用旧 `UpdateTS` 的 `TableInfo` decode。
5. event collector 传入 dispatcher cached routed `TableInfo`，它可能带着 DDL 后的新 `UpdateTS`。
6. `AssembleRows` 本地路径比较两边 `GetUpdateTS()`，于是 panic。

这说明 mismatch 不一定代表 row schema 不兼容，也可能只是 partition metadata DDL 后，logical table metadata 和 physical partition DML 使用的 schema-store 版本粒度不同。

## 版本检查是否合理

### remote / raw rows 路径

raw rows 路径需要用传入的 `tableInfo.GetFieldSlice()` decode `RawRows`。

因此，如果 `b.TableInfo.GetUpdateTS()` 和传入的 `tableInfo.GetUpdateTS()` 不一致，保守 panic 是合理的。否则可能用错误 schema decode raw bytes。

这个检查在 table route 支持之前就存在。

### local rows 路径

local rows 路径里 `Rows` 已经 decode 完成。table route 在这里主要需要保证下游 sink 使用 target schema / target table。

在支持 table route 之前，这个路径基本是：

```go
if b.Rows != nil {
    return
}
```

也就是说，本地 rows 路径以前不会做 `GetUpdateTS()` mismatch 检查。

当前新增检查的隐含前提是：

> dispatcher cached routed tableInfo 必然和当前 batch source tableInfo 来自同一个 schema version。

结合上面的链路分析，这个前提不稳，尤其在 partition metadata DDL 下容易失败。

## 当前判断

1. 当前 panic 直接由 `GetUpdateTS()` 不一致触发。
2. `GetUpdateTS()` 是 row schema / column schema 版本，不是所有 DDL 的 epoch。
3. routed `TableInfo` 在当前实现中不一定来自 DML 原始 `TableInfo.CloneWithRouting`。
4. `AssembleRows` raw rows 路径的 version mismatch 检查合理。
5. `AssembleRows` local rows 路径的 version mismatch 检查过强，可能把 partition metadata DDL 误判成 row schema mismatch。

## 待继续确认的问题

1. 对 local rows 路径，是否应该避免使用 dispatcher cached `TableInfo` 直接替换 batch `TableInfo`。
2. 如果仅需要 routed target name，是否应基于当前 batch source `TableInfo` 生成 routed clone，而不是使用 dispatcher cache。
3. 如果仍然使用 dispatcher cache，是否应该只校验 column layout 兼容，而不是要求 `GetUpdateTS()` 完全一致。
4. partition metadata DDL 后，dispatcher cache 是否应该更新 `tableInfo`。
