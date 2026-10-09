# Storage Sink Grafana 面板建议

状态：Proposed  
日期：2026-03-26  
适用范围：当前分支上的 cloud storage sink Prometheus 指标与 Grafana 面板接入

## 1. 目标

本文只做一件事：**基于当前代码里已经存在的 storage sink Prometheus 指标，给出应该添加到 Grafana 的面板建议和可直接复制的 PromQL。**

这里不讨论未来可能会加什么指标，也不讨论已经删除或准备删除的指标。  
如果某个指标今天在代码里已经存在，就应当评估是否应该进入 Grafana。

## 2. 当前已经存在的 storage sink 指标

当前 cloud storage sink 专属指标定义在 [cloudstorage.go](/Users/edison/go/ticdc/downstreamadapter/sink/metrics/cloudstorage.go)：

- `CloudStorageFlushBytesHist`
  - 代码位置：`downstreamadapter/sink/metrics/cloudstorage.go:24-31`
  - 指标名：`ticdc_sink_cloud_storage_write_bytes`
- `CloudStorageFileCounter`
  - 代码位置：`downstreamadapter/sink/metrics/cloudstorage.go:33-39`
  - 指标名：`ticdc_sink_cloud_storage_file_count`
- `CloudStorageFlushDurationHistogram`
  - 代码位置：`downstreamadapter/sink/metrics/cloudstorage.go:41-48`
  - 指标名：`ticdc_sink_cloud_storage_flush_duration_seconds`
- `CloudStorageDDLFlushDMLDurationHistogram`
  - 代码位置：`downstreamadapter/sink/metrics/cloudstorage.go:50-57`
  - 指标名：`ticdc_sink_cloud_storage_ddl_flush_dml_duration_seconds`
- `CloudStorageWorkerBusyRatio`
  - 代码位置：`downstreamadapter/sink/metrics/cloudstorage.go:59-66`
  - 指标名：`ticdc_sink_cloud_storage_worker_busy_ratio`
- `CloudStorageSpoolMemoryBytesGauge`
  - 代码位置：`downstreamadapter/sink/metrics/cloudstorage.go:68-73`
  - 指标名：`ticdc_sink_cloud_storage_spool_memory_bytes`
- `CloudStorageSpoolDiskBytesGauge`
  - 代码位置：`downstreamadapter/sink/metrics/cloudstorage.go:75-80`
  - 指标名：`ticdc_sink_cloud_storage_spool_disk_bytes`
- `CloudStoragePendingPostEnqueueGauge`
  - 代码位置：`downstreamadapter/sink/metrics/cloudstorage.go:82-87`
  - 指标名：`ticdc_sink_cloud_storage_pending_post_enqueue`
- `CloudStorageSpoolDiskQuotaWaitDurationHistogram`
  - 代码位置：`downstreamadapter/sink/metrics/cloudstorage.go:89-95`
  - 指标名：`ticdc_sink_cloud_storage_spool_disk_quota_wait_duration_seconds`
- `CloudStorageSpoolDiskQuotaWaitersGauge`
  - 代码位置：`downstreamadapter/sink/metrics/cloudstorage.go:97-102`
  - 指标名：`ticdc_sink_cloud_storage_spool_disk_quota_waiters`
- `CloudStorageLoadBytesHistogram`
  - 代码位置：`downstreamadapter/sink/metrics/cloudstorage.go:104-111`
  - 指标名：`ticdc_sink_cloud_storage_load_bytes`
- `CloudStorageRotateCountCounter`
  - 代码位置：`downstreamadapter/sink/metrics/cloudstorage.go:113-118`
  - 指标名：`ticdc_sink_cloud_storage_rotate_total`
- `CloudStorageSpoolSegmentCountGauge`
  - 代码位置：`downstreamadapter/sink/metrics/cloudstorage.go:120-125`
  - 指标名：`ticdc_sink_cloud_storage_spool_segment_count`
- `CloudStorageFlushReasonCounter`
  - 代码位置：`downstreamadapter/sink/metrics/cloudstorage.go:127-132`
  - 指标名：`ticdc_sink_cloud_storage_flush_total`

此外，cloud storage sink 还复用了两组通用 sink 指标：

- `CheckpointTsMessageDuration`
  - 代码位置：`pkg/metrics/sink.go:216-223`
  - 指标名：`ticdc_sink_mq_checkpoint_ts_message_duration`
- `CheckpointTsMessageCount`
  - 代码位置：`pkg/metrics/sink.go:225-231`
  - 指标名：`ticdc_sink_mq_checkpoint_ts_message_count`

这两组虽然名字是 `mq_*`，但当前 cloud storage sink 已在 [sink.go:285-340](/Users/edison/go/ticdc/downstreamadapter/sink/cloudstorage/sink.go#L285) 里使用。

## 3. 当前 dashboard 的问题

当前 `Sink - Cloud Storage Sink` row 在 [ticdc_new_arch.json](/Users/edison/go/ticdc/metrics/grafana/ticdc_new_arch.json) 中已经存在，但只覆盖了旧的 5 张图：

- `Write Bytes/s`
- `File Count`
- `Write duration`
- `Flush duration`
- `Worker Busy Ratio`

当前问题很明确：

- 当前 dashboard 没有覆盖这次新加的 `spool / quota / flush reason / load / rotate / segment count`
- `Write Bytes/s` 还在用旧指标名 `cloud_storage_write_bytes_total`，需要更新到当前的 `cloud_storage_write_bytes`
- `File Count` 现在更适合拆成 `Files/s` 和 `Generated Files Total`
- `Flush duration`、`DDL flush dml duration`、`spool disk quota wait duration` 这种 histogram，Grafana 应统一只看 `avg + p99`
- `Worker Busy Ratio` 实际是 busy seconds counter，Grafana 应按 `rate(...)*100` 展示为百分比

## 4. 面板建议

下面的面板建议，全部基于当前已经存在的指标。PromQL 都写成一行，方便直接复制。

## 4.1 Writer 吞吐与延迟

### 1. Flush Bytes Avg

- 监控意图：看单次 flush 到远端的数据平均大小，判断 batch 是否过碎。
- 指标来源：
  - 定义：`downstreamadapter/sink/metrics/cloudstorage.go:24-31`
  - 上报：`downstreamadapter/sink/cloudstorage/writer.go:279-281`
- PromQL：

```promql
sum(rate(ticdc_sink_cloud_storage_write_bytes_sum{k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",namespace=~"$namespace",changefeed=~"$changefeed"}[1m])) / sum(rate(ticdc_sink_cloud_storage_write_bytes_count{k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",namespace=~"$namespace",changefeed=~"$changefeed"}[1m]))
```

### 2. Flush Bytes P99

- 监控意图：看大 flush 的长尾大小，判断是否有异常大的批次。
- PromQL：

```promql
histogram_quantile(0.99,sum(rate(ticdc_sink_cloud_storage_write_bytes_bucket{k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",namespace=~"$namespace",changefeed=~"$changefeed"}[1m])) by (le))
```

### 3. Files/s

- 监控意图：看文件生成频率，辅助判断 flush 是否过于频繁。
- 指标来源：
  - 定义：`downstreamadapter/sink/metrics/cloudstorage.go:33-39`
  - 上报：`downstreamadapter/sink/cloudstorage/writer.go:279-281`
- PromQL：

```promql
sum(rate(ticdc_sink_cloud_storage_file_count{k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",namespace=~"$namespace",changefeed=~"$changefeed"}[1m])) by (namespace,changefeed)
```

### 4. Generated Files Total

- 监控意图：看累计文件数量，适合长时间窗口观察。
- PromQL：

```promql
sum(ticdc_sink_cloud_storage_file_count{k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",namespace=~"$namespace",changefeed=~"$changefeed"}) by (namespace,changefeed)
```

## 5. 推荐布局

我建议在 `Sink - Cloud Storage Sink` 下至少拆成下面四组。

### 第一组：Flush 吞吐

- `Flush Bytes Avg`
- `Flush Bytes P99`
- `Files/s`
- `Generated Files Total`

### 第二组：Flush 延迟与负载

- `Flush Duration Avg`
- `Flush Duration P99`
- `Worker Busy %`
- `DDL Flush DML Duration Avg`
- `DDL Flush DML Duration P99`

### 第三组：Batch 与推进

- `Flush Count by Reason`
- `Checkpoint Message Duration Avg`
- `Checkpoint Message Duration P99`
- `Checkpoint Message Count`

### 第四组：Spool / Quota

- `Spool Memory Bytes`
- `Spool Disk Bytes`
- `Pending PostEnqueue`
- `Disk Quota Wait Duration Avg`
- `Disk Quota Wait Duration P99`
- `Disk Quota Waiters`
- `Load Bytes Avg`
- `Load Bytes P99`
- `Rotate/s`
- `Spool Segment Count`

## 6. 结论

本 PR 上新加的 storage sink 指标，**不应该只停留在 Prometheus 暴露层**。  
按当前代码，至少下面这些新增指标应该进入 Grafana：

- `cloud_storage_write_bytes`
- `cloud_storage_flush_duration_seconds`
- `cloud_storage_ddl_flush_dml_duration_seconds`
- `cloud_storage_worker_busy_ratio`
- `cloud_storage_spool_memory_bytes`
- `cloud_storage_spool_disk_bytes`
- `cloud_storage_pending_post_enqueue`
- `cloud_storage_spool_disk_quota_wait_duration_seconds`
- `cloud_storage_spool_disk_quota_waiters`
- `cloud_storage_load_bytes`
- `cloud_storage_rotate_total`
- `cloud_storage_spool_segment_count`
- `cloud_storage_flush_total`

如果这些面板不上 Grafana，这一轮可观测性改动就只完成了一半。  
下一步最合理的做法，就是按本文的面板建议，直接更新 [ticdc_new_arch.json](/Users/edison/go/ticdc/metrics/grafana/ticdc_new_arch.json)。
