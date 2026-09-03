# TiCDC franz-go Kafka GA 本地执行记录

更新时间：2026-09-02

本文记录本地 `test-plan` 中可用于 TiCDC franz-go Kafka GA 验证的测试资产及实际 execution。测试资产以当前生效的 `caseName` 为准，不统计已注释的 case。

## 1. GA Plan 清单

### 1.1 标准环境

共 63 个非 EKS、非 TiDB-X Plan。这里不表示执行状态，实际结果以第 3、4 节为准。

- 协议、数据与工作负载：
  - `cdc_newarch_airbnb_simple_titan`
  - `cdc-newarch-kafka-debezium-basic`
  - `cdc_newarch_kafka_large_msg_claim_check`
  - `cdc_newarch_kafka_large_message_handle`
  - `cdc-newarch-kafka-multiple-topic`
  - `cdc-newarch-kafka-realtime`
  - `cdc_newarch_kafka_scale_big_table_longrun`
  - `cdc_newarch_kafka_scale_big_table_ops`
  - `cdc_newarch_kafka_simple_ops`
  - `cdc_newarch_kafka_simple_ops_titan_off`
  - `cdc_newarch_kafka_simple_protocol`
  - `cdc_newarch_kafka_simple_protocol_misc_workloads`
  - `cdc_newarch_kafka_simple_misc_workloads_dispatcher_index`
- Kafka 安全：
  - `cdc-newarch-kafka-security`
- Kafka 与 TiCDC 故障恢复：
  - `cdc-newarch-kafka-all-2-owner-network-partition`
  - `cdc-newarch-kafka-broker-failure`
  - `cdc-newarch-kafka-controller-2-cdc-random-network-partition`
  - `cdc-newarch-kafka-controller-failure`
  - `cdc-newarch-kafka-random-2_cdc-random-network-partition`
  - `cdc-newarch-kafka-random-2-owner-network-partition`
  - `cdc-newarch-kafka-controller-2-owner-network-partition`
- Release dailyrun：
  - `cdc_newarch_kafka_basic_functionality`
  - `cdc-newarch-kafka-airbnb-scenario`
  - `cdc-newarch-kafka-avro`
  - `cdc-newarch-kafka-avro-2-workloads`
  - `cdc-newarch-kafka-debezium-avro`
  - `cdc-newarch-kafka-debezium-avro-2-workloads`
  - `cdc-newarch-kafka-mysql-sync`
  - `cdc-newarch-kafka-mysql-sync-gcttl`
  - `cdc-newarch-kafka-random-node-down`
  - `cdc_newarch_kafka_scale_big_table_cdc_scale`
  - `cdc_newarch_lightning_comp_kafka`
  - `cdc_newarch_sarama_no_broken_pipe`
  - `cdc-newarch-upstream-chaos-kafka-sync`
- Kafka 版本覆盖：
  - `cdc-newarch-kafka-version-0.11.0-0-r0`
  - `cdc-newarch-kafka-version-0.11.0-1-r0`
  - `cdc-newarch-kafka-version-1.0.0-r0`
  - `cdc-newarch-kafka-version-1.0.1-r0`
  - `cdc-newarch-kafka-version-1.1.0`
  - `cdc-newarch-kafka-version-1.1.1`
  - `cdc-newarch-kafka-version-2.0.0`
  - `cdc-newarch-kafka-version-2.0.1`
  - `cdc-newarch-kafka-version-2.1.0`
  - `cdc-newarch-kafka-version-2.2.0`
  - `cdc-newarch-kafka-version-2.3.0`
  - `cdc-newarch-kafka-version-2.4.0`
  - `cdc-newarch-kafka-version-2.5.0`
  - `cdc-newarch-kafka-version-2.6.0`
  - `cdc-newarch-kafka-version-2.7.0`
  - `cdc-newarch-kafka-version-2.8.0`
  - `cdc-newarch-kafka-version-3.0.0`
  - `cdc-newarch-kafka-version-3.1.0`
  - `cdc-newarch-kafka-version-3.2.0`
  - `cdc-newarch-kafka-version-3.4.0`
  - `cdc-newarch-kafka-version-3.5.0`
  - `cdc-newarch-kafka-version-3.6.0`
  - `cdc-newarch-kafka-version-3.7.0`
  - `cdc-newarch-kafka-version-3.8.0`
  - `cdc-newarch-kafka-version-3.9.0`
  - `cdc-newarch-kafka-version-4.0.0`
  - `cdc-newarch-kafka-version-4.1.0`
  - `cdc-newarch-kafka-version-4.2.0`
  - `cdc-newarch-kafka-version-4.3.0`

### 1.2 EKS 与 TiDB-X 补充环境

这些 Plan 用于补充环境兼容性验证，不阻塞标准 TiCDC franz-go GA：

- EKS：`cdc_newarch_kafka_simple_protocol-eks`，验证基本 DDL/DML、全数据类型和端到端一致性。
- EKS：`cdc-newarch-kafka-broker-failure-eks`，验证 Chaos Mesh、Kafka Pod 故障和恢复。
- EKS：`cdc-newarch-kafka-avro-eks`，验证 Schema Registry、consumer 和跨组件网络访问。
- TiDB-X：`tidbx_cdc_newarch_kafka_basic_functionality`，验证 realtime、incremental 和 TiCDC scale。
- TiDB-X：`tidbx-cdc-newarch-kafka-broker-failure`，验证 Kafka broker 故障后的恢复。

执行前统一配置：

- TiCDC 使用同一个 franz-go 构建产物。
- Kafka 使用 Apache Kafka `4.1.2` KRaft。
- case image 使用当前 GA 测试版本。
- sdkserver 使用 `hub.pingcap.net/qa/sdkserver:kafka-auth-amd64` 或目标环境中的同一镜像。
- EKS 所需镜像先同步到 EKS 可访问的 registry。

## 2. 补充说明

### 2.1 范围

共找到 109 个 TiCDC New Architecture Kafka YAML/Jsonnet 文件：

- 标准 TiCDC：`data-platform/cdc_newarch/kafka/` 37 个。
- 标准 TiCDC dailyrun：`release/dailyrun/data-platform/cdc_newarch/` 21 个。
- TiDBX：`data-platform/tidbx_cdc_newarch/kafka/` 32 个。
- TiDBX dailyrun：`release/dailyrun/data-platform/tidbx_cdc_newarch/` 19 个。

EKS 和 TiDBX 变体复用对应标准 Plan 的 case 与验证意图，下面不重复展开相同 case，但它们不是完全等价的重复执行：

- EKS 变体主要切换 resource pool、镜像仓库、存储、节点规格和调度配置；多数 Plan 沿用相同 TiCDC version 参数，但部分 Plan 固定使用 `master`，Kafka 通常固定为 `3.9.0`。
- TiDBX 变体切换为 TiDB-X 集群拓扑和配置，并使用 `mirrors/tidbx/pingcap/ticdc/image:master-nextgen`。它可能来自同一 TiCDC 代码库，但不是标准 Plan 使用的同一个镜像产物。
- 验证指定 franz-go TiCDC binary 时，只有显式使用目标 TiCDC image 的 execution 才计入结果；EKS/TiDBX Plan 只是可复用的测试覆盖入口。

### 2.2 Kafka 安全能力

`cdc-newarch-kafka-security` 包含 15 个 case：

- GSSAPI：用户名密码、keytab、TLS + 用户名密码、TLS + keytab + ACL。
- TLS：单向 TLS、mTLS + ACL。
- SASL/PLAIN：PLAIN、TLS + PLAIN。
- SASL/SCRAM：SHA-256 + ACL、SHA-512、TLS + SHA-256、TLS + SHA-512 + ACL。
- OAuth：HTTP token、HTTP compatibility、TLS + HTTPS token 私有 CA + ACL。

对应 case：

`cdc_kafka_auth_sasl_gssapi_user`、`cdc_kafka_auth_sasl_gssapi_keytab`、`cdc_kafka_auth_tls_sasl_gssapi_user`、`cdc_kafka_auth_tls_sasl_gssapi_keytab_acl`、`cdc_kafka_auth_tls`、`cdc_kafka_auth_mtls_acl`、`cdc_kafka_auth_sasl_plain`、`cdc_kafka_auth_sasl_scram_sha_256_acl`、`cdc_kafka_auth_sasl_scram_sha_512`、`cdc_kafka_auth_tls_sasl_plain`、`cdc_kafka_auth_tls_sasl_scram_sha_256`、`cdc_kafka_auth_tls_sasl_scram_sha_512_acl`、`cdc_kafka_auth_sasl_oauthbearer`、`cdc_kafka_auth_sasl_oauthbearer_http_compatibility`、`cdc_kafka_auth_tls_sasl_oauthbearer_acl`。

### 2.3 Kafka 版本覆盖

`cdc_kafka_version.tpl.jsonnet` 为每个版本生成 `cdc-newarch-kafka-version-<version>`，执行 `kafka_realtime`。基础版本集合为：

`0.11.0-0-r0`、`0.11.0-1-r0`、`1.0.0-r0`、`1.0.1-r0`、`1.1.0`、`1.1.1`、`2.0.0`、`2.0.1`、`2.1.0`、`2.2.0`、`2.3.0`、`2.4.0`、`2.5.0`、`2.6.0`、`2.7.0`、`2.8.0`、`3.0.0`、`3.1.0`、`3.2.0`、`3.4.0`、`3.5.0`、`3.6.0`、`3.7.0`、`3.8.0`、`3.9.0`。

- 标准环境：基础集合加 `4.0.0`、`4.1.0`、`4.2.0`、`4.3.0`，共 29 个版本。
- EKS：基础集合加 `4.0.0`，共 26 个版本。
- TiDBX 与 TiDBX EKS：使用基础集合，各 25 个版本。

当前版本集合不包含 Kafka `3.3.x`。

### 2.4 TiDBX 变体

TiDBX 当前复用以下标准 Plan 的 case，但使用 TiDB-X 上游集群和 TiDBX TiCDC image：

- 基础与协议：Airbnb simple titan、Debezium、large message claim check、large message handle、multiple topic、realtime、scale big table、simple ops、simple protocol、misc workloads、dispatcher index。
- Kafka 故障：all broker to owner、broker failure、controller to CDC、controller failure、random broker to CDC、random broker to owner、controller to owner。
- Release dailyrun：basic functionality、Airbnb scenario、Avro、Avro 2 workloads、MySQL sync、GC TTL、random CDC node down、scale big table、Lightning compatibility、断线重连、upstream chaos。
- Kafka 版本：`tidbx-cdc-newarch-kafka-version-<version>` 及 EKS 变体。

TiDBX 暂无对应的 Kafka security、Debezium Avro 和 Debezium Avro 2 workloads Plan。

## 3. 已完成 execution

以下 execution 使用 franz-go TiCDC image `hub-zot.pingcap.net/mirrors/dev/pingcap/ticdc/image:pull-4167-31d4137_linux_amd64`。

### 3.1 Kafka security

- [8204473](https://tcms.pingcap.net/dashboard/executions/plan/8204473)：15/15 SUCCESS。

### 3.2 Kafka chaos

- [8229205](https://tcms.pingcap.net/dashboard/executions/plan/8229205)：broker 故障，SUCCESS。
- [8204474](https://tcms.pingcap.net/dashboard/executions/plan/8204474)：controller 故障，SUCCESS。
- [8204475](https://tcms.pingcap.net/dashboard/executions/plan/8204475)：所有 Kafka broker 到 TiCDC owner 网络分区，SUCCESS。
- [8204476](https://tcms.pingcap.net/dashboard/executions/plan/8204476)：Kafka controller 到随机 TiCDC 节点网络分区，SUCCESS。

### 3.3 Kafka 版本

- Kafka 2.x：[2.0.0](https://tcms.pingcap.net/dashboard/executions/plan/8229191)、[2.0.1](https://tcms.pingcap.net/dashboard/executions/plan/8229192)、[2.1.0](https://tcms.pingcap.net/dashboard/executions/plan/8204465)、[2.2.0](https://tcms.pingcap.net/dashboard/executions/plan/8204466)、[2.3.0](https://tcms.pingcap.net/dashboard/executions/plan/8204467)、[2.4.0](https://tcms.pingcap.net/dashboard/executions/plan/8204468)、[2.5.0](https://tcms.pingcap.net/dashboard/executions/plan/8204469)、[2.7.0](https://tcms.pingcap.net/dashboard/executions/plan/8181348)，均 SUCCESS。
- Kafka 3.x：[3.0.0](https://tcms.pingcap.net/dashboard/executions/plan/8229193)、[3.5.0](https://tcms.pingcap.net/dashboard/executions/plan/8229196)、[3.7.0](https://tcms.pingcap.net/dashboard/executions/plan/8229198)、[3.9.0](https://tcms.pingcap.net/dashboard/executions/plan/8229200)，均 SUCCESS。
- Kafka 4.x：[4.0.0](https://tcms.pingcap.net/dashboard/executions/plan/8229201)、[4.1.0](https://tcms.pingcap.net/dashboard/executions/plan/8229202)、[4.2.0](https://tcms.pingcap.net/dashboard/executions/plan/8229203)、[4.3.0](https://tcms.pingcap.net/dashboard/executions/plan/8229204)，均 SUCCESS。

### 3.4 Avro 2 workloads

`cdc-newarch-kafka-avro-2-workloads` 使用同一 franz-go TiCDC image，分别验证 Kafka 3.1 和 Kafka 4.1.2。上游 TiDB、PD、TiKV 和 BR 固定为 `v8.5.8`，sdkserver 固定为 `hub.pingcap.net/qa/sdkserver:kafka-auth-amd64`，资源池使用 `ksyun-scenario-and-system-test`。

- Kafka 3.1：[8229286](https://tcms.pingcap.net/dashboard/executions/plan/8229286)、[8181445](https://tcms.pingcap.net/dashboard/executions/plan/8181445)、[8204540](https://tcms.pingcap.net/dashboard/executions/plan/8204540)，均 SUCCESS。
- Kafka 4.1.2：[8204541](https://tcms.pingcap.net/dashboard/executions/plan/8204541)、[8229287](https://tcms.pingcap.net/dashboard/executions/plan/8229287)、[8229288](https://tcms.pingcap.net/dashboard/executions/plan/8229288)，均 SUCCESS。
