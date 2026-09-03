# franz-go 待验证事项

## P2｜Kerberos 性能

- [ ] 验证每个 `kgo.Client` 复用 Kerberos client 是否安全且有实际收益。
  - 代码：[每次 SASL 认证创建 Kerberos client](../../pkg/sink/kafka/franz_gssapi.go#L33)。
  - 当前行为：每个 broker 连接进行 SASL 认证时都会重新加载 Kerberos 配置和 keytab，并在没有可复用 TGT 的新 client 上执行 AS exchange。
  - 约束：franz-go 支持持久 Kerberos client，但要求其生命周期归属于一个 `kgo.Client`。当前 client options 会先用于临时 admin client，再用于长期共享 client，不能让二者共同关闭同一个认证状态。
  - 验证：比较连接稳定和反复重连场景的认证延迟、KDC 请求数、CPU 和分配；覆盖 password、keytab、TGT 续期、并发连接、client 关闭和 race test。
  - 完成条件：确认收益显著且生命周期隔离方案通过上述验证后再实现；否则保留当前按认证创建的行为。
