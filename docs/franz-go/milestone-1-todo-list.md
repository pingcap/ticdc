# franz-go 待验证事项

## P2｜Kerberos 性能

- [ ] 验证长期 `kgo.Client` 复用 Kerberos client 的运行行为和收益。
  - 代码：[Kerberos client 生命周期](../../pkg/sink/kafka/franz_gssapi.go#L26)。
  - 当前行为：临时 admin 和长期共享 client 分别持有 Kerberos client；一个 `kgo.Client` 的 Broker 建连和重连共享认证状态，并在 `kgo.Client.Close` 时销毁。
  - 验证：比较连接稳定和反复重连场景的认证延迟、KDC 请求数、CPU 和分配；覆盖 password、keytab、TGT 续期、并发连接、client 关闭和 race test。
  - 完成条件：真实 Kerberos 集群的功能、并发、重连、续期、关闭和性能验证通过。
