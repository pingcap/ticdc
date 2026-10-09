# Storage Sink 2 Staged Ack 机制说明

这篇文档讨论的是当前 TiCDC 分支里 storage sink 的 DML two-stage ack 机制。这里的 “2 staged ack” 不是代码中的一个正式类型名，而是对一套运行时语义的概括：

- 第一阶段：事件已经被 sink 的内部写入流水线接住，可以用于唤醒 dispatcher
- 第二阶段：事件已经完成 flush 阶段，可以用于推进 table progress / checkpoint

这套机制最早由 PR `#4263` 引入：

- `dispatcher,event,cloudstorage: add DML two-stage ack`
- 对应 commit：`29db6eb28`
- 关联 issue：`#4269 dispatcher,event,cloudstorage: fix enqueue callback semantics and race`

PR 的原始动机很明确：此前 dispatcher 的 wake callback 绑定在 `PostFlush` 上，导致 storage sink 的调度推进速度被 flush 延迟直接主导。在低流量场景下，这个问题尤其明显，因为一小批 DML 很可能要等到 flush interval 到期、或者等远端 object storage 写完，dispatcher 才能继续往前走。two-stage ack 的目标，就是把“调度推进”和“下游 flush 完成”这两件事拆开。

## 1. Background 介绍

### 1.1 这个问题为什么会在 storage sink 上暴露出来

storage sink 和 MySQL / MQ 类 sink 的一个关键差别是：它的 DML 路径天然是分阶段的。

一个 DML 从 dispatcher 发到 storage sink 后，并不会立刻变成一个已经落到下游的结果。中间至少还要经过：

```text
dispatcher
  -> sink.AddDMLEvent
  -> 内部编码与分发
  -> writer/buffer
  -> 本地暂存
  -> writer flush
  -> data file
  -> index file
```

因此，这条路径里天然有两个不同的“完成”时刻：

1. sink 已经把这条 DML 接住，dispatcher 没必要继续为它阻塞
2. sink 已经真正完成 flush，可以据此推进 table progress 和 checkpoint

如果仍然坚持单阶段 ack，就只能在两个坏选择里二选一：

- ack 太早：一旦进入 sink 的前置队列就算完成，会把 checkpoint / block event 顺序搞乱
- ack 太晚：必须等 flush 完才唤醒 dispatcher，会把调度延迟和远端写入延迟强耦合

two-stage ack 的本质，就是承认 storage sink 内部确实存在这两个时刻，并把它们显式区分开。

### 1.2 PR #4263 解决的核心问题是什么

PR `#4263` 在描述里直接写了两个结论：

1. DML 需要拆成 enqueue-stage ack 和 flush-stage ack
2. dispatcher 的 wake 应该依赖 enqueue-stage ack，而 checkpoint 仍然依赖 flush-stage ack

换句话说，这个 PR 不是为了改 checkpoint 语义，而是为了降低 dispatcher 在 storage sink 上的 wake latency。

这一点和 issue `#4269` 的问题描述也是一致的：当时 cloud storage sink 上的 `PostEnqueue` 甚至还触发得过早，只是把事件塞进 `msgCh` 就算 enqueue，这并不等于事件已经被写入流水线稳定接管。后续修正的重点，正是把 enqueue 的语义收紧到“真正进入 sink 的写路径”。

### 1.3 低流量场景为什么更需要 two-stage ack

高流量场景下，writer 往往会因为 file size 很快触发 flush，`PostFlush` 的延迟可能没有那么刺眼。

低流量场景下则不同：

- batch 很小
- flush 更依赖定时器
- object storage 的尾延迟更容易直接暴露到调度路径上

如果 dispatcher 只有在 `PostFlush` 后才被唤醒，那么它看到的是“远端 flush 完成的速度”，而不是“sink 接住事件的速度”。这会让 storage sink 的内部异步流水线失去价值。

所以，从问题背景上说，two-stage ack 首先是一个调度解耦机制，其次才是一个 callback 设计。

## 2. 2 staged ack 机制介绍

### 2.1 什么是 two-stage ack

当前实现里，DML two-stage ack 对应 `DMLEvent` 上的两个阶段：

- `PostEnqueue`
- `PostFlush`

它们分别表示：

#### Stage 1: `PostEnqueue`

语义是：

- 这条 DML 已经进入 sink 的内部写入流水线
- dispatcher 可以把这条 DML 从“当前批次阻塞条件”里拿掉

它不意味着：

- 已经写出 data file
- 已经写出 index file
- checkpoint 可以推进

#### Stage 2: `PostFlush`

语义是：

- 这条 DML 已经完成 flush 阶段
- table progress 可以删除它
- checkpoint 相关逻辑可以把它视为已完成

在 storage sink 的正常路径上，`PostFlush` 对应的是 writer 完成 data file / index file 这条 flush 边界之后的确认。

### 2.2 为什么支持 two-stage ack 的关键是 `PostEnqueue`

从代码抽象上说，two-stage ack 真正新增的不是 `PostFlush`，而是 `PostEnqueue`。

`PostFlush` 原本就存在，代表的是较强的“写下游完成”语义。PR `#4263` 的关键改动，是在 `DMLEvent` 上显式加入：

- `PostTxnEnqueued []func()`
- `PostEnqueue()`
- `AddPostEnqueueFunc(...)`

有了 `PostEnqueue` 之后，系统才能把两件原本绑在一起的事情拆开：

1. 唤醒 dispatcher
2. 推进 table progress

这也是这套机制真正成立的前提。

### 2.3 两个阶段是怎么解耦的

解耦发生在 dispatcher 这一侧。

`BasicDispatcher` 现在做的是：

- 用 `AddPostEnqueueFunc(...)` 绑定 wake callback
- 用 `tableProgress.Add(event)` 把事件放进 progress 列表
- 再由 `tableProgress` 通过 `PushFrontFlushFunc(...)` 把移除动作绑到 `PostFlush`

于是系统行为变成：

- dispatcher 的继续调度，取决于 enqueue stage
- table progress / checkpoint 的推进，取决于 flush stage

这就是 two-stage ack 的核心收益。它不是“多加一个 callback”这么简单，而是把调度语义和持久化语义拆成了两个独立阶段。

### 2.4 兼容性是怎么保持的

`DMLEvent.PostFlush()` 里仍然会 fallback 调用一次 `PostEnqueue()`。

这意味着：

- 对 storage sink 这种有显式 enqueue stage 的实现，可以先 `PostEnqueue`、后 `PostFlush`
- 对没有显式 enqueue stage 的 sink，仍然可以只在 flush 成功后调用 `PostFlush`，系统不会漏掉 enqueue callback

所以 two-stage ack 对整体框架来说是“向前兼容的语义增强”，不是只服务 storage sink 的孤立接口。

### 2.5 two-stage ack 不改变哪些事情

这套机制虽然让 wake 提前了，但它没有改变下面这些边界：

- checkpoint 仍然是 flush-bound，而不是 enqueue-bound
- block event 的严格顺序仍然依赖 flush barrier，而不是 enqueue ack
- enqueue 只是 sink 内部接受语义，不是 durable 语义

因此，two-stage ack 的正确理解应该是：

> 让 dispatcher 更早脱离“等待 flush”的状态，但不让 checkpoint 和 block event 的正确性变弱。

## 3. 具体实现介绍

### 3.1 `DMLEvent`：把两个阶段挂到同一个事件对象上

从结构上看，two-stage ack 是先在 `pkg/common/event/dml_event.go` 上落地的。

关键点只有两个：

1. 新增 `PostEnqueue` 这一阶段
2. 让 `PostEnqueue` 具备 exactly-once 语义

这里的 exactly-once 很重要。issue `#4269` 里专门提到，`PostEnqueue` 既可能从 enqueue 路径触发，也可能通过 `PostFlush` fallback 触发，因此必须是并发安全、至多一次的。

所以从事件抽象本身看，two-stage ack 的支撑条件是：

- flush stage 继续保留
- enqueue stage 明确建模
- enqueue stage 做幂等保护

### 3.2 dispatcher：wake dispatcher 和推进 table progress 的解耦点

在 dispatcher 里，最关键的变化不是“怎么发 DML”，而是“怎么判断这一批 DML 已经不再阻塞调度”。

现在的做法是：

- 对一批 DML，dispatcher 给每个事件注册一个 `PostEnqueue` callback
- 这一批事件全部 `PostEnqueue` 后，才触发这批的 wake callback

与此同时：

- `tableProgress` 依然只在 `PostFlush` 里删除事件

因此，在 dispatcher 侧，two-stage ack 实际上就是这句设计：

> wake on enqueue, checkpoint on flush

这也是 PR `#4263` 最想达成的效果。

### 3.3 storage sink：enqueue 必须代表“真正进入写流水线”

这是 storage sink 最容易被误解的一点。

如果 `PostEnqueue` 只是意味着“调用了 `sink.AddDMLEvent`”或者“塞进了第一个无限队列”，那么它没有任何价值，因为这种 enqueue 不能说明 sink 真的接住了数据。

issue `#4269` 专门指出了这个问题：cloud storage sink 一开始把 `PostEnqueue` 触发得过早，只是推入 `msgCh` 就算 enqueue，这个语义太弱。

因此，storage sink 要支持 two-stage ack，必须满足一个更强的条件：

> enqueue stage 必须对应“事件已经跨过了 sink 的前置输入队列，真正进入 sink 的写入/缓冲体系”。

在当前分支里，这个“更强的 enqueue”已经不再放在 `AddDMLEvent` 边界，而是放在 writer 侧的 buffering / spool 接入点上。

### 3.4 为什么 sink 内部必须有 memory quota 机制

这是你这次特别强调的点，也是当前 storage sink 正在继续收口的方向。

two-stage ack 把 dispatcher wake 提前了，但这个提前不是免费的。它要求 sink 内部必须有能力承接更多“已经被上游放行、但还没 flush 到远端”的数据。

如果没有一套受控的本地承载机制，就会出现两个问题：

1. enqueue ack 语义过弱，只是“进入某个无界队列”
2. 早 wake 之后，sink 内部积压会无限长，最终把内存压力重新暴露出来

所以对 storage sink 来说，two-stage ack 和 memory quota 机制其实是配套设计：

- two-stage ack 负责把 wake 从 flush 上解耦
- memory quota 负责定义“sink 到底能接住多少未 flush 数据”

没有后者，前者的语义就站不稳。

### 3.5 当前分支里 spool 的作用

当前分支正在实现的 spool，正是这套配套机制的具体落点。

spool 带来了三个直接效果：

1. 给 storage sink 提供一个受控的本地承载层
2. 通过 memory quota / spill 把“已 enqueue 但未 flush”的数据放到一个更稳定的管理边界里
3. 通过 high / low watermark 控制 `PostEnqueue` 的节奏，而不是无限制地提前 wake 上游

这意味着：

- `PostEnqueue` 不再只是一个调度优化信号
- 它开始和 sink 的本地容量管理直接相关

从这个角度看，spool 不只是 storage sink 的性能优化组件，它还是 two-stage ack 在 runtime 上成立的一个重要支撑。

### 3.6 spool 和 two-stage ack 的关系应该怎么理解

可以把两者的关系理解成下面这句话：

> two-stage ack 回答的是“什么时候可以提前放行 dispatcher”，spool / memory quota 回答的是“提前放行之后，sink 自己靠什么把这些尚未 flush 的数据稳稳接住”。

当前分支的设计方向已经很清楚：

- enqueue stage 不代表 durable
- enqueue stage 必须代表 sink 内部已经真正接管
- sink 内部接管之后，需要有本地 memory quota / spill 体系承载这段时间窗

这也是为什么随着 spool 落地，`PostEnqueue` 的语义会比 PR `#4263` 初始版本更严谨。

### 3.7 flush stage 仍然是正确性边界

虽然本文重点是 enqueue stage，但 storage sink 的正确性边界仍然在 flush stage。

也就是说：

- writer 只有在最终 flush 成功后，才会触发 `PostFlush`
- table progress / checkpoint 仍然只依赖这个阶段
- DDL / sync point 仍然要先通过 `FlushDMLBeforeBlock` 把前序 DML flush 完

所以 current storage sink 的设计不是“用 enqueue 替代 flush”，而是：

- 用 enqueue 优化调度
- 用 flush 保证正确性

## 4. 小结

如果把当前 storage sink 的 2 staged ack 机制压缩成三句话，可以表述为：

1. two-stage ack 是 PR `#4263` 为了解决 storage sink 尤其是低流量场景下 wake latency 过高而引入的，核心目标是把 dispatcher 调度推进和远端 flush 延迟解耦。
2. 这套机制真正成立的关键，是在 `DMLEvent` 上加入 `PostEnqueue`，从而把 “wake dispatcher” 和 “推进 table progress / checkpoint” 这两个动作拆开：前者绑定 enqueue，后者绑定 flush。
3. 对 storage sink 来说，仅有 two-stage ack 还不够；它还必须配套一套本地 memory quota / spool 机制，来承接那些“已经 enqueue、但还没 flush”的数据，否则 enqueue 语义会过弱，系统也无法稳定运行。

因此，当前分支上的 spool 工作，并不是和 two-stage ack 平行的另一条线；它其实是在把 two-stage ack 从“抽象语义”补成“可长期运行的工程实现”。
