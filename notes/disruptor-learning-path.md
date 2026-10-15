---
title: Disruptor 4.0.0 学习路径
date: 2026-09-24
source: conversation
---

# Disruptor 4.0.0 学习路径：从哪里看起最容易入手

这个项目虽然内核精深，但代码量很小（核心 5.7k 行），按对的路径走并不难。推荐一条"从用到懂"的顺序：

## 第一步：先跑起来（30 分钟）

**从 examples 入手，不要从源码入手。**

1. `src/examples/java/com/lmax/disruptor/examples/longevent/lambdas/LongEventMain.java` — 最简示例，lambda 风格，30 行看懂"定义事件 → 创建 Disruptor → 发布 → 消费"全流程
2. 同目录的 `legacy/` 版本 — 同一个例子的原始三段式 API（`next()`/`get()`/`publish()`），对照着看能理解 lambda 层只是语法糖
3. 真的跑一下：`./gradlew compileExamples` 后用 java 运行，改改 bufferSize、加几个消费者，观察行为

## 第二步：理解概念模型（1 小时）

读 `src/docs/asciidoc/en/user-guide/10_using_the_disruptor.adoc`，重点看：

- 组播（同一事件被多个消费者各处理一次）
- 消费者依赖图（`handleEventsWith(A).then(B)` 菱形拓扑）
- `src/docs/resources/images/` 里的拓扑图（unicast/multicast/diamond/sequencer）配着看

这一步的目标：能画出"生产者 → RingBuffer → 消费者链"的图，知道 Sequence 是干什么的。

## 第三步：读核心源码（按依赖顺序，半天）

由浅入深的阅读顺序：

| 顺序 | 文件 | 为什么先读它 |
|---|---|---|
| 1 | `Sequence.java`（130 行） | 全局最小的原子：一个带内存语义的 long，看懂 padding 和 VarHandle 就懂了地基 |
| 2 | `SingleProducerSequencer.java` | 最简实现，没有 CAS，逻辑直白 |
| 3 | `RingBuffer.java` 只看 `next/publish/get/create` 主干 | 它是门面，真正逻辑都在 Sequencer |
| 4 | `ProcessingSequenceBarrier.java`（90 行） | 消费者如何等待，很短 |
| 5 | `BatchEventProcessor.java` 的 `processEvents()` | 消费者主循环，批处理的精髓 |
| 6 | `dsl/Disruptor.java` 的 `createEventProcessors` 和 `updateGatingSequencesForNextInChain` | 依赖图和门控序号怎么装配 |
| 7 | `MultiProducerSequencer.java` | 最难的一个，`availableBuffer` 的绕圈标志编码，留到最后 |

**技巧**：每读一个类，先读它的测试（`src/test/java` 同名 `*Test.java`）——测试就是可执行的用法文档，比 javadoc 更具体。

## 第四步：动手验证理解

- 改一个 WaitStrategy 的行为，跑 `./gradlew test` 看哪些测试炸
- 跑 `./gradlew jmh` 里的 `SequenceBenchmark`，对比 padded/unpadded 的吞吐差异——亲身体会伪共享的代价
- 读 `src/jcstress/java` 的 6 个探针，理解"并发正确性如何被机器验证"

## 不建议的路径

- ❌ 从 `RingBuffer.java` 从头读到尾——1124 行里大半是 12 个 publish 重载矩阵，会淹死你
- ❌ 一上来就啃 `MultiProducerSequencer`——它是全项目最难的类
- ❌ 先读 perftest——那是给性能工程师的，不是入门材料

## 一句话总结

**examples 跑通 → user-guide 建立概念 → Sequence→单生产者→屏障→消费循环→DSL→多生产者 的源码顺序 → 用测试和 JMH 验证理解**。前两步走完你就已经会用了；第三步走完你就懂设计了。
