# pacifica-rs

pacifica-rs 是一个基于一致性算法PacificA并采用rust语言实现的生产级高性能日志复制框架。
不同与其他“多数节点”的算法如paxos、raft、zab等，它采用的Quorum机制(W+R>N)是N+1>N，
即对于写请求要求“全部节点”，而读请求仅仅需要其中的一个节点，这对于要求“读”性能的场景是非常友好的。
pacifica-rs是一个通用的框架，你可以基于它在你的应用中实现多个数据副本，以满足“读”服务的高可用，
以及多个数据副本的一致性，其次pacifica-rs适合在“数据”层而非“元数据”层。
你仅仅需要实现自己核心组件“状态机”以及其它基础组件（pacifica-rs提供了默认的日志存储和镜像快照存储实现），
pacifica-rs会自动帮你完成“状态机”之间的一致。

## 功能特性
- 多数据副本一致性保证
- 容错性：允许N-1个数据副本故障，不影响系统整体可用性
- 日志复制和副本恢复
- 快照和日志压缩
- 主动变更 Primary
- 对称网络分区容忍性
- 非对称网络分区容忍性

## 文档
### 如何使用
请参阅文档[《如何使用》](./docs/how-to-use.md)

### 指南
请参参阅[指南文档](./GUIDELINES.md)

### 关于PacificA
请参阅原文[《PacificA: Replication in Log-Based Distributed Storage Systems》](https://github.com/bybyset/jpacifica/blob/main/docs/PacificA.pdf)


## 贡献
欢迎对 pacifica-rs 的开发进行贡献！
如果你有任何建议、问题或想贡献代码，请参阅我们的[《贡献指南》](./HOW-TO-CONTRIBUTE.md)。


## 许可证
pacifica-rs 采用 Apache License 2.0 和 MIT 进行开源发布。有关详细信息，请参阅 [LICENSE-APACHE](./LICENSE-APACHE) 或 [LICENSE-MIT](./LICENSE-MIT) 协议文件。