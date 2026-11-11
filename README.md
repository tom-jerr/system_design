# 分布式系统学习：与 AI Agent 结对

这个仓库用 [MIT 6.5840](https://6.5840.github.io/) 实验和经典系统论文训练分布式系统设计能力，维护者与 AI Agent 结对：一起建模、实现、注入故障并复盘，但设计取舍和最终实现仍要由维护者自己解释和验证。

当前内容：`6.5840/` 是课程的 Go 实现，包含 MapReduce、KV 服务与租约锁、Raft、复制状态机 KV 和 ShardKV 及其测试设施；`gfs/` 是 GFS 论文机制的 Rust 设计文档与分阶段实施路线，尚未进入实现。

Agent 协作与工程约定见 [`AGENTS.md`](AGENTS.md) 和 [`docs/DESIGN_RULES.md`](docs/DESIGN_RULES.md)。
