# 单机内存 KV 与租约锁

这是一个 Rust 学习原型：单个服务端通过 tarpc TCP 提供版本化条件写、客户端重试、租约锁和带 fencing token 的 KV 写入。服务端状态只在内存中；客户端崩溃后的锁回收不依赖客户端执行清理。设计、不变量和故障边界见 [KV 与租约锁设计](docs/KV_LEASE_DESIGN.md)。

## 构建与启动

```sh
cd shard_kv/kvsrv
cargo build --bin kvsrvd
cargo run --bin kvsrvd -- 127.0.0.1:7000 3000
```

最后一个参数是全服务端统一的租约时长，单位毫秒，必须为正整数。使用 `127.0.0.1:0` 可由系统选择端口；服务端启动后打印 `LISTENING` 和实际地址。Ctrl-C 会关闭监听器和连接任务。

## 客户端接口

客户端使用 `kvsrv::client::Clerk`。`get` 返回值和版本；`put` 提交期望版本，创建新键使用版本 `0`。同版本的条件写最多成功一次。若一个写请求没有收到回复，随后重试遇到版本冲突，返回 `KvError::Maybe`：调用者需要根据应用状态处理可能已提交的结果。其他终态错误发生在结果不确定时以 `MaybeWithCause` 返回，保留错误原因。

```rust,no_run
use kvsrv::client::Clerk;
use std::net::SocketAddr;

# async fn example() -> Result<(), kvsrv::protocol::KvError> {
let addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
let clerk = Clerk::new(addr);
clerk.put("key", "first", 0).await?;

let guard = clerk.acquire("lock-name").await?;
let current = clerk.get("key").await?;
clerk.put_fenced("key", "second", current.version, "lock-name", guard.token()).await?;
guard.release().await?;
# Ok(())
# }
```

示例中的受保护写入必须使用约定的锁名和 `put_fenced`。普通 `put` 不检查锁。`LeaseGuard` 在后台续租，`wait_lost` 在续租被拒绝或无法确认时通知调用者；`is_lost() == false` 不能证明租约仍有效。显式 `release().await` 结束续租并请求释放；直接丢弃 guard 只停止续租，服务端在租约到期后回收锁。

`Clerk::new` 每次 RPC 尝试默认超时 1 秒；`Clerk::with_timeout` 可调整。没有收到回复的请求每 100 毫秒重试，直到收到应用回复或遇到不可恢复的协议错误；服务端长期不可达时调用可能一直等待，调用者可在需要时自行取消 future。每次尝试使用新的 TCP 连接。

## 验证

```sh
cd shard_kv/kvsrv
cargo test
```

单元测试检查截止时间等号、重复获取、续租、计数溢出，并构造迟到获取的反例。集成测试覆盖并发条件写、小规模操作历史的线性一致性检查、TCP 丢包与延迟/乱序、连接中断、请求取消、客户端分区、租约过期后的 fencing，以及杀死独立持锁进程后的重获。测试模型和课程代码的对应关系见[设计记录](docs/KV_LEASE_DESIGN.md)；这些测试不证明所有故障时序、服务器重启恢复或未来 Raft 的正确性。
