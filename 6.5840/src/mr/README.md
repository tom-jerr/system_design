# MapReduce worker 故障恢复

面向单机多进程实验：coordinator 和 worker 使用 Unix socket 和本地文件系统。
worker 共享输出目录；coordinator 可使用不同工作目录，RPC 文件名使用绝对路径，
最终输出发布到 worker 的输出目录。coordinator 在整个作业期间保持运行。

## 身份与调度

- 每次 worker 启动调用 `RegisterWorker`；coordinator 在锁内分配递增 ID。
  ID 在该 coordinator 生命周期内唯一，worker 重启不会复用旧 ID。
  不采用 PID，因为 PID 会复用；这里无需额外 UUID 依赖。
- 每个任务分配时递增 `Attempt`，记录 worker ID 和开始时间。
  结果提交必须匹配任务类型、任务 ID、worker ID 和 attempt。
- 没有任务时返回 `wait`，worker 每 200ms 询问一次。
  `InProgress` 任务达到 10 秒后可以重派，不依赖崩溃 worker 上报或唤醒。
  超时只说明任务可重试；原 worker 仍可能继续执行。

## 输出提交

Map 每次执行写入独立临时文件并关闭，再上报文件列表。
coordinator 只把当前 attempt 的文件加入对应 reduce 分区。
reduce 数量固定为 `nReduce`，所有 map 提交后才分配 reduce。

Reduce 同样写入独立文件；coordinator 在调度锁内校验 attempt，
将其 rename 为 `mr-out-N` 后标记完成。rename 是最终输出的发布点。
过期 worker 无法覆盖最终输出，重复上报不会重复加入 map 输入。
Map/Reduce 函数可能执行多次，不保证外部副作用只发生一次。

RPC 失败的提交结果可能未知，worker 保留文件后退出，让其他 worker
在需要时重试任务。被拒绝的执行删除自己的文件；进程崩溃留下的孤立文件
暂不回收。已接受的 map 文件在 worker 崩溃后仍由共享文件系统保留。

## 边界与验证

恢复要求仍有存活 worker 或后续启动新 worker。若每次尝试均超过超时，
不能承诺作业最终结束。coordinator 重启、整机崩溃、共享文件丢失和断电持久性
不在当前保证内；ID 不承诺跨 coordinator 重启或跨作业全局唯一。

`TestRecoveryAttempts` 使用过期时间模拟崩溃，检查 ID 分配、map/reduce 重派、
迟到/重复提交、同一 worker 的旧 attempt，以及最终输出不被覆盖。
`mr_test.go` 提供多进程结果比较、并发和随机 worker crash 测试。
使用 Go 标准库完成 RPC、JSON、临时文件和 rename；任务重试与提交协议
保留在实验实现中，便于检查状态转换。

本次验证：`make -C src mr-build` 和在 `src` 下运行
`go test -v -race ./mr -count=1 -timeout=5m` 均通过，包含全部已有测试和
`TestRecoveryAttempts`；随机 crash 测试的输出与无崩溃版本一致。
