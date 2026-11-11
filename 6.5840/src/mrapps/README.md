# Distributed Grep

`grep.go` 是符合现有 `Map` / `Reduce` 插件接口的应用。
它采用 [MapReduce 论文 §2.3](https://research.google.com/archive/mapreduce-osdi04.pdf)
的处理方式：Map 筛选匹配行，Reduce 复制匹配内容。

## 语义

- `MR_GREP_PATTERN` 是 Go 正则表达式；未设置时默认搜索 `Sherlock`。
  显式设置为空字符串会匹配每一行。所有 worker 必须使用同一个表达式，
  且整个作业期间配置不变；非法正则会在 Map 中报错并终止该 worker。
- Map 对每个命中行输出 `Key=文件名:行号, Value=原始行内容`。
  行号从 1 开始，相同文本出现在不同位置时全部保留。
- Reduce 原样连接输入内容。框架输出格式是 `key value`，所以结果为：

  ```text
  input.log:2 ERROR repeated
  input.log:3 ERROR repeated
  ```

- 使用换行符 `\n` 分行，不删除空格或 `\r`；最后一行没有换行符也会处理。
  空文件没有行，结尾换行不产生额外的空行。不会受 Scanner 默认行长度限制。
- 输入文件列表应使用不同的路径标识各个文件；重复提交同一路径会重复匹配，
  并将同一位置的内容合并到一次 Reduce 调用中。
- 多个 reduce 分区各自输出文件，不保证输入顺序或全局行号顺序。
  任务重试及结果提交沿用现有框架协议。

使用文件位置作为 key 是为了适配实验中 `Reduce` 只返回一个字符串的接口，
同时避免把相同文本错误地去重或计数。

## 运行

在 `src` 目录编译（也可使用 `make mr-build`）：

```sh
go build -o main/mrcoordinator main/mrcoordinator.go
go build -o main/mrworker main/mrworker.go
go build -o main/mrsequential main/mrsequential.go
go build -buildmode=plugin -o mrapps/grep.so mrapps/grep.go
```

所有程序和插件应使用一致的构建选项；例如启用 `-race` 时全部启用。

在 `src/main` 目录启动 coordinator：

```sh
./mrcoordinator /tmp/mr-grep.sock pg-*.txt
```

另一个终端也进入 `src/main`，启动两个 worker：

```sh
export MR_GREP_PATTERN='Sherlock|Watson'
./mrworker ../mrapps/grep.so /tmp/mr-grep.sock &
./mrworker ../mrapps/grep.so /tmp/mr-grep.sock &
wait
cat mr-out-[0-9]* | sort
```

也可先用顺序执行器检查应用：

```sh
MR_GREP_PATTERN='Sherlock|Watson' ./mrsequential ../mrapps/grep.so pg-*.txt
```

请在单独的工作目录运行不同作业，避免 `mr-out-*` 相互覆盖。

## 验证

从 `src` 目录执行：

```sh
(cd mrapps && go test -race grep.go grep_test.go)
make RUN='-run ^TestGrep$' mr
```

应用测试覆盖正则匹配、重复文本、中文、空文件、空行、无结尾换行和超长行。
`TestGrep` 使用两个输入文件、三个 worker，并与显式预期输出比较。
`mrapps` 中每个文件都是独立插件，不能将整个目录作为一个 Go 包编译或测试。
