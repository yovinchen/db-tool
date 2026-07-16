# 本地连接配置有界化证据

状态：PASS（2026-07-16）

范围：IF-T79。本项只证明本地配置、环境目录、DSN 解析和 `conn list` 输出有限；数据库目录
标量字节属于 IF-T74，通用 mutation input 属于 IF-T78，不互相冒充完成。

## 固定边界

| 层级 | 上限 | 超限行为 |
| --- | ---: | --- |
| `connections.toml` | 1 MiB、regular file | 在 TOML parse 前拒绝；symlink、directory/device、读取中增长或路径替换均 fail closed |
| 文件条目 | 1024 | bounded map deserializer 在保留下一个条目前拒绝 |
| 配置名称 / DSN / 文本 limit | 256 B / 16 KiB / 256 B | 不回显字段内容；保存前再次验证 |
| 环境连接目录 | 256 项、512 KiB 累计 | 非 UTF-8、控制名称、规范化冲突、单值或累计超限均返回 `CONFIG_ERROR` |
| raw / expanded DSN | 16 KiB | parser 在 clone/connect 前校验 raw，并在每个 `${ENV}` fragment 与每轮展开时校验 |
| `conn list` | `min(--limit,512)`、`min(--max-bytes,256 KiB)` | 固定 metadata 不可容纳时 stdout 为空并返回 `READ_BUDGET_EXCEEDED` |

保存路径先对 modeled payload 和 TOML basic-string 最坏转义大小做上界计算，再分配输出 String；
序列化后还有实际大小复核，任何失败都发生在同目录临时文件发布前，旧配置保持完整。

## 输出和路径合同

- JSON、Table、NDJSON 的最终 stdout 长度连同末尾换行都不超过 caller byte budget。
- 所有 DSN userinfo、任意 query/fragment、不可解析值和错误源行都不会进入输出。
- `conn list/add/remove` 使用绝对词法配置路径；普通 UTF-8 `data.config_path` 保持真实可用路径。
- 控制字符只以可见转义输出；确认 scope 使用独立 injective `utf8:/bytes:/wide:` identity，
  因而 ESC 与字面转义、非 UTF-8 与 replacement character、相对 XDG 的不同 cwd 不会共享 token。

## 自动化证据

| 层级 | 结果 |
| --- | --- |
| Core | 139/139 + doctest 1/1 PASS |
| CLI `conn_crud` | 15/15 PASS：CRUD、原子失败、item/byte/fixed envelopes、环境/DSN、路径与 token 绑定 |
| CLI conn unit | 8/8 PASS |
| CLI `cli_json` | 35/35 PASS |
| 静态门禁 | Core/CLI all-target Clippy `-D warnings`、rustfmt、`git diff --check` PASS |

关键命令：

```text
cargo test -p dbtool-core
cargo test -p dbtool-cli --test conn_crud
cargo test -p dbtool-cli --bin dbtool cmd::conn::tests
cargo test -p dbtool-cli --test cli_json
cargo clippy -p dbtool-core -p dbtool-cli --all-targets -- -D warnings
cargo fmt --all -- --check
git diff --check
```

## 已记录边界

- 当前主机不是 Windows；drive-relative/rooted path cfg 回归已实现，但当前 SHA 的 Windows x64
  runtime 与 arm64 compile/link 仍必须由 IF-T68 Windows 门禁提供，不能把 cfg 单测写成 live PASS。
- 同 inode、同长度且在读取期间原地改写的外部进程无法仅靠 portable file metadata 完全识别；
  size、inode/path swap 和 symlink/FIFO/device 路径已经关闭。
- `conn add/remove` 保证单次原子发布，不声明跨进程 compare-and-swap 或全局文件锁语义。
- 最坏转义预检刻意保守，极端但实际可落入 1 MiB 的 modeled 配置可能被提前拒绝。
