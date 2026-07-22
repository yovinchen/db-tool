# 发布依赖安全审计

Result: OPEN_UPSTREAM_REMEDIATION

Run at (UTC): 2026-07-17

Scope: `Cargo.lock`、正式 macOS ARM64 `portable` CLI、未发布的 TUI

## 当前告警

| Dependabot | Severity | Dependency | Locked path | First patched |
| --- | --- | --- | --- | --- |
| [#5](https://github.com/yovinchen/db-tool/security/dependabot/5) / `GHSA-82j2-j2ch-gfr8` | HIGH | `rustls-webpki` | `0.101.7` / `0.102.8` | `0.103.13` |
| [#2](https://github.com/yovinchen/db-tool/security/dependabot/2) / `GHSA-pwjx-qhcg-rvj4` | MEDIUM | `rustls-webpki` | `0.102.8` | `0.103.10` |
| [#3](https://github.com/yovinchen/db-tool/security/dependabot/3) / `GHSA-965h-392x-2mh5` | LOW | `rustls-webpki` | `0.101.7` / `0.102.8` | `0.103.12` |
| [#4](https://github.com/yovinchen/db-tool/security/dependabot/4) / `GHSA-xgp8-3hg3-c2mh` | LOW | `rustls-webpki` | `0.101.7` / `0.102.8` | `0.103.12` |
| [#1](https://github.com/yovinchen/db-tool/security/dependabot/1) / `GHSA-rhfx-m35p-ff5j` | LOW | `lru` | `0.12.5` | `0.16.3` |

这五条告警来自两个依赖，不是五个不同的运行时组件。

## 发布影响

- `rustls-webpki 0.101.7` 由
  `dbtool-cli -> adapter-sqlserver -> tiberius 0.12.3 -> rustls 0.21.12`
  引入，进入正式 `portable` CLI。
- `rustls-webpki 0.102.8` 由
  `dbtool-cli -> adapter-nats -> async-nats 0.37.0` 引入，也进入正式 CLI。
- `rustls-webpki 0.103.13` 已同时用于 SQLx、MongoDB、Redis、Search、AMQP
  以及 NATS 的现代 Rustls verifier，但它不会替换上述两个不兼容的旧依赖槽位。
- `lru 0.12.5` 只由 `dbtool-tui -> ratatui 0.28.1` 引入；TUI 当前不在正式
  Release archive 中。

HIGH 告警需要调用方配置恶意或异常 CRL；当前 SQL Server Rustls 路径传入空 CRL，
仓库也没有 `RevocationOptions`。MEDIUM CRL 告警不在 async-nats 的实际 verifier
路径上。两条 name-constraint LOW 告警仍可能影响 SQL Server 的普通证书校验，
因此只能说明可利用路径受限，不能把告警标成已修复。

## 为什么不能直接更新锁文件

- `rustls 0.21.12` 固定在 `rustls-webpki ^0.101.7`；当前 crates.io 最高
  `tiberius` 仍是 `0.12.3`，没有修复分支可直接升级。
- `async-nats 0.37.0` 固定在 `rustls-webpki ^0.102`；首个迁移到修复系列的
  `async-nats 0.47.0` 同时把 MSRV 提高到 Rust 1.88，并跨越十个 minor 版本。
- `ratatui 0.28.1` 固定在 `lru ^0.12`；稳定版 `ratatui 0.29.0` 仍未迁移，
  不能强制解析到不兼容的 `lru 0.16.3`。

离线 `cargo update --dry-run` 已证明三个旧依赖槽位都不能只靠 lockfile 更新解除。
为追求零告警而切换 SQL Server 到 `native-tls` 可能破坏 Linux musl 自包含和交叉编译，
未经五目标构建与产品 TLS 回归不能合入。

## 解除条件

1. NATS 独立升级到 `async-nats >= 0.47`，明确评审 MSRV 1.88，完成 adapter、
   JetStream、MQ TLS 和五目标 `portable` 构建回归。
2. SQL Server 采用通过五目标验证的安全 TLS feature，或维护迁移到
   `tokio-rustls 0.26 / rustls 0.23` 的受控 Tiberius fork，并完成真实 SQL Server
   TLS 产品测试。
3. TUI 等待 Ratatui 稳定升级，或对 `lru 0.12.5` 做最小受控 backport；不得把
   `^0.12` 强制替换为 API 不兼容的 `0.16.3`。
4. 重跑 `cargo tree --locked`、全仓验证、NATS/SQL Server TLS live、macOS ARM64
   打包，并确认 Dependabot open alerts 清零后，才能把本证据改为 PASS。
