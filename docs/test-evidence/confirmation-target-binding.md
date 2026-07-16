# 破坏性操作连接目标隐藏绑定证据

状态：PASS（2026-07-16）

范围：所有 CLI/TUI 破坏性 SQL/CQL、Document、KV、Messaging、Search 与 transfer import
确认路径。本项不增加写权限；它只保证确认 token 精确绑定实际 resolved connection identity。

## 威胁与合同

userinfo、任意 query 和 fragment 必须从公开 DSN 隐藏，但脱敏会让多个实际连接折叠到同一个
display。若直接以 display 计算 token，不同 credential、tenant 或 provider route 可能复用
同一确认。因此实现分成两层：

1. `safety_target_display` 只生成可公开的 `conn:<name>` 或 redacted DSN，供
   `impact.target` 和 transfer artifact 使用。
2. `confirmation_target` 通过 `SafetyGuard::bind_target_scope` 把完整、已展开的 resolved DSN
   变成不公开的内部 scope；token 使用完整内部 target，impact 只使用 display。

内部 SHA-256 scope 不进入 JSON、Table/NDJSON、TUI 文本或 artifact，避免把低熵凭据的公开
hash 变成离线猜测验证器。named connection 内容变化、`${ENV}` 展开变化以及同时出现
`--conn/--dsn` 时的 `--dsn` 优先级都会改变或选择正确 token identity。

## 调用面审计

| 调用面 | 结果 |
| --- | --- |
| SQL / CQL destructive statement | bound target 直接传 `check_with_target` |
| Document drop / many mutation / aggregate out-merge | bound target；aggregate 先构造纯 display，再绑定一次，hidden suffix 不参与字符串拼接 |
| KV raw mutation / Messaging delete / Search delete-index | bound target + 原有操作语义 scope |
| transfer import replacement | bound target；export `source.connection` 只保存 display |
| TUI destructive SQL | 解析 template 后以 `Dsn::raw` expanded value 绑定 |
| connection config replace/remove | 独立绝对 OS path identity 已包含在配置操作 scope，不复用 DSN helper |

## 验证

| 测试 | 结果 |
| --- | --- |
| Core SafetyGuard | 16/16：hidden scope 不公开、同 display 不同 scope token 不同、旧 target 兼容 |
| CLI 真实进程 `conn_crud` | 15/15：credential/query/fragment、named DSN change、stale replay、env expansion、selector precedence |
| CLI command unit | 102/102 PASS，包含 aggregate/transfer/SQL/CQL/Document/KV/MQ/Search safety paths |
| CLI JSON / transfer artifact | 35/35、10/10 PASS |
| TUI | 33/33：不同 raw/expanded DSN token identity 且 display 无 secret |
| 静态门禁 | Core/CLI/TUI all-target Clippy `-D warnings`、rustfmt、diff check PASS |

两个测试 DSN 的公开 target 可完全相同，但 `confirm_token` 必须不同；把第一个 token 重放到
同名连接的新 DSN 会在连接前返回 mismatch。所有 marker 同时在 stdout、stderr、impact 和
内部 target 的可见部分中断言不存在。
