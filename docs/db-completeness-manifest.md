# Database Completeness Manifest

本页是数据库完整性机器清单的文档入口。权威机器源是
[`testdata/db-completeness.manifest`](../testdata/db-completeness.manifest)，人工任务表是
[`docs/db-completeness-tasks.md`](db-completeness-tasks.md)，逐产品操作证据位于
[`docs/test-evidence/`](test-evidence/README.md)。

当前共 27 项：24 `COMPLETE`、1 `BLOCKED`、2 `EXTERNAL`、0 `PARTIAL`。

- `BLOCKED`: IBM Db2 需要正式安装 64 位 Data Server Client、已 source
  `db2profile` 的 x86_64 self-hosted runner；服务器容器库复制方案已通过架构、依赖和
  注册检查，但在环境句柄初始化返回 `IM004`，不能冒充受支持客户端。
- `EXTERNAL`: Redshift DSN、AutoMQ/WarpStream/Confluent vendor DSN。
- ScyllaDB 已从 alias-only `PARTIAL` 升级为真实产品 `COMPLETE`，证据见
  [`scylladb.md`](test-evidence/scylladb.md)。
- SQL Server 2022 CU26 已在 GitHub Actions x86_64 官方产品容器上完成完整 SQL
  family checklist；Apple Silicon 本机不再被误当成产品运行环境，证据见
  [`sqlserver.md`](test-evidence/sqlserver.md)。
- Db2 托管阻塞的逐次实验与解除条件见
  [`db2-host-runtime-blocker.md`](test-evidence/db2-host-runtime-blocker.md)。

更新流程：先运行真实产品 family checklist；再写逐资源 evidence；最后同步 manifest、
任务表和状态文档，并执行：

```bash
./scripts/validate-db-completeness.sh
./scripts/validate-final-goal.sh
```

缺少运行时或 DSN 时必须保留 `BLOCKED`/`EXTERNAL`。跳过、协议别名和兼容产品运行均
不得冒充命名产品的 `COMPLETE`。
