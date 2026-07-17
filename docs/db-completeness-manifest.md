# Database Completeness Manifest

本页是数据库完整性机器清单的文档入口。权威机器源是
[`testdata/db-completeness.manifest`](../testdata/db-completeness.manifest)，人工任务表是
[`docs/db-completeness-tasks.md`](db-completeness-tasks.md)，逐产品操作证据位于
[`docs/test-evidence/`](test-evidence/README.md)。

当前共 27 项：23 `COMPLETE`、2 `BLOCKED`、2 `EXTERNAL`、0 `PARTIAL`。

- `BLOCKED`: SQL Server 本机 ARM64 产品容器、IBM Db2 主机 ODBC runtime。
- `EXTERNAL`: Redshift DSN、AutoMQ/WarpStream/Confluent vendor DSN。
- ScyllaDB 已从 alias-only `PARTIAL` 升级为真实产品 `COMPLETE`，证据见
  [`scylladb.md`](test-evidence/scylladb.md)。

更新流程：先运行真实产品 family checklist；再写逐资源 evidence；最后同步 manifest、
任务表和状态文档，并执行：

```bash
./scripts/validate-db-completeness.sh
./scripts/validate-final-goal.sh
```

缺少运行时或 DSN 时必须保留 `BLOCKED`/`EXTERNAL`。跳过、协议别名和兼容产品运行均
不得冒充命名产品的 `COMPLETE`。
