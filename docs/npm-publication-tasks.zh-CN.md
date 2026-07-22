# npm 发布任务表

最后更新：2026-07-22

这个任务表把“已经可以生成 npm 包”和“已经公开发布到 npm”分开记录。
npm 版本不可覆盖，因此入口包只有在同版本的六个平台包都准备好后才能发布。

| Task | 优先级 | 状态 | 已完成内容 / 解除条件 | 证据 |
| --- | --- | --- | --- | --- |
| NPM-T01 包拓扑 | P0 | 完成 | 固定 6 个平台包（Linux/macOS/Windows × x64/arm64）和 1 个入口包 `@yovinchen/dbtool`；入口包用 optionalDependencies 映射宿主平台。 | `scripts/package-npm.mjs`、`dist/npm/bin/dbtool.js` |
| NPM-T02 发布元数据与许可证 | P0 | 完成 | 主包和平台包均包含 MIT、Apache-2.0 文件、`MIT OR Apache-2.0`、公开 npm registry 和规范的小写 GitHub 源地址。 | `LICENSE-MIT`、`LICENSE-APACHE-2.0`、`node scripts/package-npm-test.mjs` |
| NPM-T03 安装与调用 | P0 | 完成 | 离线安装入口包和当前宿主平台包后，可通过 Node wrapper 执行原生 binary；保留 `DBTOOL_BINARY` 显式覆盖。 | `verifyOfflineInstall`、`verifyBinaryOverride` |
| NPM-T04 失败输出 | P1 | 完成 | 缺少平台包时只输出一行可操作错误，不泄漏 Node 调用栈，并说明重装或 `DBTOOL_BINARY`。 | `verifyWrapperFailures` |
| NPM-T05 完整矩阵门禁 | P0 | 完成 | 默认缺少任一平台 binary 都失败且不清空既有输出；完整 fixture 矩阵生成 7 个 tgz，并对每个包执行 `npm publish --dry-run`。 | `node scripts/package-npm-test.mjs` |
| NPM-T06 六平台真实制品 | P0 | 未开始（发布前阻塞） | 官方发布当前只有 macOS ARM64；需要为同一版本生成并验证其余 5 个真实平台 binary，禁止用 fixture 或其它架构文件替代。 | 解除条件：六个平台 release artifact 均通过目标平台 smoke |
| NPM-T07 npm 身份与 scope | P0 | 外部阻塞 | 当前 `npm whoami` 返回 `ENEEDAUTH`。需要确认 `@yovinchen` scope 控制权、开启发布所需 2FA，首次建立 7 个包。 | 解除条件：授权账户可查询并管理全部包名 |
| NPM-T08 可信发布与 provenance | P1 | 外部阻塞 | 首次发布后，为 7 个包配置 GitHub Actions trusted publisher；workflow 使用 npm 支持的 Node/npm 版本和 `id-token: write`，并生成 provenance。 | npm 官方 trusted publishing / provenance 文档 |
| NPM-T09 正式发布 | P0 | 外部阻塞 | 先发布 6 个平台包，全部可查询后最后发布入口包；在 Linux、macOS、Windows 分别执行全新安装和 `dbtool --version`。 | 解除条件：NPM-T06–08 全部完成并保存 registry/install 证据 |

建议的正式顺序：

1. 从同一个不可变 tag 构建并验证六个平台二进制。
2. 执行 `node scripts/package-npm.mjs <artifact-root> <out-dir> v1.0.1`。
3. 再次执行 7 个 `npm publish --dry-run`，核对包名、版本、OS/CPU、许可证和 provenance 源地址。
4. 按 Linux x64、Linux arm64、macOS x64、macOS arm64、Windows x64、Windows arm64 的顺序发布平台包。
5. 确认平台包在 registry 可见后，最后发布 `@yovinchen/dbtool`。
6. 在三个操作系统进行无缓存安装与 CLI 运行验收。

官方规则参考：

- [发布公开 scoped package](https://docs.npmjs.com/creating-and-publishing-scoped-public-packages/)
- [Trusted publishing](https://docs.npmjs.com/trusted-publishers/)
- [Provenance statements](https://docs.npmjs.com/generating-provenance-statements/)

