# mise / ubi install metadata

dbtool release assets are named for direct consumption by mise's `ubi` backend:

```text
dbtool-v0.1.0-x86_64-unknown-linux-musl.tar.gz
dbtool-v0.1.0-aarch64-unknown-linux-musl.tar.gz
dbtool-v0.1.0-x86_64-apple-darwin.tar.gz
dbtool-v0.1.0-aarch64-apple-darwin.tar.gz
dbtool-v0.1.0-x86_64-pc-windows-msvc.tar.gz
dbtool-v0.1.0-aarch64-pc-windows-msvc.tar.gz
```

The archive root contains `dbtool` on Unix platforms and `dbtool.exe` on Windows,
plus shell completions under `completions/` and a manpage at `man/dbtool.1`.

Expected mise form:

```bash
mise use -g ubi:YoVinchen/db-tool@latest
```

If a local mise version needs explicit binary selection, use repository `YoVinchen/db-tool`, binary `dbtool`, and the target-specific asset above.
