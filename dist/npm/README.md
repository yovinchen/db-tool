# @yovinchen/dbtool

This package installs the `dbtool` CLI through a small Node.js wrapper. The wrapper dispatches to the platform package installed through `optionalDependencies`.

Release packages include shell completions under `completions/` and a manpage at `man/dbtool.1`.

For local smoke tests without publishing platform packages, set `DBTOOL_BINARY=/path/to/dbtool`.
