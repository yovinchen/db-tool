# dbtool-bin

`dbtool-bin` is the pip/uv wrapper for the `dbtool` CLI. Release wheels are generated from GitHub Actions binary artifacts by `scripts/package-python-wheel.py`; the wrapper does not rebuild Rust code during packaging.
