# Database Test Evidence

One file is committed for each backend that completes a real capability-family
run. A file is evidence only when it contains all of these literal fields:

- `Task ID:` matching `testdata/db-completeness.manifest`
- `Result: LIVE_PASS`
- `Run at (UTC):`
- `Environment:`
- `Command:`
- `Product version:`
- `Resource operations:`
- `Cleanup: PASS` or an explicit `Cleanup: UNSUPPORTED` boundary

Evidence summarizes only disposable `dbtool_it_*` fixture resources. It must
not contain passwords, raw authenticated DSNs, private certificates, or
unbounded production data.
