# Redis Completeness Evidence

Task ID: DB-REDIS-001

Result: LIVE_PASS

Run at (UTC): 2026-07-15T20:50:05Z (focused KV artifact v3 lifetime refresh; binary/raw refresh 2026-07-15T20:14:52Z; original full-family run 2026-07-14T20:13:29Z)

Environment: Docker on macOS arm64; Rust 1.96.0; Redis 7.4.9

Product version: Redis 7.4.9

Command: `./scripts/integration-test.sh`; `./scripts/integration-fixture-data-test.sh`; `./scripts/integration-data-roundtrip-test.sh`; focused `kv_binary_raw redis_live_binary_values_and_raw_policy_are_exact`; focused `live_services redis_live_kv_lifecycle_and_raw_safety`; `DBTOOL_RUN_INTEGRATION=1 DBTOOL_IT_REDIS_DSN=redis://127.0.0.1:16379/0 cargo test -p dbtool-cli --test transfer_artifacts redis_artifact_v3_preserves_lifetimes_skips_expired_and_binds_replacement -- --exact --nocapture`; adapter live non-UTF-8 SCAN test

Resource operations:

| Resource | Create | Insert/write | Read all fixture data | Update/overwrite | Targeted delete | Metadata/admin | Guard | Limit/transfer | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_redis_{key,ttl,nx,raw,counter,scan}_*` | 30 isolated keys PASS | SET plus confirmed raw SET/INCR, atomic NX+TTL, and 25 scan keys PASS | five scalar fixtures plus the exact set of all 25 scan keys PASS | main value `alice` to `alice-updated`; second NX rejected and preserved `created-once` PASS | DEL reported 30/30 PASS | PING, ordinary TTL and NX TTL 1..30 PASS | raw mutation required write flag and two-step confirmation; FLUSHALL fail-closed; unmet NX explicit PASS | exact N=2 returned `truncated=false`; N+1 returned true; 25-key multi-page scan returned complete deduplicated set PASS | all scalar GETs null and scan empty PASS |
| `dbtool_it_kv_binary_raw_*` | binary, empty, text and confirmed-raw keys PASS | canonical base64 bytes `00 ff 68 65 6c 6c 6f`, zero bytes, compatible UTF-8 and raw SET PASS | get preserved exact typed bytes; empty differed from missing; raw GET remained binary PASS | changed raw value/key could not reuse token PASS | DEL reported 4 existing keys plus cleanup probe 1/1 PASS | encoding was binary/utf8/null exactly PASS | no-write raw SET blocked; FLUSHALL/SELECT/EVAL/FUNCTION/unknown/KEYS/HGETALL returned CONFIG_ERROR PASS | 1 MiB/arg, 8 MiB request/response and 10,000 RESP-node contracts covered offline/adapter PASS | all five keys missing; prefix scan `[]` PASS |
| `dbtool_it_non_utf8_*:<ff>` | one binary Redis key PASS | direct fixture SET PASS | portable scan returned `SERIALIZATION_ERROR` rather than lossy text or partial success PASS | N/A | direct fixture DEL 1/1 PASS | explicit cursor loop reached the matching page PASS | portable UTF-8 key boundary enforced PASS | page decode failure propagated PASS | binary fixture key deleted PASS |
| `dbtool_it_fixture:user:{1,2,3}` | three source keys PASS | Alice/Bob/Carol 3/3 PASS | MGET exactly `alice,bob,carol` PASS | N/A | N/A | SCAN count 3 PASS | bounded prefix only | exported all 3 keys PASS | source keys deleted PASS |
| `dbtool_it_roundtrip:user:{1,2,3}` | import-created keys PASS | public import restored 3/3 PASS | MGET exactly `alice,bob,carol` PASS | N/A | DEL removed 3/3 PASS | SCAN count 3; source persistent lifetime preserved as PTTL `-1` PASS | import required `--allow-write` | JSON export/import v3 with prefix remap and explicit expiry PASS | restored keys deleted PASS |
| `dbtool_it_artifact_*:{source,target}:*` | persistent, binary, empty, 120-second and 1-second keys PASS | v3 export captured exact bytes plus persistent/absolute expiry; import restored 4 and skipped expired 1 PASS | binary `AP8=`, empty bytes, persistent text and long-lived value exact PASS | replacement restored 4 existing targets; expired entry remained skipped PASS | final bounded DEL issued for all source/target names PASS | persistent target PTTL `-1`; long target deadline did not exceed artifact deadline PASS | incomplete artifact rejected; default NX conflict explicit; token changed when expiry changed PASS | partial N+1 export rejected; v2/missing expiry rejected before DSN; multi-key `atomic=false`, per-entry atomic PASS | source and target prefix scans both `[]` PASS |

Assertions: connection and KV capability, SET/GET/overwrite, atomic NX+TTL,
explicit NX conflict without overwrite, TTL, exact N/N+1 truncation, complete
multi-page SCAN with de-duplication, non-UTF-8 failure propagation, exact binary/
empty/text/missing distinction, canonical base64 input, typed bounded raw output,
confirmed raw mutations, fail-closed dangerous/unbounded/unknown commands,
complete content/deletion verification, and public export/import all passed. The
KV v3 refresh additionally proved per-key absolute-expiry preservation, expired
skip without SET, no TTL extension, and expiry-bound replacement confirmation.

## IF-T78 exact mutation refresh

Run at (UTC): 2026-07-16T12:13:31Z

`redis_live_budgeted_kv_mutations_reject_before_write_and_round_trip` passed
1/1 against Redis 7.4.9. The adapter advertised exact SET, lifetime restore,
DEL, and raw mutation request/response operations. N-1 SET/restore/raw and N+1
DEL returned `INPUT_BUDGET_EXCEEDED` while all corresponding keys remained
absent or unchanged. Exact operations preserved binary bytes, persistent
lifetime, NX condition results, two-key delete counts, raw `OK`, and raw value
readback. A one-byte raw response budget failed after SET as
`OUTCOME_INDETERMINATE`, and direct bounded read proved the value existed.
Final exact DEL removed every test key and the unique prefix scan was empty.

IF-T78 fixture resource operations:

| Resource | Create | Insert/write | Read all fixture data | Update/overwrite | Targeted delete | Metadata/admin | Guard | Limit/timeout | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| accepted key group `dbtool_it_input_<suffix>:{set,restore,delete:one,delete:two,raw,raw-response}` | six isolated keys created by exact SET/restore, direct delete fixtures, and raw SET PASS | binary SET, persistent restore, two raw SETs and two direct delete fixtures PASS | exact binary `00 ff 5a`, `restored`, `raw-value`, and post-response-budget value read back PASS | NX restore replacement rejected and original preserved PASS | exact two-key DEL returned 2; final six-key DEL and prefix scan empty PASS | persistent restore semantics and exact operation advertisement PASS | request N-1/N+1 rejected without changing matching state; raw response overflow marked `OUTCOME_INDETERMINATE` PASS | exact item/batch and raw response budgets exercised PASS | final prefix scan returned 0 keys PASS |
| rejected preflight attempts on `dbtool_it_input_<suffix>:{set,restore,raw,delete:*}` | N-1 SET/restore/raw created 0/3 keys PASS | rejected before Redis command PASS | absent keys remained absent; both delete fixtures remained readable PASS | N/A | N+1 two-key DEL removed 0/2 PASS | prefix remained limited to accepted/direct fixtures PASS | `INPUT_BUDGET_EXCEEDED` PASS | N-1 byte and N+1 item envelopes rejected PASS | no separate rejected resource remained |

Verification: adapter-redis 53/53 PASS; Redis Docker exact KV mutation 1/1
PASS; strict Clippy, rustfmt, and diff check PASS. Implementation commit:
`cfbb998`. Valkey/KeyDB/Dragonfly share this implementation but were not rerun
for IF-T78, so their earlier complete compatibility evidence is not rewritten
as a new live pass.

Cleanup: PASS

Commits: `974886f`, `561ea93`, `bea6bed`, `74a4907`, `19a3527`, `1ceffc8`,
`2dd5590`, `cfbb998`, IF-T57/IF-T62/IF-T64/IF-T78
