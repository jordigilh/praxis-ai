# `redis-rs` 1.7.0 production-suitability result

## Decision by topology

| Topology | Result | Required profile |
|---|---|---|
| Standalone | Go for the repaired accounting candidate | One cached multiplexed data connection with 500 ms connect/response bounds; no automatic mutation replay. |
| Sentinel | Go for the repaired accounting candidate through the qualified adapter | Cache the writable primary; bound the complete read-only discovery loop to 3 s; coalesce discovery; invalidate stale generations; retry only discovery/connection before dispatch. |
| Native Cluster | No-go for the unchanged contract | Exact aggregate caps, IDs, cleanup, and telemetry remain slot-incompatible with cross-primary subject distribution. |

## Runtime evidence

- Redis 8.10.2 and Red Hat Valkey 8.0.11 pass the same repaired standalone and Sentinel suites under `EVAL` and `EVALSHA`.
- Sentinel steady state reuses the cached data connection and performs no discovery round trip.
- Planned and unplanned promotion preserve offset-verified state; stale connections fail once, are invalidated, and the failed mutation is not replayed.
- One unavailable Sentinel is tolerated. Discovery retries stay within one outer deadline.
- `NOREPLICAS`, `READONLY`, and `MASTERDOWN` are known-not-applied. Timeouts, disconnects, malformed successful replies, and other post-dispatch errors are unconfirmed.
- `redis::Script` cold-cache and post-promotion recovery pass only after a confirmed `NOSCRIPT`; ambiguous dispatch is never retried.

## Boundaries

The adapter does not add per-write `WAIT` or `WAITAOF`; replication remains asynchronous and acknowledged writes are not RPO=0. Sentinel and data-node credential/TLS separation remains coordinated with #833. Numeric dedicated-hardware latency/throughput is not claimed by this local functional run; #1421's architectural performance gate is steady-state round-trip parity, which passes.

Evidence: both `*-fixed-standalone.json` reports, all four `*-sentinel-{eval,evalsha}.json` reports, `result-matrix.json`, and `environment.json`.
