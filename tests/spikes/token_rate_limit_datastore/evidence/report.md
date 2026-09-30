# Local Redis/Valkey datastore qualification — repaired result

## Outcome

The mixed-configuration repair at `b6fe4704af72c05167a280d5070a291997d2085d` qualifies standalone and Sentinel HA on both exact products under `EVAL` and `EVALSHA`. Frozen upstream, proposed-unrepaired `EVALSHA`, and PR #1380 remain historical no-go inputs; they were not overwritten. Native Cluster remains a no-go for the unchanged exact aggregate contract.

## Repaired correctness result

- One schema-versioned fingerprint is persisted per namespace/rule/algorithm identity.
- Sliding fingerprints cover canonical budgets, reservation timeout, `max_keys`, and active-reservation bounds.
- Token-bucket fingerprints cover capacity, exact refill representation, reservation timeout, `max_keys`, and active-reservation bounds.
- Request estimates are excluded. A mismatch returns the distinct `[3]` result before mutation and maps to 503 even under fail-open policy.
- Both products pass allow/deny, refund, overage, duplicate/expired reconcile, mixed-config rejection with unchanged state, exact-once reconciliation, caps, concurrency, timeout, and script-cache scenarios.

## Sentinel HA result

Every lane uses one writable primary, one offset-verified replica, three Sentinels, quorum two, and asynchronous replication. The common suite passes cached steady state, an unavailable Sentinel, `NOREPLICAS`, ambiguous timeout/no replay, planned failover, unplanned primary shutdown, cold script state, and post-failover admission plus 429 enforcement.

| Product | Invocation | Planned failover convergence | Unplanned promotion | Rediscovery after unplanned event |
|---|---:|---:|---:|---:|
| Redis 8.10.2 | `eval` | 11325 ms | 1876 ms | 780 ms / 15 |
| Redis 8.10.2 | `eval_sha` | 11280 ms | 1646 ms | 832 ms / 16 |
| Red Hat Valkey 8.0.11 | `eval` | 11250 ms | 1303 ms | 835 ms / 16 |
| Red Hat Valkey 8.0.11 | `eval_sha` | 11329 ms | 1219 ms | 780 ms / 14 |

Times above are functional observations from a local host, not dedicated-hardware performance claims.

## Decisions

| Decision | Result |
|---|---|
| Repaired `EVAL`, sliding window and token bucket | Go: standalone and Sentinel on both products |
| Repaired `EVALSHA`, sliding window and token bucket | Correctness go: standalone and Sentinel on both products; not selected over production `EVAL` by performance |
| Frozen upstream `EVAL` / unrepaired `EVALSHA` | Historical no-go |
| Frozen PR #1380 plain design | Historical no-go |
| Redis native Cluster, unchanged aggregate contract | No-go |
| Valkey native Cluster, unchanged aggregate contract | No-go |
| Sentinel steady-state round-trip parity | Pass: cached Sentinel uses the same one mutation round trip as standalone |
| Numeric latency/throughput claim | None; local functional host is explicitly non-qualifying for dedicated performance |

## Durability boundary

Praxis waits for the primary's successful mutation response before forwarding upstream, but issues no production `WAIT` or `WAITAOF`. Replication is asynchronous. A primary can fail after acknowledgement but before replication, and a partitioned old primary can accept writes later discarded. Sentinel quorum, persistence, and replication-health write gates reduce risk but do not provide strong consistency or RPO=0.

## Evidence

- `runtime/redis-8.10.2-fixed-standalone.json`
- `runtime/red-hat-valkey-8.0.11-fixed-standalone.json`
- `runtime/redis-8.10.2-sentinel-eval.json`
- `runtime/redis-8.10.2-sentinel-evalsha.json`
- `runtime/red-hat-valkey-8.0.11-sentinel-eval.json`
- `runtime/red-hat-valkey-8.0.11-sentinel-evalsha.json`
- historical `*-standalone.json`, `*-cluster-negative.json`, and `static-audit.json`
- `result-matrix.json`, `contract.json`, `source-ledger.json`, and `redis-rs-production-suitability.md`

All evidence remains local and unpublished.
