# Redis/Valkey HA spike — review summary

Date: 2026-09-30.

**Status: the functional HA investigation is complete and ready to present.
The final production integration is not complete.**

## Result

The original symmetric Redis/Valkey failures were caused by mixed accounting
configurations in Praxis, not by a vendor incompatibility. The repair in #1442
rejects incompatible writers before mutation. On that repaired contract, the
authenticated rerun passes every recorded standalone and Sentinel lane:

| Product | Standalone EVAL | Standalone EVALSHA | Sentinel EVAL | Sentinel EVALSHA |
|---|---|---|---|---|
| Redis 8.10.2 | Pass | Pass | Pass | Pass |
| Red Hat Valkey 8.0.11 | Pass | Pass | Pass | Pass |

The six runtime reports cover these eight combinations: each standalone report
contains both invocation methods. All six reports have zero failures.

The qualification uses immutable repair commit
`b6fe4704af72c05167a280d5070a291997d2085d`, exact image digests, and redis-rs 1.7.0.
It does not certify arbitrary Redis-compatible products or untested integrations.

## What the HA evidence demonstrates

- The sliding-window and token-bucket accounting contracts pass, including quota
  denial, refunds, overages, mixed-configuration rejection, and duplicate
  reconciliation behavior.
- Each Sentinel lane has one primary, one offset-verified replica, three
  Sentinels, and quorum two.
- The tested adapter handles an unavailable Sentinel, planned and unplanned
  primary replacement, cold script state, replication-health rejection, and
  ambiguous timeout without mutation replay.
- Healthy cached Sentinel traffic adds no discovery command or backend round
  trip relative to standalone.
- The final runner passed 11 harness tests and Clippy. Generated decision
  artifacts regenerate byte-identically.

A fresh Valkey EVALSHA run initially raised a harness PTTL timing false negative.
The failed report is preserved in `diagnostics/`; exact absolute-expiry comparison
and a unit regression corrected the observer, then the complete matrix passed.

## Architectural conclusion

Proceed toward shared-backend HA for both qualified products, **not toward
upstreaming the current custom consumer-owned Sentinel manager unchanged**.

Align with the proposed unified state/storage interface in
[enhancements PR #17](https://github.com/praxis-proxy/enhancements/pull/17):
consumers use named shared backends; supported backend/client infrastructure owns
topology integration. Do not add a bespoke Sentinel/failover shim for every
consumer. PR #17 is still proposed and does not settle the service model or exact
Sentinel integration.

Preserve the typed atomic token ledger. Generic KV CAS/increment capabilities do
not automatically express the complete multi-key reserve/reconcile operation.
The selected shared-backend integration must be tested in its own right; the
passing existing adapter is evidence, not automatic qualification of a replacement.

## Separate work and accepted limits

- **Sharding:** the unchanged key layout does not qualify for native Cluster.
  Same-slot accounting groups, actual cross-primary distribution, aggregate
  semantics, and state migration belong to sharding work, not single-primary HA.
  This finding is published in the
  [#843 comment](https://github.com/praxis-proxy/ai/issues/843#issuecomment-5914411977).
- **Durability:** asynchronous replication and no production WAIT/WAITAOF remain
  the accepted policy. This is not a zero-write-loss design.
- **Invocation:** production remains EVAL. EVALSHA correctness passing does not
  select the migration tracked in #831.
- **Performance:** no numeric production capacity or latency/throughput
  superiority is claimed. Relative-performance work remains separate under
  #1482; the offered host has only had a read-only preflight, not benchmark runs.

## Evidence and follow-on delivery

The local evidence bundle includes `report.md`, `result-matrix.json`,
`contract.json`, `source-ledger.json`, `environment.json`,
`redis-rs-production-suitability.md`, and all six repaired runtime reports under
`runtime/`. Historical failure and Cluster-negative evidence remains preserved.
The harness and full evidence bundle have not been published.

Production delivery still requires reconciling #1421 with the shared-backend
boundary and qualifying the chosen integration through failover/failure-policy
tests. At the latest check, #1442 remains open and #1421 remains unassigned;
repository policy requires assignment before opening its implementation PR.
