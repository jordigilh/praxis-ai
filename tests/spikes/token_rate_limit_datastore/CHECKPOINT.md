# Parked HA checkpoint and sharding handoff

Status: 2026-09-30. HA is parked pending the sharding investigation and review of
its results. Do not open an HA implementation PR from this checkpoint.

## What is preserved

This branch preserves the existing Sentinel feature work at `877a6382`, stacked
on the mixed-configuration repair `b6fe4704` from #1442. That implementation is
qualification evidence, **not** the agreed final production integration.

This directory checkpoints the previously local datastore spike: source code,
immutable source fixtures, pinned lockfile, runners, repaired results, historical
failures, and the harness PTTL false-negative diagnosis/regression. It is excluded
from the production Cargo workspace and is not a published Rust package.

The files copied from the local harness are intentionally preserved byte-for-byte.
Their original README and historical reports describe their pre-checkpoint
local/unpublished status; this file supersedes that publication-status wording.
The host/environment manifest remains the record of the actual local run, not a
new benchmark or a claim that the checkpoint has already run elsewhere.

The unposted #1421 ownership amendment is preserved in
`evidence/drafts/issue-1421-backend-ownership.md`; it still requires user approval
before posting and does not describe an implemented shared-backend integration.

The user authorized committing and pushing this checkpoint branch. No registry
pull secret, credential values, build output, or remote-host preflight notes are
included. Registry authfiles remain external and must not be printed or committed.

## Qualified result and limits

Redis 8.10.2 and Red Hat Valkey 8.0.11 pass the repaired standalone and Sentinel
contracts under EVAL and EVALSHA. All six repaired runtime reports have zero
failures; each standalone report contains both invocation methods. The final local
runner passed 11 harness tests and Clippy; generated evidence is byte-idempotent.

Both products support the tested accounting and single-primary HA contract. The
unchanged native-Cluster layout does not qualify for sharded token accounting.
Putting the entire namespace in one hash slot is not a sharding solution.
Numeric latency/throughput qualification remains separate under #1482; production
invocation remains EVAL, with an EVALSHA migration tracked separately in #831.

## Architectural requirements for the sharding investigation

- Recall Engram project methodology and the manually synthesized model
  `praxis-state-storage-ha-ownership`. Vertex AI is unavailable for Engram memory
  synthesis: use manual synthesis/retention, not reflection or model refresh.
- Align with [enhancements PR #17](https://github.com/praxis-proxy/enhancements/pull/17)
  at `31aa004`. It is still a proposed API, not a merged implementation. Consumers
  use named shared backends rather than owning clients, pools, Sentinel managers,
  or shard maps. Use supported native backend/client topology capabilities rather
  than a bespoke Praxis topology shim.
- Preserve typed atomic reserve/reconcile operations. The proposed generic
  KvStore atomic CAS/increment capability does not prove whole-ledger atomicity.
  PR #17 leaves scripted token ledgers to proposal `00121`, borrowing the named
  backend pool.
- Preserve exact aggregate cap, reservation-ID, cleanup, fingerprint, and
  telemetry semantics unless a change is explicitly approved. Identify an
  unsatisfiable requirement honestly; do not force a passing result by silently
  weakening it or moving all state to one shard.
- Declare every Lua-accessed key, including cleanup keys, and keep each atomic
  group in one Cluster slot while demonstrating actual distribution across
  multiple primary shards.
- Qualify Redis and Valkey symmetrically under native Cluster sharding-only and
  sharding + HA. Use native Cluster failover, not Sentinel layered over Cluster.
  Keep native slot routing, resharding, and topology ownership in the backend and
  maintained client; do not build a Praxis shard map or cross-shard coordinator.
- Preserve bounded operations/backlog and no replay of ambiguous mutations by any
  client or intermediary. Validate the resolved redis-rs configuration rather
  than trusting default retries or equating redirection support with retry safety.
- Keep asynchronous replication. Do not add production WAIT/WAITAOF or claim zero
  write loss. This is an accepted policy, not an outstanding design blocker.
- Give any revised schema/algorithm its own immutable candidate ID, digest,
  isolated namespace, and migration contract. Preserve historical failures and
  current HA reports rather than overwriting them with a different candidate.

## Sharding deliverable and publication boundary

Produce an evidence-backed go/no-go report for both exact products, the key-slot
and aggregate-accounting design, client retry/routing safety, resharding and
replica-promotion behavior, and implications for the chosen shared-backend HA
integration. State clearly what was executed, what remains unproven, and any
required redesign or approval. Tests cannot establish absolute certainty under
all faults; do not present finite qualification as that guarantee.

Work on a separate sharding branch in its own dedicated worktree. Keep this HA
checkpoint branch and checkout unchanged.
Do not open an HA or sharding PR, change the published issue scope, or post more
results without user approval. The user will follow the sharding session and
return to the HA session after reviewing its results.

Existing approved architectural finding:
[#843 comment](https://github.com/praxis-proxy/ai/issues/843#issuecomment-5914411977).

## Reproduce the checkpointed harness

Run from this directory, not the production workspace:

```sh
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo fmt --check
./scripts/run_repaired_gate.sh /path/to/red-hat-pull-secret.json
```

The fixture fetcher pins source commits. The runner uses isolated containers and
overwrites repaired runtime reports; preserve a candidate's reports before
rerunning or changing the candidate. The authfile stays outside this directory.

For the current result start with `evidence/ha-spike-summary.md`, `evidence/report.md`,
`evidence/result-matrix.json`, and `evidence/redis-rs-production-suitability.md`.
