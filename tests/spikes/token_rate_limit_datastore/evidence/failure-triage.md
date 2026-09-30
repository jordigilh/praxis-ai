# Mixed-configuration accounting failure triage

## Classification

- **Status:** confirmed, deterministic, symmetric across the two exact product pins
- **Code-defect confidence:** 99/100
- **Severity:** high — silent fail-open quota enforcement
- **Confidence:** high — runtime consequence reproduced under both `EVAL` and `EVALSHA`, with source-level causality
- **Trigger:** two writers address the same datastore, namespace, rule, algorithm, and subject while using different semantic configurations
- **Affected:** Redis 8.10.2 and Red Hat Valkey 8.0.11; standalone and any Sentinel topology using the same scripts
- **Not the cause:** server vendor, `EVAL` versus `EVALSHA`, script-cache behavior, or RESP framing

The remaining one point of uncertainty concerns real deployment exposure, not whether the code path is defective: no production namespace/rule/config inventory was inspected and the reproducer is a datastore-level harness rather than a full Praxis request E2E. Current upstream `main` at `ed5527974b364f1143d18b242a891e20d1471ca2` was checked on 2026-09-29 and still contains the affected pruning, expiry, token interpretation, and key-identity paths. Runtime evidence is frozen at audited commit `56af0e37e4bda2f06c7e33546db66a8d01fce0b7`.

## Root cause

The state identity is incomplete. `backend.rs` derives physical keys from namespace, rule, subject, and an algorithm marker, but does not include or validate the accounting configuration. Every invocation then supplies its local window/capacity/refill/timeout values to Lua and mutates the shared state as though all writers agree.

Three independent mutation paths make this unsafe:

1. **Unconditional expiry replacement.** Sliding reserve lines 159–164 and token-bucket reserve lines 87–110/125–129 issue `PEXPIRE` with the caller's local horizon. A shorter-horizon writer replaces a longer remaining TTL.
2. **Destructive sliding-window pruning.** `sliding_window_reserve.lua:85–90` removes settled entries according to the current writer's window. A 10 ms writer therefore deletes usage that a coexisting 10 s writer still requires.
3. **Configuration-dependent token interpretation.** `token_bucket_reserve.lua:77–87` reads one shared `tokens`/`last_refill_ms` pair and applies the current writer's capacity/refill rate. The key-retention score is likewise replaced using the current horizon.

Reservation timeout is also caller-local: `sliding_window_reserve.lua:96–108` can expire another writer's active reservation early. No server-side configuration fingerprint or mismatch error exists.

## Runtime consequence

Both exact products produced the same results for `EVAL` and `EVALSHA`:

- **Sliding window:** a settled entry under a 10 s/5-token policy existed before the 10 ms writer and was gone afterward (`ZCARD 1 → 0`). The 10 s writer then returned an admitted reply even though its 5-token capacity had already been consumed.
- **Token bucket:** a 10-token bucket refilling at 1 token/s was depleted. A 10 token/s writer denied the immediate request but reduced state TTL to ~1.1 s. After 1.2 s the state no longer existed, and the 1 token/s writer admitted all 10 tokens again rather than having only ~1.2 tokens available.

These operations return normal admitted/denied replies. They do not surface a backend error, so the caller cannot map the condition to 503. Expected 429 enforcement can silently become an admission.

## Blast radius

The defect requires shared key identity plus configuration skew. Plausible triggers are:

- a rolling policy/configuration update with old and new replicas overlapping;
- two deployments sharing a datastore and the default namespace;
- two independent filter instances reusing the same namespace and rule name;
- an intentional capacity, window, refill-rate, or reservation-timeout change without a state migration contract.

A homogeneous fleet with one immutable configuration is not affected by this specific mixed-writer failure. Existing affected state is ephemeral, but over-admission, lost overage reconciliation, inaccurate telemetry, and cap/index inconsistencies can occur during the overlap.

## Recommended repair

### Required correctness fix

Add a server-side, schema-versioned accounting-configuration fingerprint per rule/algorithm and check it atomically before every mutation. The fingerprint should cover at least:

- algorithm and state-schema version;
- canonical sliding budgets, or token-bucket capacity and exact refill representation;
- reservation timeout;
- retained-key and active-reservation limit semantics.

The first writer may register the fingerprint. A mismatch must return a distinct backend-unavailable result **before any state mutation**, mapping to fail-closed 503 behavior. Deliberate semantic changes need an explicit state generation/namespace transition and a documented reset, drain, or migration policy.

### Defense in depth, not a complete fix

Replace unconditional expiry updates with monotonic extension (`PEXPIRE ... NX` followed by `PEXPIRE ... GT`, or an equivalent atomic helper) on every relevant state and telemetry key. This prevents the measured TTL shortening, but **does not** fix destructive sliding pruning or incompatible token-bucket interpretation; it cannot qualify on its own.

Do not merely put an automatic config hash into key names: during a rolling update that creates two independent budgets and can double effective capacity unless an explicit cutover/migration protocol accompanies it.

## Immediate mitigation

Until repaired:

1. Freeze accounting-semantic changes for a live namespace/rule.
2. Require a unique namespace per deployment; audit use of the default namespace and duplicate rule names across deployments.
3. Do not perform an overlapping rolling update that changes window, capacity, refill rate, or reservation timeout.
4. If an emergency change is necessary, use an explicit new state generation/namespace and treat the resulting budget reset as an acknowledged operational event.

## Regression gate

A fix should prove on both product pins and both invocation modes that:

- same-fingerprint writers interoperate;
- a mismatched writer receives the designated 503-class result before mutation;
- PTTL, settled history, active reservations, token state, index scores, counters, and telemetry are byte-for-byte/semantically unchanged after a rejected mismatch;
- an existing reservation still reconciles exactly once after a mismatch attempt;
- both long→short and short→long transitions, capacity/refill changes, and timeout changes are covered;
- an explicit generation change has documented reset/migration behavior.

## One-command reproduction

From the harness root:

```sh
./scripts/reproduce_mixed_config_bug.sh
```

The script starts the exact pinned Redis image on an isolated loopback port, runs both `EVAL` and `EVALSHA` scenarios, asserts the TTL and functional consequences, prints `CONFIRMED`, and removes the container. The inner harness intentionally exits 1 because the correctness assertions fail; the wrapper exits 0 only when every expected failure is observed.

The underlying sequence is:

1. Consume and settle all 5 tokens under a 10 s sliding window.
2. Wait 30 ms and invoke the same key through a 10 ms writer.
3. Observe settled history fall from one entry to zero, then observe the 10 s writer admit again.
4. Deplete and settle a 10-token bucket configured to refill at 1 token/s.
5. Invoke the same key through a 10 token/s writer with a short timeout; it denies but leaves ~1.1 s PTTL.
6. Wait 1.2 s, observe the state is absent, then observe the 1 token/s writer admit all 10 tokens again.

## Evidence

- `runtime/redis-8.10.2-standalone.json`
- `runtime/red-hat-valkey-8.0.11-standalone.json`
- `../scripts/reproduce_mixed_config_bug.sh`
- `../fixtures/source/baseline/backend.rs`
- `../fixtures/source/baseline/lua/sliding_window_reserve.lua`
- `../fixtures/source/baseline/lua/token_bucket_reserve.lua`
