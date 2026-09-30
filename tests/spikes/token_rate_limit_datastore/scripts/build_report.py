#!/usr/bin/env python3
"""Build the gated result matrix and concise local spike report."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "evidence"
ALLOWED_STATES = {
    "pass",
    "fail",
    "blocked_by_correctness_gate",
    "unsupported",
    "not_applicable",
}
CANDIDATES = {
    "current_eval": "Frozen upstream Lua invoked with EVAL",
    "proposed_evalsha": "Identical frozen Lua invoked through redis::Script/EVALSHA",
    "plain_pr_1380": "Frozen PR #1380 plain-command design",
}
ALGORITHMS = ["sliding_window", "token_bucket"]
PRODUCTS = {
    "redis_8_10_2": "Redis 8.10.2",
    "red_hat_valkey_8_0_11": "Red Hat Valkey 8.0.11",
}
TOPOLOGIES = ["standalone", "sentinel_ha", "cluster_sharding", "cluster_sharding_ha"]


def load(relative: str) -> Any:
    return json.loads((EVIDENCE / relative).read_text())


def invariant(report: dict[str, Any], candidate: str, name: str) -> dict[str, Any]:
    candidate_report = next(
        entry for entry in report["candidates"] if entry["candidate"] == candidate
    )
    return next(entry for entry in candidate_report["invariants"] if entry["name"] == name)


def cell(
    candidate: str,
    algorithm: str,
    product: str,
    topology: str,
    state: str,
    reason: str | None,
    evidence: list[str],
    gate: str | None = None,
) -> dict[str, Any]:
    assert state in ALLOWED_STATES
    assert evidence
    assert state == "pass" or reason
    return {
        "id": f"{candidate}.{algorithm}.{product}.{topology}",
        "candidate": candidate,
        "algorithm": algorithm,
        "product": product,
        "topology": topology,
        "state": state,
        "reason": reason,
        "evidence": evidence,
        "gate": gate,
    }


def correctness_gate_reason(candidate: str, algorithm: str) -> str:
    if candidate in {"current_eval", "proposed_evalsha"}:
        if algorithm == "sliding_window":
            return (
                "a short-window writer shortened live TTL and deleted settled usage still "
                "required by a coexisting long-window writer, which then admitted above its cap"
            )
        return (
            "a fast-refill writer shortened depleted bucket state from about 12 s to about "
            "1.1 s; after expiry the slow-refill writer restored and admitted full capacity early"
        )
    if algorithm == "sliding_window":
        return (
            "the frozen source admits concurrent overrun, races namespace caps, can shorten "
            "per-subject TTLs, and can lose a claimed settlement delta after GETDEL"
        )
    return (
        "the frozen source watches only the subject bucket while racing namespace caps and "
        "uses a local-writer PEXPIRE that can shorten required state"
    )


def standalone_evidence(candidate: str, product: str) -> list[str]:
    if candidate in {"current_eval", "proposed_evalsha"}:
        if product == "redis_8_10_2":
            return ["runtime/redis-8.10.2-standalone.json"]
        return ["runtime/red-hat-valkey-8.0.11-standalone.json"]
    return ["static-audit.json"]


def build_correctness_cells() -> list[dict[str, Any]]:
    cells = []
    for candidate in CANDIDATES:
        for algorithm in ALGORITHMS:
            for product in PRODUCTS:
                product_gate = f"{candidate}.{algorithm}.{product}.standalone"
                for topology in TOPOLOGIES:
                    if topology == "standalone":
                        cells.append(
                            cell(
                                candidate,
                                algorithm,
                                product,
                                topology,
                                "fail",
                                correctness_gate_reason(candidate, algorithm),
                                standalone_evidence(candidate, product),
                            )
                        )
                    else:
                        cells.append(
                            cell(
                                candidate,
                                algorithm,
                                product,
                                topology,
                                "blocked_by_correctness_gate",
                                f"the candidate/algorithm failed the named product standalone gate {product_gate}; topology execution would be non-qualifying",
                                standalone_evidence(candidate, product),
                                gate=product_gate,
                            )
                        )
    return cells


def build_performance_cells(correctness: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for source in correctness:
        result = dict(source)
        result["id"] = f"performance.{source['id']}"
        result["state"] = "blocked_by_correctness_gate"
        result["reason"] = (
            f"performance is prohibited after correctness cell {source['id']} did not pass; "
            "no dedicated-hardware claim was attempted"
        )
        result["gate"] = source["id"]
        output.append(result)
    return output


def validate(cells: list[dict[str, Any]], expected: int) -> None:
    assert len(cells) == expected, (len(cells), expected)
    ids = [entry["id"] for entry in cells]
    assert len(ids) == len(set(ids)), "duplicate matrix cell"
    for entry in cells:
        assert entry["state"] in ALLOWED_STATES
        assert entry["evidence"]
        if entry["state"] != "pass":
            assert entry["reason"]


def main() -> None:
    standalone = load("runtime/redis-8.10.2-standalone.json")
    valkey_standalone = load("runtime/red-hat-valkey-8.0.11-standalone.json")
    cluster_negative = load("runtime/redis-8.10.2-cluster-negative.json")
    valkey_cluster_negative = load("runtime/red-hat-valkey-8.0.11-cluster-negative.json")
    static = load("static-audit.json")
    assert standalone["server_identity"]["redis_version"] == "8.10.2"
    assert standalone["eval_evalsha_semantics_equal"]
    assert standalone["eval_evalsha_invariant_outcomes_equal"]
    assert valkey_standalone["server_identity"]["valkey_version"] == "8.0.11"
    assert valkey_standalone["eval_evalsha_semantics_equal"]
    assert valkey_standalone["eval_evalsha_invariant_outcomes_equal"]
    assert cluster_negative["passed"]
    assert valkey_cluster_negative["passed"]
    assert not static["native_cluster_contract"]["unchanged_contract_satisfiable"]
    for product_report in [standalone, valkey_standalone]:
        for candidate in ["eval", "eval_sha"]:
            sliding = invariant(
                product_report,
                candidate,
                "sliding_window.mixed_configuration_preserves_long_window_history",
            )
            bucket = invariant(
                product_report,
                candidate,
                "token_bucket.mixed_configuration_preserves_slow_refill_state",
            )
            assert not sliding["passed"]
            assert sliding["observed"]["settled_entries_before_short_writer"] == 1
            assert sliding["observed"]["settled_entries_after_short_writer"] == 0
            assert sliding["observed"]["long_writer_reply_after_short_writer"][0] == 1
            assert not bucket["passed"]
            assert not bucket["observed"]["state_exists_after_1200ms"]
            assert bucket["observed"]["slow_writer_reply_after_1200ms"][0] == 1

    correctness = build_correctness_cells()
    performance = build_performance_cells(correctness)
    validate(correctness, len(CANDIDATES) * len(ALGORITHMS) * len(PRODUCTS) * len(TOPOLOGIES))
    validate(performance, len(correctness))

    matrix = {
        "schema_version": 1,
        "status": "complete_no_go_for_tested_candidates_and_exact_product_digests",
        "allowed_result_states": sorted(ALLOWED_STATES),
        "candidate_descriptions": CANDIDATES,
        "product_descriptions": PRODUCTS,
        "correctness_and_topology_cells": correctness,
        "performance_cells": performance,
        "diagnostic_cells": [
            {
                "id": "redis_8_10_2.cluster.focused_negative_controls",
                "state": "pass",
                "meaning": "all required negative controls produced the expected rejection or client-decomposition evidence; this is not positive Cluster qualification",
                "evidence": ["runtime/redis-8.10.2-cluster-negative.json"],
            },
            {
                "id": "red_hat_valkey_8_0_11.cluster.focused_negative_controls",
                "state": "pass",
                "meaning": "all required negative controls produced the expected rejection or client-decomposition evidence; this is not positive Cluster qualification",
                "evidence": ["runtime/red-hat-valkey-8.0.11-cluster-negative.json"],
            },
            {
                "id": "native_cluster.unchanged_aggregate_contract",
                "state": "unsupported",
                "reason": static["native_cluster_contract"]["conclusion"],
                "evidence": ["static-audit.json"],
            },
        ],
        "decisions": {
            "scripted_sliding_window": "no_go_current_eval_and_evalsha",
            "scripted_token_bucket": "no_go_current_eval_and_evalsha",
            "plain_sliding_window": "no_go_frozen_pr_1380",
            "plain_token_bucket": "no_go_frozen_pr_1380",
            "standalone_equal_support_under_corrected_registry_and_exact_digests": "no_go",
            "sentinel_equal_support_under_exact_digests": "blocked_by_correctness_gate",
            "redis_native_cluster_unchanged_contract": "no_go",
            "valkey_native_cluster_unchanged_contract": "no_go",
            "performance_selection": "not_run_no_conformant_candidate",
            "selected_candidate": None,
        },
    }
    (EVIDENCE / "result-matrix.json").write_text(
        json.dumps(matrix, indent=2, sort_keys=True) + "\n"
    )

    contract = {
        "schema_version": 1,
        "source_revisions": {
            "frozen_upstream": "56af0e37e4bda2f06c7e33546db66a8d01fce0b7",
            "plain_pr_1380": "3ad8d12d6ad7014f7f632ba3468f74f8de13ce77",
        },
        "client": {
            "crate": "redis",
            "version": "1.7.0",
            "checksum": "2acbc41a996f7652b2ddd9dfd98cc4ff602cfd742ae35382f07f608405ab50ed",
            "production_features": ["tokio-comp"],
            "spike_features": ["tokio-comp", "sentinel", "cluster-async", "script"],
            "protocol": "RESP2",
            "connect_timeout_ms": 500,
            "per_response_timeout_ms": 500,
            "outer_operation_deadline_ms": 1000,
        },
        "outcomes": {
            "reserve_admitted": [
                "tag=1",
                "reservation_id",
                "estimate",
                "usage_after",
                "remaining",
                "rule_active_reservations",
                "rule_retained_keys",
            ],
            "reserve_denied": [
                "tag=0",
                "retry_after_ms",
                "remaining",
                "rule_active_reservations",
                "rule_retained_keys",
            ],
            "reconcile_applied": [
                "tag=1",
                "actual",
                "refund",
                "overage",
                "remaining",
                "rule_active_reservations",
                "rule_retained_keys",
            ],
            "reconcile_noop": [
                "tag=0",
                "remaining",
                "rule_active_reservations",
                "rule_retained_keys",
            ],
        },
        "candidate_commands": {
            "current_eval": {
                "outer": ["EVAL"],
                "reserve_round_trips": 1,
                "reconcile_round_trips": 1,
                "lua_visible": static["lua_commands"],
                "clock_authority": "server TIME",
                "atomicity": "one server-side script in standalone; rejected when declared keys cross Cluster slots",
            },
            "proposed_evalsha": {
                "warm_outer": ["EVALSHA"],
                "cold_outer": ["EVALSHA (NOSCRIPT)", "SCRIPT LOAD", "EVALSHA"],
                "lua_visible": static["lua_commands"],
                "clock_authority": "server TIME",
                "observed_cluster_script_load_scope": "all three primaries",
            },
            "plain_pr_1380": {
                "commands": static["plain_commands"],
                "pexpire_options": ["NX", "GT"],
                "sliding_reserve_round_trips": 3,
                "sliding_reconcile_round_trips": 2,
                "token_bucket_round_trips_per_attempt": 2,
                "clock_authority": "caller-provided now_ms",
                "transaction_boundaries": ["MULTI/EXEC", "WATCH/UNWATCH"],
            },
        },
        "mutation_classification": {
            "known_before_dispatch": "known_not_applied; closed mode 503; prospective open mode bypassed",
            "confirmed_denial": "not applied; 429 only for quota exhaustion",
            "confirmed_noscript": "not applied; targeted script load/fallback may retry inside the same outer deadline",
            "timeout_disconnect_or_lost_reply_after_dispatch": "ambiguous; reservation must not be replayed",
            "reconcile": "retry only if the complete candidate operation is atomic/idempotent or proven not applied",
        },
        "stable_error_evidence": {
            "cross_slot": {
                "redis_rs_kind": "Server(CrossSlot)",
                "server_code": "CROSSSLOT",
                "match_complete_text": False,
            },
            "dynamic_non_local_script_key": {
                "redis_rs_kind": "Server(ResponseError)",
                "server_code": "ERR",
                "match_complete_text": False,
                "note": "generic ERR is insufficient as a portable compatibility interface; retain raw detail only as evidence",
            },
        },
        "observed_cap_scope": {
            "frozen_lua": "namespace-wide per algorithm for enforcement; separate per-rule telemetry",
            "intended_scope": "unresolved; Rust comments say per rule",
        },
        "numeric": {
            "redis_integer": "signed 64-bit",
            "maximum_reported_remaining": 9007199254740991,
            "token_bucket_storage": "server floating point serialized through hash fields",
        },
        "key_and_cluster": {
            "layouts": static["key_layouts"],
            "dynamic_lua_key_access": static["lua_dynamic_key_access"],
            "unchanged_contract": static["native_cluster_contract"],
            "client_decomposition": {
                "redis_8_10_2": cluster_negative["decomposition_cases"],
                "red_hat_valkey_8_0_11": valkey_cluster_negative[
                    "decomposition_cases"
                ],
            },
        },
        "qualification_blockers": {
            "runtime": {
                "redis_8_10_2": standalone["failures"],
                "red_hat_valkey_8_0_11": valkey_standalone["failures"],
            },
            "plain_source": static["plain_source_findings"],
        },
    }
    (EVIDENCE / "contract.json").write_text(
        json.dumps(contract, indent=2, sort_keys=True) + "\n"
    )

    ledger = {
        "schema_version": 1,
        "claims": [
            {
                "id": "source.freeze",
                "claim": "The harness inputs are frozen upstream and PR revisions with per-file SHA-256 hashes.",
                "evidence": ["static-audit.json", "../fixtures/source/manifest.json"],
            },
            {
                "id": "products.mixed_ttl",
                "claim": "Both frozen Lua algorithms silently under-enforce under mixed writer configurations on Redis 8.10.2 and Red Hat Valkey 8.0.11: sliding history is deleted early and a depleted token bucket regains full capacity early after shortened expiry.",
                "evidence": [
                    "runtime/redis-8.10.2-standalone.json",
                    "runtime/red-hat-valkey-8.0.11-standalone.json",
                ],
            },
            {
                "id": "eval.evalsha.equivalence",
                "claim": "EVAL and EVALSHA have equal normalized lifecycle semantics and equal invariant outcomes in the executed gate.",
                "evidence": [
                    "runtime/redis-8.10.2-standalone.json",
                    "runtime/red-hat-valkey-8.0.11-standalone.json",
                ],
            },
            {
                "id": "plain.nonconformant",
                "claim": "The frozen plain-command revision contains source-proven settlement, concurrency, cap, and TTL blockers.",
                "evidence": ["static-audit.json"],
            },
            {
                "id": "cluster.negative_controls",
                "claim": "Frozen layouts reject atomically or are decomposed by redis-rs; dynamic cleanup can access a key owned by another primary.",
                "evidence": [
                    "runtime/redis-8.10.2-cluster-negative.json",
                    "runtime/red-hat-valkey-8.0.11-cluster-negative.json",
                    "static-audit.json",
                ],
            },
            {
                "id": "cluster.aggregate_impossibility",
                "claim": "Exact shared aggregate semantics and cross-primary subject distribution are mutually incompatible without a contract change or coordination layer.",
                "evidence": ["static-audit.json"],
            },
            {
                "id": "valkey.registry.resolved",
                "claim": "The terms-gated Red Hat Valkey repository is served from authenticated registry.redhat.io rather than registry.access.redhat.com; the exact requested manifest digest was preserved and executed.",
                "evidence": [
                    "environment.json",
                    "runtime/red-hat-valkey-8.0.11-standalone.json",
                    "runtime/valkey-preflight-declared-registry.txt",
                    "runtime/valkey-preflight-default-auth.txt",
                ],
            },
            {
                "id": "draft.source_audit",
                "claim": "Pre-execution source and requirements claims remain traced in the local audited proposal.",
                "evidence": ["../../redis-valkey-compat-performance-spike-audit.md"],
            },
        ],
    }
    (EVIDENCE / "source-ledger.json").write_text(
        json.dumps(ledger, indent=2, sort_keys=True) + "\n"
    )

    redis_rs_report = """# `redis-rs` 1.7.0 production-suitability result

## Decision by topology

| Topology | Result | Basis |
|---|---|---|
| Standalone | No go for the tested candidates | The accounting correctness gate failed identically on Redis 8.10.2 and Red Hat Valkey 8.0.11 before transport-fault qualification. The direct multiplexed path used finite 500 ms connect/response limits and a 1 s outer harness deadline, but ambiguous-write cases were intentionally stopped. |
| Sentinel | Not qualified | No candidate cleared standalone. Source inspection also shows ordered discovery starting at the first Sentinel and default internal discovery/role-check timeouts; no runtime failover evidence was allowed past the gate. |
| Native Cluster | No go unchanged | The contract is slot-unsatisfiable, current layouts fail, and runtime confirms cross-slot `MGET` and `WATCH` decomposition plus all-primary `SCRIPT LOAD`. The central async request channel is unbounded and generic retry handling is not mutation-semantic-aware. |

## Runtime observations

- Cluster bootstrap issued `CLUSTER SLOTS`; each node connection issued `READONLY`, while the configured read-routing policy remained the primary-only default.
- `redis::Script` cold recovery loaded the script on all three primaries before retrying the target operation.
- Cross-slot `MGET` returned a combined ordered reply, with one `MGET` observed on each owning primary.
- Cross-slot multi-key `WATCH` returned `OK`, with `WATCH` observed on both owning primaries; this is not one node-local optimistic transaction watch set.
- Atomic cross-slot pipelines and the frozen scripts returned structured `Server(CrossSlot)` / `CROSSSLOT` errors on both products.

## Source-qualified blockers retained from the audited plan

- Async Cluster caller ingress uses an unbounded channel; per-node limits are acquired later and do not bound caller backlog.
- Default Cluster retries are not aware of Praxis mutation semantics. A dropped caller suppresses later work only after the worker observes closure and cannot cancel an already dispatched command.
- Sentinel traverses endpoints in fixed order and uses default async settings for internal discovery while caller settings apply to the final data connection.
- A qualifying future adapter therefore needs an outer admission bound, one whole-operation deadline, explicit connection/retry settings, primary-only routing assertions, and deterministic ambiguous-write traces. If supported APIs cannot separate safe redirect/NOSCRIPT recovery from ambiguous mutation replay, the result must remain a wrapper/upstream requirement or no-go—not a silent local fork.

## Selection consequence

No `redis-rs` topology receives a production go from this run. This is not a claim that 1.7.0 cannot ever be used; it is a claim that the tested candidates and currently evidenced adapter behavior do not satisfy the mandatory objectives.

Evidence: `runtime/redis-8.10.2-standalone.json`, `runtime/red-hat-valkey-8.0.11-standalone.json`, both `*-cluster-negative.json` files, `static-audit.json`, and `../../redis-valkey-compat-performance-spike-audit.md`.
"""
    (EVIDENCE / "redis-rs-production-suitability.md").write_text(redis_rs_report)

    triage = """# Mixed-configuration accounting failure triage

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
"""
    (EVIDENCE / "failure-triage.md").write_text(triage)

    report = f"""# Local Redis/Valkey datastore qualification spike — gated result

## Outcome

No tested accounting candidate qualifies unchanged, so Sentinel, positive Cluster, fault/recovery, and performance execution are stopped by the correctness gate. Nothing in this result selects `EVAL`, `EVALSHA`, or PR #1380.

## Gate-breaking evidence

- **Frozen Lua, sliding window:** on both products, a short-window writer shortened live TTL and deleted settled history (`ZCARD 1 → 0`) still required by a long-window writer; that long-window writer then admitted above its cap.
- **Frozen Lua, token bucket:** on both products, a fast-refill writer shortened depleted state from approximately 12,000 ms to 1,100 ms; after 1.2 seconds the slow-refill writer restored and admitted all 10 tokens rather than having only ~1.2 available.
- **EVAL versus EVALSHA:** lifecycle semantics and invariant outcomes were identical. `redis::Script` cold use issued failed `EVALSHA`, `SCRIPT LOAD`, then successful `EVALSHA`; changing invocation mode did not repair either TTL violation.
- **Frozen PR #1380 sliding window:** immutable source explicitly permits concurrent over-admission and acknowledges that `GETDEL` followed by a failed settlement can lose an overage. Namespace caps and per-subject TTLs also race/shorten.
- **Frozen PR #1380 token bucket:** `WATCH` covers the subject bucket but not shared cap indexes, permitting distinct-key cap races; per-subject TTLs can also be shortened by mixed writers.

## Native Cluster result

The unchanged aggregate contract is unsatisfiable with cross-primary subject distribution. Exact shared caps/IDs/telemetry must be atomic with each subject state, which forces one slot; splitting them changes semantics, and coordinating them adds the forbidden Praxis sharding layer.

The Redis and Red Hat Valkey three-primary diagnostics both confirmed:

- frozen `EVAL` and `EVALSHA` layouts fail with structured `CROSSSLOT` for both algorithms;
- PR #1380's sliding transaction is cross-slot;
- a hash-tag-only scripted redesign still fails when cleanup follows a dynamic index entry to a key owned by another primary;
- `redis-rs` 1.7.0 transparently decomposes cross-slot `MGET` and `WATCH`;
- standard `redis::Script` cold loading fans `SCRIPT LOAD` to all three primaries.

These are negative-control/client observations, not positive Cluster qualification.

## Product-pin result

Redis ran from the exact 8.10.2 manifest digest on Linux/arm64. Red Hat Valkey ran from the exact requested 8.0.11 manifest digest after correcting only the registry endpoint from `registry.access.redhat.com` to the canonical authenticated `registry.redhat.io` repository. No product, version, architecture, or digest was substituted. Both products independently fail the same standalone correctness invariant.

## Decisions

| Decision | Result |
|---|---|
| Current `EVAL`, per algorithm | No-go |
| Proposed `EVALSHA`, per algorithm | No-go; equivalent logic failure |
| Frozen PR #1380 plain design, per algorithm | No-go |
| Standalone equal support under exact digests | No-go; symmetric failure reproduced |
| Sentinel equal support | Blocked by correctness gate |
| Redis native-Cluster unchanged contract | No-go |
| Valkey native-Cluster unchanged contract | No-go |
| Performance comparison | Not run; no conformant lane |
| Selected candidate | None |

## Required next contract revision

1. Reject mixed semantic configurations atomically with a server-side rule fingerprint before mutation; monotonic TTL extension is required defense in depth but is not sufficient by itself.
2. Replace PR #1380's split sliding settlement and racy cap enforcement before reconsidering plain commands.
3. Decide explicitly whether aggregate caps, reservation IDs, cleanup, and telemetry may change scope for Cluster. Without that approved contract change, sharding remains a no-go.
4. Normalize the Red Hat image reference to `registry.redhat.io` while retaining the exact manifest digest and authenticated pull requirement.

## Evidence

- `runtime/redis-8.10.2-standalone.json`
- `runtime/redis-8.10.2-cluster-negative.json`
- `runtime/red-hat-valkey-8.0.11-standalone.json`
- `runtime/red-hat-valkey-8.0.11-cluster-negative.json`
- `static-audit.json`
- `result-matrix.json`
- `contract.json`
- `source-ledger.json`
- `redis-rs-production-suitability.md`
- `failure-triage.md`
- `environment.json`
- `runtime/valkey-preflight-declared-registry.txt`
- `runtime/valkey-preflight-default-auth.txt`

All work remains local and unpublished. No dedicated-hardware performance claim was made.
"""
    (EVIDENCE / "report.md").write_text(report)
    print(EVIDENCE / "result-matrix.json")
    print(EVIDENCE / "report.md")


if __name__ == "__main__":
    import runpy

    runpy.run_path(Path(__file__).with_name("build_repaired_report.py"), run_name="__main__")
