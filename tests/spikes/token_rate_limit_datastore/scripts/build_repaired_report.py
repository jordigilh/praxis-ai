#!/usr/bin/env python3
"""Build the combined historical and repaired datastore qualification report."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "evidence"
RUNTIME = EVIDENCE / "runtime"
REPAIR_COMMIT = "b6fe4704af72c05167a280d5070a291997d2085d"
ALLOWED_STATES = {
    "pass",
    "fail",
    "blocked_by_correctness_gate",
    "unsupported",
    "not_applicable",
}
ALGORITHMS = ["sliding_window", "token_bucket"]
PRODUCTS = {
    "redis_8_10_2": {
        "name": "Redis 8.10.2",
        "prefix": "redis-8.10.2",
        "version_field": "redis_version",
        "version": "8.10.2",
    },
    "red_hat_valkey_8_0_11": {
        "name": "Red Hat Valkey 8.0.11",
        "prefix": "red-hat-valkey-8.0.11",
        "version_field": "valkey_version",
        "version": "8.0.11",
    },
}
TOPOLOGIES = ["standalone", "sentinel_ha", "cluster_sharding", "cluster_sharding_ha"]
HISTORICAL_CANDIDATES = {
    "current_eval": "Frozen upstream Lua invoked with EVAL",
    "proposed_evalsha": "Identical frozen Lua invoked through redis::Script/EVALSHA",
    "plain_pr_1380": "Frozen PR #1380 plain-command design",
}
REPAIRED_CANDIDATES = {
    "mixed_configuration_repair_eval": {
        "description": "Mixed-configuration repair at b6fe4704 invoked with EVAL",
        "invocation": "eval",
        "file_invocation": "eval",
    },
    "mixed_configuration_repair_evalsha": {
        "description": "Mixed-configuration repair at b6fe4704 invoked with redis::Script/EVALSHA",
        "invocation": "eval_sha",
        "file_invocation": "evalsha",
    },
}


def load(path: Path) -> Any:
    return json.loads(path.read_text())


def relative(path: Path) -> str:
    return str(path.relative_to(EVIDENCE))


def cell(
    candidate: str,
    algorithm: str,
    product: str,
    topology: str,
    state: str,
    evidence: list[str],
    reason: str | None = None,
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


def historical_reason(candidate: str, algorithm: str) -> str:
    if candidate in {"current_eval", "proposed_evalsha"}:
        if algorithm == "sliding_window":
            return (
                "the frozen implementation let a short-window writer delete history still "
                "required by a long-window writer and then over-admit"
            )
        return (
            "the frozen implementation let a fast-refill writer shorten depleted state and "
            "restore a slow-refill bucket to full capacity early"
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


def historical_evidence(candidate: str, product: str) -> list[str]:
    if candidate == "plain_pr_1380":
        return ["static-audit.json"]
    return [f"runtime/{PRODUCTS[product]['prefix']}-standalone.json"]


def build_historical_cells() -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []
    for candidate in HISTORICAL_CANDIDATES:
        for algorithm in ALGORITHMS:
            for product in PRODUCTS:
                gate = f"{candidate}.{algorithm}.{product}.standalone"
                evidence = historical_evidence(candidate, product)
                cells.append(
                    cell(
                        candidate,
                        algorithm,
                        product,
                        "standalone",
                        "fail",
                        evidence,
                        historical_reason(candidate, algorithm),
                    )
                )
                for topology in TOPOLOGIES[1:]:
                    cells.append(
                        cell(
                            candidate,
                            algorithm,
                            product,
                            topology,
                            "blocked_by_correctness_gate",
                            evidence,
                            f"the frozen candidate failed {gate}; later topology execution was non-qualifying",
                            gate,
                        )
                    )
    return cells


def repaired_paths(product: str, candidate: str) -> tuple[Path, Path]:
    prefix = PRODUCTS[product]["prefix"]
    invocation = REPAIRED_CANDIDATES[candidate]["file_invocation"]
    return (
        RUNTIME / f"{prefix}-fixed-standalone.json",
        RUNTIME / f"{prefix}-sentinel-{invocation}.json",
    )


def validate_repaired_evidence() -> dict[str, dict[str, dict[str, Any]]]:
    reports: dict[str, dict[str, dict[str, Any]]] = {}
    for product, profile in PRODUCTS.items():
        reports[product] = {}
        fixed_path = RUNTIME / f"{profile['prefix']}-fixed-standalone.json"
        fixed = load(fixed_path)
        assert fixed["candidate"] == "mixed_configuration_repair"
        assert fixed["source_commit"] == REPAIR_COMMIT
        assert fixed["server_identity"][profile["version_field"]] == profile["version"]
        assert fixed["failures"] == []
        assert fixed["eval_evalsha_semantics_equal"]
        assert fixed["eval_evalsha_invariant_outcomes_equal"]
        invocation_reports = {entry["invocation"]: entry for entry in fixed["invocations"]}
        assert set(invocation_reports) == {"eval", "eval_sha"}
        for invocation in invocation_reports.values():
            assert invocation["passed"]
            assert all(event["passed"] for event in invocation["events"])
            assert all(invariant["passed"] for invariant in invocation["invariants"])
        reports[product]["standalone"] = fixed

        for candidate, candidate_profile in REPAIRED_CANDIDATES.items():
            _, sentinel_path = repaired_paths(product, candidate)
            sentinel = load(sentinel_path)
            assert sentinel["candidate"] == "mixed_configuration_repair"
            assert sentinel["source_commit"] == REPAIR_COMMIT
            assert sentinel["invocation"] == candidate_profile["invocation"]
            assert sentinel["failures"] == []
            assert sentinel["standalone_contract_over_sentinel"]["passed"]
            assert sentinel["topology"]["sentinel_count"] == 3
            assert sentinel["topology"]["data_node_count"] == 2
            assert sentinel["topology"]["quorum"] == 2
            assert sentinel["topology"]["initial_primary"]["connected_replicas"] == 1
            assert (
                sentinel["topology"]["initial_replica"]["replica_repl_offset"]
                >= sentinel["topology"]["initial_primary"]["master_repl_offset"]
            )
            assert all(scenario["passed"] for scenario in sentinel["scenarios"])
            assert all(failover["passed"] for failover in sentinel["failovers"])
            reports[product][candidate] = sentinel
    return reports


def build_repaired_cells() -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []
    cluster_reason = (
        "the unchanged exact aggregate caps, reservation IDs, cleanup, and telemetry contract "
        "cannot both share one atomic slot and distribute independent subjects across native Cluster primaries"
    )
    for candidate in REPAIRED_CANDIDATES:
        for algorithm in ALGORITHMS:
            for product in PRODUCTS:
                standalone_path, sentinel_path = repaired_paths(product, candidate)
                cells.append(
                    cell(
                        candidate,
                        algorithm,
                        product,
                        "standalone",
                        "pass",
                        [relative(standalone_path)],
                    )
                )
                cells.append(
                    cell(
                        candidate,
                        algorithm,
                        product,
                        "sentinel_ha",
                        "pass",
                        [relative(sentinel_path)],
                    )
                )
                for topology in TOPOLOGIES[2:]:
                    cells.append(
                        cell(
                            candidate,
                            algorithm,
                            product,
                            topology,
                            "unsupported",
                            ["static-audit.json", "contract.json"],
                            cluster_reason,
                        )
                    )
    return cells


def build_performance_cells(
    historical: list[dict[str, Any]], repaired: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for source in historical:
        result = dict(source)
        result["id"] = f"performance.{source['id']}"
        result["state"] = "blocked_by_correctness_gate"
        result["reason"] = f"performance was prohibited after correctness cell {source['id']} did not pass"
        result["gate"] = source["id"]
        output.append(result)
    for source in repaired:
        result = dict(source)
        result["id"] = f"performance.{source['id']}"
        if source["state"] != "pass":
            result["state"] = "blocked_by_correctness_gate"
            result["reason"] = f"performance is inapplicable because topology cell {source['id']} is unsupported"
            result["gate"] = source["id"]
        else:
            result["state"] = "not_applicable"
            result["reason"] = (
                "no numeric latency/throughput qualification is claimed from this local functional host; "
                "#1421's release criterion is the separately recorded steady-state round-trip parity gate"
            )
            result["evidence"] = ["environment.json", *source["evidence"]]
            result["gate"] = None
        output.append(result)
    return output


def build_architectural_performance_cells() -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []
    for candidate in REPAIRED_CANDIDATES:
        for product in PRODUCTS:
            standalone_path, sentinel_path = repaired_paths(product, candidate)
            cells.append(
                {
                    "id": f"round_trip_parity.{candidate}.{product}",
                    "candidate": candidate,
                    "product": product,
                    "state": "pass",
                    "assertion": (
                        "standalone and cached Sentinel-primary steady state issue the same one scripted "
                        "mutation round trip; Sentinel discovery occurs only on initial connection or invalidation"
                    ),
                    "evidence": [relative(standalone_path), relative(sentinel_path)],
                }
            )
    return cells


def validate_cells(cells: list[dict[str, Any]]) -> None:
    ids = [entry["id"] for entry in cells]
    assert len(ids) == len(set(ids)), "duplicate matrix cell"
    for entry in cells:
        assert entry["state"] in ALLOWED_STATES
        assert entry["evidence"]
        assert entry["state"] == "pass" or entry.get("reason")


def repaired_failover_rows(reports: dict[str, dict[str, dict[str, Any]]]) -> list[str]:
    rows: list[str] = []
    for product, profile in PRODUCTS.items():
        for candidate, candidate_profile in REPAIRED_CANDIDATES.items():
            report = reports[product][candidate]
            failovers = {entry["kind"]: entry for entry in report["failovers"]}
            planned = failovers["planned_sentinel_failover"]
            unplanned = failovers["unplanned_primary_shutdown"]
            rows.append(
                "| "
                f"{profile['name']} | `{candidate_profile['invocation']}` | "
                f"{planned['elapsed_ms']} ms | {unplanned['elapsed_ms']} ms | "
                f"{unplanned['rediscovery_elapsed_ms']} ms / {unplanned['rediscovery_attempts']} |"
            )
    return rows


def update_contract() -> None:
    current = load(EVIDENCE / "contract.json")
    historical = current.get("historical_frozen_contract", current)
    contract = {
        "schema_version": 2,
        "source_revisions": {
            "frozen_upstream": "56af0e37e4bda2f06c7e33546db66a8d01fce0b7",
            "plain_pr_1380": "3ad8d12d6ad7014f7f632ba3468f74f8de13ce77",
            "mixed_configuration_repair": REPAIR_COMMIT,
        },
        "client": {
            "crate": "redis",
            "version": "1.7.0",
            "checksum": "2acbc41a996f7652b2ddd9dfd98cc4ff602cfd742ae35382f07f608405ab50ed",
            "repair_features": ["tokio-comp", "sentinel", "script"],
            "protocol": "RESP2",
            "connect_timeout_ms": 500,
            "response_timeout_ms": 500,
            "sentinel_discovery_deadline_ms": 3000,
            "automatic_mutation_retries": 0,
        },
        "repaired_accounting_identity": {
            "schema": "v1",
            "scope": "namespace/rule/algorithm",
            "sliding_window_fields": [
                "canonical (window_ms, capacity) budget set",
                "reservation_timeout_ms",
                "max_keys",
                "max_active_reservations",
            ],
            "token_bucket_fields": [
                "capacity",
                "exact IEEE-754 refill_rate bits",
                "reservation_timeout_ms",
                "max_keys",
                "max_active_reservations",
            ],
            "excluded": ["per-request estimate"],
            "mismatch_reply": [3],
            "mismatch_semantics": "reject before shared-state mutation; Praxis maps to fail-closed 503",
            "intentional_change": "use an explicit namespace/state-generation transition",
        },
        "sentinel_adapter": {
            "topology": "one primary, one caught-up replica, three Sentinels, quorum two",
            "steady_state": "cached multiplexed writable-primary connection; no Sentinel command or extra round trip",
            "discovery_retries": "read-only traversal and pre-dispatch connection establishment only, within one deadline",
            "mutation_retry": "never replay timeout, disconnect, or other ambiguous dispatched mutation",
            "replication": "asynchronous; no production WAIT or WAITAOF and no RPO=0 claim",
            "primary_only": True,
        },
        "topology_decisions": {
            "standalone": "go for both exact products under repaired EVAL and EVALSHA",
            "sentinel_ha": "go for both exact products under repaired EVAL and EVALSHA",
            "native_cluster": "no-go for the unchanged exact aggregate contract",
        },
        "historical_frozen_contract": historical,
    }
    (EVIDENCE / "contract.json").write_text(json.dumps(contract, indent=2, sort_keys=True) + "\n")


def update_source_ledger() -> None:
    path = EVIDENCE / "source-ledger.json"
    ledger = load(path)
    claims = {entry["id"]: entry for entry in ledger["claims"]}
    claims.update(
        {
            "repair.standalone": {
                "id": "repair.standalone",
                "claim": (
                    "The mixed-configuration repair passes the same standalone semantic and mismatch-before-mutation "
                    "suite on both exact products under EVAL and EVALSHA."
                ),
                "evidence": [
                    "runtime/redis-8.10.2-fixed-standalone.json",
                    "runtime/red-hat-valkey-8.0.11-fixed-standalone.json",
                ],
            },
            "repair.sentinel": {
                "id": "repair.sentinel",
                "claim": (
                    "The repair passes cached-primary, unavailable-Sentinel, replication-health, ambiguous-timeout, "
                    "planned failover, unplanned shutdown, cold-script, and post-failover enforcement scenarios on both products."
                ),
                "evidence": [
                    "runtime/redis-8.10.2-sentinel-eval.json",
                    "runtime/redis-8.10.2-sentinel-evalsha.json",
                    "runtime/red-hat-valkey-8.0.11-sentinel-eval.json",
                    "runtime/red-hat-valkey-8.0.11-sentinel-evalsha.json",
                ],
            },
            "repair.performance": {
                "id": "repair.performance",
                "claim": (
                    "Cached Sentinel steady state has command/round-trip parity with standalone. No numeric "
                    "latency or throughput claim is made from the local functional host."
                ),
                "evidence": ["environment.json", "result-matrix.json"],
            },
        }
    )
    ledger["schema_version"] = 2
    ledger["claims"] = list(claims.values())
    path.write_text(json.dumps(ledger, indent=2, sort_keys=True) + "\n")


def write_redis_rs_report() -> None:
    text = """# `redis-rs` 1.7.0 production-suitability result

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
"""
    (EVIDENCE / "redis-rs-production-suitability.md").write_text(text)


def write_report(reports: dict[str, dict[str, dict[str, Any]]]) -> None:
    rows = "\n".join(repaired_failover_rows(reports))
    report = f"""# Local Redis/Valkey datastore qualification — repaired result

## Outcome

The mixed-configuration repair at `{REPAIR_COMMIT}` qualifies standalone and Sentinel HA on both exact products under `EVAL` and `EVALSHA`. Frozen upstream, proposed-unrepaired `EVALSHA`, and PR #1380 remain historical no-go inputs; they were not overwritten. Native Cluster remains a no-go for the unchanged exact aggregate contract.

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
{rows}

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
"""
    (EVIDENCE / "report.md").write_text(report)


def main() -> None:
    reports = validate_repaired_evidence()
    historical = build_historical_cells()
    repaired = build_repaired_cells()
    correctness = [*historical, *repaired]
    performance = build_performance_cells(historical, repaired)
    validate_cells(correctness)
    validate_cells(performance)

    matrix = {
        "schema_version": 2,
        "status": "standalone_and_sentinel_correctness_qualified_performance_numeric_claim_deferred",
        "allowed_result_states": sorted(ALLOWED_STATES),
        "candidate_descriptions": {
            **HISTORICAL_CANDIDATES,
            **{key: value["description"] for key, value in REPAIRED_CANDIDATES.items()},
        },
        "product_descriptions": {key: value["name"] for key, value in PRODUCTS.items()},
        "correctness_and_topology_cells": correctness,
        "performance_cells": performance,
        "architectural_performance_cells": build_architectural_performance_cells(),
        "diagnostic_cells": [
            {
                "id": "native_cluster.unchanged_aggregate_contract",
                "state": "unsupported",
                "reason": load(EVIDENCE / "static-audit.json")["native_cluster_contract"]["conclusion"],
                "evidence": ["static-audit.json"],
            }
        ],
        "decisions": {
            "mixed_configuration_repair_eval": "go_standalone_and_sentinel_both_products",
            "mixed_configuration_repair_evalsha": "correctness_go_standalone_and_sentinel_both_products",
            "production_invocation": "eval",
            "historical_current_eval": "no_go_frozen_revision",
            "historical_proposed_evalsha": "no_go_frozen_revision",
            "historical_plain_pr_1380": "no_go_frozen_revision",
            "standalone_equal_support_under_exact_digests": "go",
            "sentinel_equal_support_under_exact_digests": "go",
            "redis_native_cluster_unchanged_contract": "no_go",
            "valkey_native_cluster_unchanged_contract": "no_go",
            "steady_state_round_trip_parity": "pass",
            "numeric_dedicated_hardware_performance": "not_claimed",
        },
    }
    (EVIDENCE / "result-matrix.json").write_text(json.dumps(matrix, indent=2, sort_keys=True) + "\n")
    update_contract()
    update_source_ledger()
    write_redis_rs_report()
    write_report(reports)
    print(EVIDENCE / "result-matrix.json")
    print(EVIDENCE / "report.md")


if __name__ == "__main__":
    main()
