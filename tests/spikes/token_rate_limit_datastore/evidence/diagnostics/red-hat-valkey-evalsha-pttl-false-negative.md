# Red Hat Valkey Sentinel `EVALSHA` harness false negative

## Observation

The first authenticated fresh run on 2026-09-30 reported one failed invariant in
the Red Hat Valkey 8.0.11 Sentinel `EVALSHA` lane:
`sliding_window.fingerprint.budget_set.reverse`.

The preserved report is
`red-hat-valkey-8.0.11-sentinel-evalsha-pttl-false-negative.json`.

## Classification

This was a qualification-harness timing defect, not a Redis/Valkey behavior
difference or an accounting mutation:

- reserve and reconcile both returned the configuration-mismatch result `[3]`;
- every before/after `DUMP` payload and digest was identical;
- the owning writer still reconciled exactly once, followed by a duplicate no-op;
- only `PTTL` values differed, decreasing naturally while the snapshots were
  collected.

The comparison bounded TTL decay using a timer started after the complete
"before" snapshot. Time spent collecting the tail of that snapshot was therefore
excluded even though it contributed to decay for keys sampled near its start.

## Correction and validation

The harness now records `PEXPIRETIME` and compares the absolute expiry timestamp,
while retaining `PTTL` as human-readable evidence. This permits natural TTL decay
but detects any expiry reset exactly. A unit regression covers both cases.

The complete authenticated matrix then passed from scratch for exact Redis
8.10.2 and Red Hat Valkey 8.0.11 images: standalone, Sentinel `EVAL`, and Sentinel
`EVALSHA`. All generated decision artifacts also regenerate byte-identically.
