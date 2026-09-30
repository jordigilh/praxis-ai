# Local Redis/Valkey datastore qualification spike

Local-only evidence harness for the Praxis `token_rate_limit` datastore contract.

This directory is intentionally outside the Praxis checkout. It must not be published,
committed, or treated as production code without explicit approval.

## Immutable inputs

- Praxis baseline: `56af0e37e4bda2f06c7e33546db66a8d01fce0b7`
- Mixed-configuration repair: `b6fe4704af72c05167a280d5070a291997d2085d`
- Plain-command candidate: `3ad8d12d6ad7014f7f632ba3468f74f8de13ce77`
- `redis-rs`: exactly `1.7.0`
- Redis image: `redis:8.10.2-alpine@sha256:3811787313eba226a2ef38658c6ccb91cd5e110edc89c37767de373120a0e5a0`
- Red Hat Valkey image: `registry.redhat.io/rhel10/valkey-8@sha256:5929be16ac020c4851d8dc1f5ead6d48c6da066a9a74349711186925ebd553b9`

Run `python3 scripts/fetch_sources.py` before compiling. Generated evidence belongs
under `evidence/`; source snapshots and their hashes belong under `fixtures/source/`.

## Focused mixed-configuration bug reproduction

This starts one isolated exact-pinned Redis container, reproduces both functional
fail-open consequences under `EVAL` and `EVALSHA`, asserts the expected observations,
and removes the container on exit:

```sh
./scripts/reproduce_mixed_config_bug.sh
```

Success means the script prints `CONFIRMED`; the harness command inside intentionally
returns status 1 because correctness assertions fail.

## Reproduce the executed Redis gate

The script below uses only the exact Redis pin, starts isolated containers, performs
bounded readiness polling, and removes only its two named containers on exit:

```sh
./scripts/run_redis_gate.sh
```

The standalone command intentionally exits non-zero because the frozen candidates
violate the mixed-configuration TTL invariant; the runner checks for that expected
gate failure before continuing to focused Cluster negative controls.

## Reproduce the executed Red Hat Valkey gate

The Red Hat pull secret is passed by path and is never copied into this directory:

```sh
./scripts/run_valkey_gate.sh /path/to/pull-secret.json
```

The originally supplied `registry.access.redhat.com` reference reports that this
terms-gated repository is served from authenticated `registry.redhat.io`. The runner
uses that canonical endpoint while preserving the exact manifest digest. Preflight
failures from the original endpoint and stale default authentication are retained as
diagnostics under `evidence/runtime/`; they are not qualification results.

## Reproduce the repaired standalone and Sentinel qualification

The unified repaired gate refreshes immutable sources, tests and lints the harness,
pulls both exact images, runs the repaired standalone suite under `EVAL` and
`EVALSHA`, then starts a clean one-primary/one-replica/three-Sentinel topology for
each product and invocation. Sentinel readiness requires quorum plus a test-only
`WAIT 1` marker and replica readback; the production adapter does not issue `WAIT`
or `WAITAOF`.

```sh
./scripts/run_repaired_gate.sh /path/to/red-hat-pull-secret.json
```

Each Sentinel lane is destructive only to its uniquely named isolated container and
is restarted before the next invocation. The command overwrites the six repaired
runtime reports and regenerates `contract.json`, `source-ledger.json`,
`result-matrix.json`, `report.md`, and `redis-rs-production-suitability.md`.

For an offline rerun only after independently verifying that both exact digests are
already present locally:

```sh
USE_CACHED_IMAGES=1 ./scripts/run_repaired_gate.sh
```

This explicit override still checks the cached Red Hat image digest. It is not a
substitute for the authenticated pull in reproducible CI or release qualification.
