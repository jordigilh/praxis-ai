#!/bin/sh
set -eu

ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
IMAGE='docker.io/library/redis:8.10.2-alpine@sha256:3811787313eba226a2ef38658c6ccb91cd5e110edc89c37767de373120a0e5a0'
CONTAINER='praxis-spike-mixed-config-repro'
PORT='16381'
OUTPUT='evidence/runtime/redis-8.10.2-mixed-config-repro.json'

cleanup() {
  podman rm --force "$CONTAINER" >/dev/null 2>&1 || true
}

trap cleanup EXIT INT TERM
cleanup
cd "$ROOT"

podman pull "$IMAGE"
podman run --detach --rm --name "$CONTAINER" \
  --publish "127.0.0.1:${PORT}:6379" \
  "$IMAGE" redis-server \
  --bind 0.0.0.0 --protected-mode no --save '' --appendonly no --maxmemory-policy noeviction

attempts=0
until podman exec "$CONTAINER" redis-cli PING 2>/dev/null | grep -qx PONG; do
  attempts=$((attempts + 1))
  if [ "$attempts" -ge 100 ]; then
    echo 'Redis did not become ready' >&2
    exit 1
  fi
  sleep 0.1
done

set +e
cargo run --locked -- standalone \
  --url "redis://127.0.0.1:${PORT}/" \
  --product redis \
  --expected-version 8.10.2 \
  --output "$OUTPUT" \
  --allow-destructive-isolated
status=$?
set -e

if [ "$status" -ne 1 ]; then
  echo "expected correctness-gate exit 1, got $status" >&2
  exit 1
fi

python3 - "$OUTPUT" <<'PY'
import json
import sys

path = sys.argv[1]
report = json.load(open(path, encoding="utf-8"))
assert report["server_identity"]["redis_version"] == "8.10.2"

names = {
    "sliding_window.mixed_configuration_ttl_never_shortens",
    "sliding_window.mixed_configuration_preserves_long_window_history",
    "token_bucket.mixed_configuration_ttl_never_shortens",
    "token_bucket.mixed_configuration_preserves_slow_refill_state",
}

for candidate in report["candidates"]:
    observed = {entry["name"]: entry for entry in candidate["invariants"]}
    assert names <= observed.keys()
    assert all(not observed[name]["passed"] for name in names)

    sliding_ttl = observed[
        "sliding_window.mixed_configuration_ttl_never_shortens"
    ]["observed"]
    assert sliding_ttl["long_writer_pttl_ms"] >= 11_000
    assert sliding_ttl["after_short_writer_pttl_ms"] <= 1_200

    sliding = observed[
        "sliding_window.mixed_configuration_preserves_long_window_history"
    ]["observed"]
    assert sliding["settled_entries_before_short_writer"] == 1
    assert sliding["settled_entries_after_short_writer"] == 0
    assert sliding["long_writer_reply_after_short_writer"][0] == 1

    bucket = observed[
        "token_bucket.mixed_configuration_preserves_slow_refill_state"
    ]["observed"]
    assert bucket["state_exists_after_1200ms"] is False
    assert bucket["slow_writer_reply_after_1200ms"][0] == 1

print("CONFIRMED: EVAL and EVALSHA both reproduce the mixed-configuration bug")
print("- sliding history: 1 settled entry -> 0; long-window writer then admitted")
print("- token bucket: depleted state expired; slow-refill writer then admitted 10 tokens")
print(f"evidence: {path}")
PY
