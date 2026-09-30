#!/usr/bin/env bash
set -euo pipefail

# Reproduces the token_rate_limit mixed-configuration over-admission bug in
# Praxis AI v0.4.1. It creates and destroys its own Redis container and does
# not expose Redis on a host port.

readonly PRAXIS_VERSION='v0.4.1'
readonly PRAXIS_COMMIT='b9d6016764888e02dc049ec088496b10b7e886c1'
readonly BACKEND_SHA256='9ea7c04bce8c64b43463392e69654d755dc16676ae6d36d53f4e5e3a6c62e7b8'
readonly BACKEND_URL="https://raw.githubusercontent.com/praxis-proxy/ai/${PRAXIS_COMMIT}/filters/src/token_rate_limit/backend.rs"
readonly REDIS_IMAGE='docker.io/library/redis:8.10.2-alpine@sha256:3811787313eba226a2ef38658c6ccb91cd5e110edc89c37767de373120a0e5a0'
readonly CONTAINER="praxis-trl-mixed-config-repro-$$"

for command in curl python3; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "missing required command: $command" >&2
    exit 2
  fi
done

if command -v podman >/dev/null 2>&1; then
  RUNTIME='podman'
elif command -v docker >/dev/null 2>&1; then
  RUNTIME='docker'
else
  echo 'missing required container runtime: install podman or docker' >&2
  exit 2
fi
readonly RUNTIME

WORKDIR=$(mktemp -d "${TMPDIR:-/tmp}/praxis-trl-repro.XXXXXX")
readonly WORKDIR

cleanup() {
  "$RUNTIME" rm --force "$CONTAINER" >/dev/null 2>&1 || true
  rm -rf "$WORKDIR"
}
trap cleanup EXIT INT TERM

fail() {
  echo "FAILED: $*" >&2
  exit 1
}

json_item() {
  python3 -c 'import json, sys; print(json.loads(sys.argv[1])[int(sys.argv[2])])' "$1" "$2"
}

key_hash() {
  python3 - "$@" <<'PY'
import hashlib
import sys

digest = hashlib.sha256()
for index, value in enumerate(sys.argv[1:]):
    if index:
        digest.update(b"\0")
    digest.update(value.encode())
print(digest.hexdigest())
PY
}

redis_command() {
  "$RUNTIME" exec "$CONTAINER" redis-cli --raw "$@"
}

redis_eval() {
  local script=$1
  shift
  "$RUNTIME" exec "$CONTAINER" redis-cli --json --eval "/tmp/$script" "$@"
}

echo "Fetching production Lua source from Praxis AI ${PRAXIS_VERSION} (${PRAXIS_COMMIT})"
curl --fail --silent --show-error --location "$BACKEND_URL" --output "$WORKDIR/backend.rs"

python3 - "$WORKDIR/backend.rs" "$WORKDIR" "$BACKEND_SHA256" <<'PY'
import hashlib
from pathlib import Path
import re
import sys

source_path = Path(sys.argv[1])
output = Path(sys.argv[2])
expected_digest = sys.argv[3]
source_bytes = source_path.read_bytes()
actual_digest = hashlib.sha256(source_bytes).hexdigest()
if actual_digest != expected_digest:
    raise SystemExit(
        f"backend.rs digest mismatch: expected {expected_digest}, got {actual_digest}"
    )
source = source_bytes.decode()
scripts = {
    "sliding-reserve.lua": "RESERVE_SCRIPT",
    "sliding-reconcile.lua": "RECONCILE_SCRIPT",
    "bucket-reserve.lua": "TOKEN_BUCKET_RESERVE_SCRIPT",
    "bucket-reconcile.lua": "TOKEN_BUCKET_RECONCILE_SCRIPT",
}
for filename, constant in scripts.items():
    pattern = rf'(?:pub\(super\)\s+)?const {constant}: &str = "\n(.*?)\n";'
    match = re.search(pattern, source, re.DOTALL)
    if not match:
        raise SystemExit(f"could not extract {constant} from backend.rs")
    body = match.group(1)
    if r'\"' in body or r'\\' in body:
        raise SystemExit(f"unexpected Rust escape in {constant}; update the extractor")
    (output / filename).write_text(body + "\n")
PY

echo "Starting isolated Redis 8.10.2 container with $RUNTIME"
"$RUNTIME" pull "$REDIS_IMAGE" >/dev/null
"$RUNTIME" run --detach --rm --name "$CONTAINER" \
  "$REDIS_IMAGE" redis-server \
  --bind 0.0.0.0 --protected-mode no --save '' --appendonly no \
  --maxmemory-policy noeviction >/dev/null

for _ in $(seq 1 100); do
  if [ "$(redis_command PING 2>/dev/null || true)" = 'PONG' ]; then
    break
  fi
  sleep 0.1
done
[ "$(redis_command PING 2>/dev/null || true)" = 'PONG' ] || fail 'Redis did not become ready'

for script in sliding-reserve.lua sliding-reconcile.lua bucket-reserve.lua bucket-reconcile.lua; do
  "$RUNTIME" cp "$WORKDIR/$script" "$CONTAINER:/tmp/$script"
done

namespace='praxis:trl:mixed-config-repro'
rule='shared-rule'
subject='shared-subject'

sliding_hash=$(key_hash "$namespace" "$rule" "$subject")
sliding_prefix="${namespace}:v1:${sliding_hash}"
sliding_keys=(
  "$sliding_prefix"
  "${sliding_prefix}:settled"
  "${sliding_prefix}:active"
  "${namespace}:keys"
  "${namespace}:active-count"
  "${namespace}:reservation-seq"
  "${namespace}:active-index"
)

echo 'Reproducing sliding-window history deletion and over-admission'
long_reply=$(redis_eval sliding-reserve.lua "${sliding_keys[@]}" , 2000 100 100 5 1 10000 5)
[ "$(json_item "$long_reply" 0)" = '1' ] || fail "long-window setup was not admitted: $long_reply"
long_id=$(json_item "$long_reply" 1)
reconcile_reply=$(redis_eval sliding-reconcile.lua "${sliding_keys[@]}" , "$long_id" 5)
[ "$(json_item "$reconcile_reply" 0)" = '1' ] || fail "long-window settlement failed: $reconcile_reply"

settled_before=$(redis_command ZCARD "${sliding_prefix}:settled")
[ "$settled_before" = '1' ] || fail "expected one settled entry, got $settled_before"

sleep 0.03
short_reply=$(redis_eval sliding-reserve.lua "${sliding_keys[@]}" , 100 100 100 1 1 10 5)
[ "$(json_item "$short_reply" 0)" = '1' ] || fail "short-window writer was not admitted: $short_reply"
settled_after=$(redis_command ZCARD "${sliding_prefix}:settled")
[ "$settled_after" = '0' ] || fail "expected short writer to delete history, got ZCARD=$settled_after"

long_retry=$(redis_eval sliding-reserve.lua "${sliding_keys[@]}" , 2000 100 100 1 1 10000 5)
[ "$(json_item "$long_retry" 0)" = '1' ] || fail "expected reproduced over-admission, got $long_retry"

echo "  settled history: ${settled_before} -> ${settled_after}"
echo "  long-window retry: admitted (expected denial)"

redis_command FLUSHDB >/dev/null

bucket_hash=$(key_hash "$namespace" 'token_bucket' "$rule" "$subject")
bucket_prefix="${namespace}:v1:tb:${bucket_hash}"
bucket_keys=(
  "$bucket_prefix"
  "${bucket_prefix}:active"
  "${namespace}:tb:keys"
  "${namespace}:tb:active-count"
  "${namespace}:tb:reservation-seq"
  "${namespace}:tb:active-index"
)

echo 'Reproducing token-bucket early expiry and full-capacity reset'
slow_reply=$(redis_eval bucket-reserve.lua "${bucket_keys[@]}" , 10 1 2000 100 100 10)
[ "$(json_item "$slow_reply" 0)" = '1' ] || fail "slow-refill setup was not admitted: $slow_reply"
slow_id=$(json_item "$slow_reply" 1)
bucket_reconcile=$(redis_eval bucket-reconcile.lua "${bucket_keys[@]}" , "$slow_id" 10 10 1)
[ "$(json_item "$bucket_reconcile" 0)" = '1' ] || fail "slow-refill settlement failed: $bucket_reconcile"

slow_pttl=$(redis_command PTTL "$bucket_prefix")
(( slow_pttl >= 11000 )) || fail "expected approximately 12000ms initial bucket TTL, got ${slow_pttl}ms"

fast_reply=$(redis_eval bucket-reserve.lua "${bucket_keys[@]}" , 10 10 100 100 100 10)
[ "$(json_item "$fast_reply" 0)" = '0' ] || fail "fast-refill writer should initially deny: $fast_reply"
fast_pttl=$(redis_command PTTL "$bucket_prefix")
(( fast_pttl > 0 && fast_pttl <= 1200 )) || fail "expected shortened TTL near 1100ms, got ${fast_pttl}ms"

sleep 1.2
state_exists=$(redis_command EXISTS "$bucket_prefix")
[ "$state_exists" = '0' ] || fail 'depleted bucket did not expire after shortened TTL'

slow_retry=$(redis_eval bucket-reserve.lua "${bucket_keys[@]}" , 10 1 2000 100 100 10)
[ "$(json_item "$slow_retry" 0)" = '1' ] || fail "expected full-capacity reset and admission, got $slow_retry"

echo "  bucket TTL: ${slow_pttl}ms -> ${fast_pttl}ms"
echo '  bucket state after 1.2s: absent'
echo '  slow-refill retry of 10 tokens: admitted (expected denial)'
echo
echo "CONFIRMED: Praxis AI ${PRAXIS_VERSION} mixed-config writers silently over-admit"
