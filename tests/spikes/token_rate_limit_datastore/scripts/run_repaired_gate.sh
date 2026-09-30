#!/bin/sh
set -eu

ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
REDIS_IMAGE='docker.io/library/redis:8.10.2-alpine@sha256:3811787313eba226a2ef38658c6ccb91cd5e110edc89c37767de373120a0e5a0'
VALKEY_IMAGE='registry.redhat.io/rhel10/valkey-8@sha256:5929be16ac020c4851d8dc1f5ead6d48c6da066a9a74349711186925ebd553b9'
AUTHFILE=${1:-${REGISTRY_AUTH_FILE:-}}
USE_CACHED_IMAGES=${USE_CACHED_IMAGES:-0}
ACTIVE_CONTAINER=

cleanup() {
  if [ -n "$ACTIVE_CONTAINER" ]; then
    podman rm --force "$ACTIVE_CONTAINER" >/dev/null 2>&1 || true
    ACTIVE_CONTAINER=
  fi
}

trap cleanup EXIT INT TERM

if [ "$USE_CACHED_IMAGES" != 1 ] && { [ -z "$AUTHFILE" ] || [ ! -r "$AUTHFILE" ]; }; then
  echo 'usage: run_repaired_gate.sh /path/to/red-hat-pull-secret.json' >&2
  echo 'or set USE_CACHED_IMAGES=1 only after independently verifying the exact local image digests' >&2
  exit 2
fi

wait_for_ping() {
  container=$1
  cli_bin=$2
  port=$3
  attempts=0
  until podman exec "$container" "$cli_bin" -p "$port" PING 2>/dev/null | grep -qx PONG; do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge 100 ]; then
      echo "readiness failed for $container:$port" >&2
      return 1
    fi
    sleep 0.1
  done
}

verify_local_digest() {
  image=$1
  expected=$2
  actual=$(podman image inspect "$image" --format '{{.Digest}}')
  if [ "$actual" != "$expected" ]; then
    echo "unexpected local image digest for $image: $actual" >&2
    return 1
  fi
}

prepare_images() {
  podman pull "$REDIS_IMAGE"
  if [ "$USE_CACHED_IMAGES" = 1 ]; then
    verify_local_digest "$VALKEY_IMAGE" 'sha256:5929be16ac020c4851d8dc1f5ead6d48c6da066a9a74349711186925ebd553b9'
  else
    podman pull --authfile "$AUTHFILE" "$VALKEY_IMAGE"
  fi
}

start_standalone() {
  ACTIVE_CONTAINER="praxis-spike-repaired-$PRODUCT-standalone"
  cleanup
  ACTIVE_CONTAINER="praxis-spike-repaired-$PRODUCT-standalone"
  podman run --detach --rm --name "$ACTIVE_CONTAINER" \
    --publish "127.0.0.1:$STANDALONE_PORT:6379" \
    --entrypoint "$SERVER_BIN" \
    "$IMAGE" \
    --port 6379 --bind 0.0.0.0 --protected-mode no \
    --save '' --appendonly no --maxmemory-policy noeviction >/dev/null
  wait_for_ping "$ACTIVE_CONTAINER" "$CLI_BIN" 6379
}

start_sentinel() {
  ACTIVE_CONTAINER="praxis-spike-repaired-$PRODUCT-sentinel"
  cleanup
  ACTIVE_CONTAINER="praxis-spike-repaired-$PRODUCT-sentinel"
  podman run --detach --rm --name "$ACTIVE_CONTAINER" \
    --publish 127.0.0.1:17200:17200 \
    --publish 127.0.0.1:17201:17201 \
    --publish 127.0.0.1:17210:17210 \
    --publish 127.0.0.1:17211:17211 \
    --publish 127.0.0.1:17212:17212 \
    --env SERVER_BIN="$SERVER_BIN" \
    --volume "$ROOT/scripts/start_sentinel_topology.sh:/start-sentinel-topology:ro" \
    --entrypoint /bin/sh \
    "$IMAGE" /start-sentinel-topology >/dev/null

  ready=false
  attempts=0
  while [ "$attempts" -lt 200 ]; do
    primary=$(podman exec "$ACTIVE_CONTAINER" "$CLI_BIN" -p 17200 --raw INFO replication 2>/dev/null | tr -d '\r' || true)
    replica=$(podman exec "$ACTIVE_CONTAINER" "$CLI_BIN" -p 17201 --raw INFO replication 2>/dev/null | tr -d '\r' || true)
    quorum=$(podman exec "$ACTIVE_CONTAINER" "$CLI_BIN" -p 17210 --raw SENTINEL CKQUORUM praxis-master 2>/dev/null | tr -d '\r' || true)
    if printf '%s\n' "$primary" | grep -qx 'role:master' \
      && printf '%s\n' "$replica" | grep -Eq '^role:(slave|replica)$' \
      && printf '%s\n' "$replica" | grep -qx 'master_link_status:up' \
      && printf '%s\n' "$quorum" | grep -q '^OK '; then
      ready=true
      break
    fi
    attempts=$((attempts + 1))
    sleep 0.1
  done
  if [ "$ready" != true ]; then
    echo "Sentinel readiness failed for $PRODUCT" >&2
    return 1
  fi

  podman exec "$ACTIVE_CONTAINER" "$CLI_BIN" -p 17200 SET praxis:spike:replication-ready ready | grep -qx OK
  podman exec "$ACTIVE_CONTAINER" "$CLI_BIN" -p 17200 WAIT 1 5000 | grep -qx 1
  podman exec "$ACTIVE_CONTAINER" "$CLI_BIN" -p 17201 --raw GET praxis:spike:replication-ready \
    | tr -d '\r' | grep -qx ready
  podman exec "$ACTIVE_CONTAINER" "$CLI_BIN" -p 17200 DEL praxis:spike:replication-ready | grep -qx 1
}

run_product() {
  PRODUCT=$1
  PRODUCT_ARGUMENT=$2
  VERSION=$3
  IMAGE=$4
  SERVER_BIN=$5
  CLI_BIN=$6
  STANDALONE_PORT=$7
  OUTPUT_PREFIX=$8

  start_standalone
  cargo run --locked -- fixed-standalone \
    --url "redis://127.0.0.1:$STANDALONE_PORT/" \
    --product "$PRODUCT_ARGUMENT" \
    --expected-version "$VERSION" \
    --output "evidence/runtime/$OUTPUT_PREFIX-fixed-standalone.json" \
    --allow-destructive-isolated
  cleanup

  for invocation in eval eval-sha; do
    start_sentinel
    if [ "$invocation" = eval ]; then
      suffix=eval
    else
      suffix=evalsha
    fi
    cargo run --locked -- sentinel \
      --sentinel redis://127.0.0.1:17210/ \
      --sentinel redis://127.0.0.1:17211/ \
      --sentinel redis://127.0.0.1:17212/ \
      --node redis://127.0.0.1:17200/ \
      --node redis://127.0.0.1:17201/ \
      --service-name praxis-master \
      --invocation "$invocation" \
      --product "$PRODUCT_ARGUMENT" \
      --expected-version "$VERSION" \
      --output "evidence/runtime/$OUTPUT_PREFIX-sentinel-$suffix.json" \
      --allow-destructive-isolated
    cleanup
  done
}

cd "$ROOT"
python3 scripts/fetch_sources.py
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
prepare_images

run_product redis redis 8.10.2 "$REDIS_IMAGE" /usr/local/bin/redis-server /usr/local/bin/redis-cli 16379 redis-8.10.2
run_product valkey red_hat_valkey 8.0.11 "$VALKEY_IMAGE" /usr/bin/valkey-server /usr/bin/valkey-cli 16380 red-hat-valkey-8.0.11

python3 scripts/capture_environment.py
python3 scripts/build_repaired_report.py
