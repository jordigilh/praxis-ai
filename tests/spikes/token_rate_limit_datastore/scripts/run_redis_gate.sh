#!/bin/sh
set -eu

ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
IMAGE='docker.io/library/redis:8.10.2-alpine@sha256:3811787313eba226a2ef38658c6ccb91cd5e110edc89c37767de373120a0e5a0'
STANDALONE='praxis-spike-redis-standalone'
CLUSTER='praxis-spike-redis-cluster'

cleanup() {
  podman rm --force "$STANDALONE" >/dev/null 2>&1 || true
  podman rm --force "$CLUSTER" >/dev/null 2>&1 || true
}

wait_for_ping() {
  container=$1
  port=$2
  attempts=0
  until podman exec "$container" redis-cli -p "$port" PING 2>/dev/null | grep -qx PONG; do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge 100 ]; then
      echo "readiness failed for $container:$port" >&2
      return 1
    fi
    sleep 0.1
  done
}

trap cleanup EXIT INT TERM
cleanup

cd "$ROOT"
python3 scripts/fetch_sources.py
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo run --locked -- static-audit --output evidence/static-audit.json

podman pull "$IMAGE"
podman run --detach --rm --name "$STANDALONE" \
  --publish 127.0.0.1:16379:6379 \
  "$IMAGE" redis-server \
  --bind 0.0.0.0 --protected-mode no --save '' --appendonly no --maxmemory-policy noeviction
wait_for_ping "$STANDALONE" 6379

if cargo run --locked -- standalone \
  --url redis://127.0.0.1:16379/ \
  --product redis \
  --expected-version 8.10.2 \
  --output evidence/runtime/redis-8.10.2-standalone.json \
  --allow-destructive-isolated; then
  echo 'expected the frozen candidates to fail their correctness gate' >&2
  exit 1
fi

podman run --detach --rm --name "$CLUSTER" \
  --publish 127.0.0.1:17000:17000 \
  --publish 127.0.0.1:17001:17001 \
  --publish 127.0.0.1:17002:17002 \
  --volume "$ROOT/scripts/start_redis_cluster.sh:/usr/local/bin/start-spike-cluster:ro,Z" \
  --entrypoint /usr/local/bin/start-spike-cluster \
  "$IMAGE"
wait_for_ping "$CLUSTER" 17000
wait_for_ping "$CLUSTER" 17001
wait_for_ping "$CLUSTER" 17002

podman exec "$CLUSTER" redis-cli --cluster create \
  127.0.0.1:17000 127.0.0.1:17001 127.0.0.1:17002 \
  --cluster-replicas 0 --cluster-yes

attempts=0
until podman exec "$CLUSTER" redis-cli -p 17000 CLUSTER INFO 2>/dev/null | grep -q '^cluster_state:ok'; do
  attempts=$((attempts + 1))
  if [ "$attempts" -ge 100 ]; then
    echo 'Cluster did not reach cluster_state:ok' >&2
    exit 1
  fi
  sleep 0.1
done

cargo run --locked -- cluster-negative \
  --seed redis://127.0.0.1:17000/ \
  --seed redis://127.0.0.1:17001/ \
  --seed redis://127.0.0.1:17002/ \
  --product redis \
  --expected-version 8.10.2 \
  --output evidence/runtime/redis-8.10.2-cluster-negative.json \
  --allow-destructive-isolated

python3 scripts/build_report.py
python3 scripts/capture_environment.py
