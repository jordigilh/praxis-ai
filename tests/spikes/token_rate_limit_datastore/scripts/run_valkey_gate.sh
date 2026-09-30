#!/bin/sh
set -eu

ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
IMAGE='registry.redhat.io/rhel10/valkey-8@sha256:5929be16ac020c4851d8dc1f5ead6d48c6da066a9a74349711186925ebd553b9'
STANDALONE='praxis-spike-valkey-standalone'
CLUSTER='praxis-spike-valkey-cluster'
AUTHFILE=${1:-${REGISTRY_AUTH_FILE:-}}

if [ -z "$AUTHFILE" ] || [ ! -r "$AUTHFILE" ]; then
  echo 'usage: run_valkey_gate.sh /path/to/red-hat-pull-secret.json' >&2
  exit 2
fi

cleanup() {
  podman rm --force "$STANDALONE" >/dev/null 2>&1 || true
  podman rm --force "$CLUSTER" >/dev/null 2>&1 || true
}

wait_for_ping() {
  container=$1
  port=$2
  attempts=0
  until podman exec "$container" /usr/bin/valkey-cli -p "$port" PING 2>/dev/null | grep -qx PONG; do
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

podman pull --authfile "$AUTHFILE" "$IMAGE"
podman run --detach --rm --name "$STANDALONE" \
  --publish 127.0.0.1:16380:6379 \
  --entrypoint /usr/bin/valkey-server \
  "$IMAGE" \
  --port 6379 --bind 0.0.0.0 --protected-mode no \
  --save '' --appendonly no --maxmemory-policy noeviction
wait_for_ping "$STANDALONE" 6379

if cargo run --locked -- standalone \
  --url redis://127.0.0.1:16380/ \
  --product red_hat_valkey \
  --expected-version 8.0.11 \
  --output evidence/runtime/red-hat-valkey-8.0.11-standalone.json \
  --allow-destructive-isolated; then
  echo 'expected the frozen candidates to fail their correctness gate' >&2
  exit 1
fi

podman run --detach --rm --name "$CLUSTER" \
  --publish 127.0.0.1:17100:17100 \
  --publish 127.0.0.1:17101:17101 \
  --publish 127.0.0.1:17102:17102 \
  --volume "$ROOT/scripts/start_valkey_cluster.sh:/usr/local/bin/start-spike-cluster:ro,Z" \
  --entrypoint /usr/local/bin/start-spike-cluster \
  "$IMAGE"
wait_for_ping "$CLUSTER" 17100
wait_for_ping "$CLUSTER" 17101
wait_for_ping "$CLUSTER" 17102

podman exec "$CLUSTER" /usr/bin/valkey-cli --cluster create \
  127.0.0.1:17100 127.0.0.1:17101 127.0.0.1:17102 \
  --cluster-replicas 0 --cluster-yes

attempts=0
until podman exec "$CLUSTER" /usr/bin/valkey-cli -p 17100 CLUSTER INFO 2>/dev/null | grep -q '^cluster_state:ok'; do
  attempts=$((attempts + 1))
  if [ "$attempts" -ge 100 ]; then
    echo 'Cluster did not reach cluster_state:ok' >&2
    exit 1
  fi
  sleep 0.1
done

cargo run --locked -- cluster-negative \
  --seed redis://127.0.0.1:17100/ \
  --seed redis://127.0.0.1:17101/ \
  --seed redis://127.0.0.1:17102/ \
  --product red_hat_valkey \
  --expected-version 8.0.11 \
  --output evidence/runtime/red-hat-valkey-8.0.11-cluster-negative.json \
  --allow-destructive-isolated

python3 scripts/build_report.py
python3 scripts/capture_environment.py
