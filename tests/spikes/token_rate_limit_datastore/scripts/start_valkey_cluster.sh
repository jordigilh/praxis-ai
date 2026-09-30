#!/bin/sh
set -eu

for port in 17100 17101 17102; do
  directory="/tmp/valkey-cluster/$port"
  mkdir -p "$directory"
  /usr/bin/valkey-server \
    --port "$port" \
    --bind 0.0.0.0 \
    --protected-mode no \
    --save '' \
    --appendonly no \
    --maxmemory-policy noeviction \
    --cluster-enabled yes \
    --cluster-config-file "$directory/nodes.conf" \
    --cluster-node-timeout 2000 \
    --cluster-require-full-coverage yes \
    --cluster-announce-ip 127.0.0.1 \
    --cluster-announce-port "$port" \
    --cluster-announce-bus-port "$((port + 10000))" \
    --dir "$directory" \
    >"$directory/server.log" 2>&1 &
done

wait
