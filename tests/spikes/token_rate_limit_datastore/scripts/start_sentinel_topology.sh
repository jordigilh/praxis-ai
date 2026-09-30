#!/bin/sh
set -eu

SERVER_BIN=${SERVER_BIN:?set SERVER_BIN to redis-server or valkey-server}
PRIMARY_PORT=${PRIMARY_PORT:-17200}
REPLICA_PORT=${REPLICA_PORT:-17201}
SENTINEL_PORT_1=${SENTINEL_PORT_1:-17210}
SENTINEL_PORT_2=${SENTINEL_PORT_2:-17211}
SENTINEL_PORT_3=${SENTINEL_PORT_3:-17212}
SERVICE_NAME=${SERVICE_NAME:-praxis-master}
ROOT=/tmp/praxis-sentinel

mkdir -p "$ROOT"

cat >"$ROOT/primary.conf" <<EOF
port $PRIMARY_PORT
bind 0.0.0.0
protected-mode no
daemonize yes
pidfile $ROOT/primary.pid
logfile $ROOT/primary.log
dir $ROOT
dbfilename primary.rdb
save ""
appendonly no
maxmemory-policy noeviction
enable-debug-command yes
replica-announce-ip 127.0.0.1
replica-announce-port $PRIMARY_PORT
repl-diskless-sync yes
repl-diskless-sync-delay 0
EOF

cat >"$ROOT/replica.conf" <<EOF
port $REPLICA_PORT
bind 0.0.0.0
protected-mode no
daemonize yes
pidfile $ROOT/replica.pid
logfile $ROOT/replica.log
dir $ROOT
dbfilename replica.rdb
save ""
appendonly no
maxmemory-policy noeviction
enable-debug-command yes
replicaof 127.0.0.1 $PRIMARY_PORT
replica-announce-ip 127.0.0.1
replica-announce-port $REPLICA_PORT
repl-diskless-sync yes
repl-diskless-sync-delay 0
EOF

"$SERVER_BIN" "$ROOT/primary.conf"
"$SERVER_BIN" "$ROOT/replica.conf"

start_sentinel() {
  port=$1
  index=$2
  config="$ROOT/sentinel-$index.conf"
  cat >"$config" <<EOF
port $port
bind 0.0.0.0
protected-mode no
daemonize yes
pidfile $ROOT/sentinel-$index.pid
logfile $ROOT/sentinel-$index.log
dir $ROOT
sentinel monitor $SERVICE_NAME 127.0.0.1 $PRIMARY_PORT 2
sentinel down-after-milliseconds $SERVICE_NAME 500
sentinel failover-timeout $SERVICE_NAME 5000
sentinel parallel-syncs $SERVICE_NAME 1
sentinel announce-ip 127.0.0.1
sentinel announce-port $port
EOF
  "$SERVER_BIN" "$config" --sentinel
}

start_sentinel "$SENTINEL_PORT_1" 1
start_sentinel "$SENTINEL_PORT_2" 2
start_sentinel "$SENTINEL_PORT_3" 3

while :; do
  sleep 3600
done
