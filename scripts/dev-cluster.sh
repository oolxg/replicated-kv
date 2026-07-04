#!/usr/bin/env bash
# Local dev cluster: builds, starts N storage nodes + 1 router, waits until
# ready, and tears everything down on Ctrl-C.
#
#   ./scripts/dev-cluster.sh [N]     # N storage nodes, default 3
#
# Ports: router on 19000, storage on 19001, 19002, ...
set -euo pipefail

N="${1:-3}"
ROUTER_PORT=19000
BASE_PORT=19001

cd "$(dirname "$0")/.."

echo "building..."
go build -o bin/storage ./cmd/storage
go build -o bin/router ./cmd/router

echo "killing stale kv processes..."
pkill -f 'bin/storage' 2>/dev/null || true
pkill -f 'bin/router' 2>/dev/null || true

pids=()
nodes=()
for i in $(seq 0 $((N - 1))); do
	port=$((BASE_PORT + i))
	KV_ADDR=":$port" ./bin/storage &
	pids+=($!)
	nodes+=("127.0.0.1:$port")
done

KV_NODES=$(
	IFS=,
	echo "${nodes[*]}"
)
KV_ADDR=":$ROUTER_PORT" KV_NODES="$KV_NODES" ./bin/router &
pids+=($!)

cleanup() {
	echo
	echo "stopping cluster..."
	kill "${pids[@]}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# Wait until the router is accepting requests (no foreground sleep needed).
curl -sf --retry 60 --retry-connrefused --retry-delay 0 "localhost:$ROUTER_PORT/healthz" >/dev/null

echo
echo "cluster up:  router :$ROUTER_PORT  ->  ${nodes[*]}"
echo "try:"
echo "  curl -XPUT localhost:$ROUTER_PORT/kv/apple -d '{\"value\":\"bar\"}'"
echo "  curl localhost:$ROUTER_PORT/kv/apple"
echo
echo "Ctrl-C to stop."

wait
