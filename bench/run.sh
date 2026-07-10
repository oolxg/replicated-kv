#!/usr/bin/env bash
# Runs one benchmark configuration end to end:
#   deploy -> wait healthy -> warmup -> measure -> control measure -> collect -> destroy
#
# Usage:            ./bench/run.sh <node_count> <project_id>
# Full curve:       for n in 1 3 5; do ./bench/run.sh $n <project_id>; done
#
# The control measure is a second, independent 60s window: if it disagrees
# with the first by more than a few percent, the numbers are not steady-state
# and must not be used.
set -euo pipefail

NODES="${1:?usage: run.sh <node_count> <project_id>}"
PROJECT="${2:?usage: run.sh <node_count> <project_id>}"
ZONE="${ZONE:-europe-west3-a}"
REGION="${REGION:-europe-west3}"
VUS="${VUS:-300}"
CPU_QUOTA="${CPU_QUOTA:-25%}" # bonus scale-up runs: CPU_QUOTA=40% VUS=450
ROUTER_INTERNAL="10.10.0.5:8080"

BENCH_DIR="$(cd "$(dirname "$0")" && pwd)"
TF_DIR="$BENCH_DIR/../deploy/terraform"
OUT_DIR="${OUT_DIR:-$BENCH_DIR/results}"
mkdir -p "$OUT_DIR"

tf() { terraform -chdir="$TF_DIR" "$@"; }
loadgen_ssh() { gcloud compute ssh kv-loadgen --zone "$ZONE" --project "$PROJECT" --command "$1" 2>/dev/null; }

echo "==> [$NODES node(s)] deploy (cpu quota $CPU_QUOTA, vus $VUS)"
tf apply -input=false -auto-approve -var "project=$PROJECT" -var "node_count=$NODES" -var kv_rf=1 -var "storage_cpu_quota=$CPU_QUOTA" -var "zone=$ZONE" -var "region=$REGION" >/dev/null
ROUTER_IP="$(tf output -raw router_public_ip)"
curl -sf --retry 60 --retry-all-errors --retry-delay 3 --max-time 5 "http://$ROUTER_IP:8080/healthz" >/dev/null
echo "    router up: $ROUTER_IP"

echo "==> [$NODES node(s)] verifying storage CPU quota is live"
QUOTA="$(gcloud compute ssh kv-storage-0 --zone "$ZONE" --project "$PROJECT" \
  --command 'systemctl show kv-storage -p CPUQuotaPerSecUSec' 2>/dev/null)"
case "$QUOTA" in
  *infinity*) echo "    FATAL: CPUQuota not applied on kv-storage-0 ($QUOTA)"; exit 1 ;;
  *) echo "    $QUOTA" ;;
esac

echo "==> [$NODES node(s)] waiting for k6 on loadgen"
loadgen_ssh 'for i in $(seq 1 60); do command -v k6 >/dev/null && exit 0; sleep 5; done; echo "k6 missing" >&2; exit 1'
gcloud compute scp "$BENCH_DIR/k6/throughput.js" "$BENCH_DIR/k6/seed.js" kv-loadgen:~/ --zone "$ZONE" --project "$PROJECT" >/dev/null 2>&1

echo "==> [$NODES node(s)] seeding keyspace (100k keys)"
loadgen_ssh "k6 run --quiet -e ROUTER=$ROUTER_INTERNAL ~/seed.js >/dev/null 2>&1"

echo "==> [$NODES node(s)] warmup 30s"
loadgen_ssh "k6 run --quiet -e ROUTER=$ROUTER_INTERNAL -e DURATION=30s -e VUS=$VUS ~/throughput.js >/dev/null 2>&1"

echo "==> [$NODES node(s)] measure 60s"
loadgen_ssh "k6 run --quiet -e ROUTER=$ROUTER_INTERNAL -e DURATION=60s -e VUS=$VUS --summary-export=\$HOME/result.json ~/throughput.js 2>&1 | grep -E 'http_reqs|http_req_duration' | grep -v expected"

echo "==> [$NODES node(s)] control measure 60s"
loadgen_ssh "k6 run --quiet -e ROUTER=$ROUTER_INTERNAL -e DURATION=60s -e VUS=$VUS --summary-export=\$HOME/control.json ~/throughput.js 2>&1 | grep -E 'http_reqs' | grep -v expected"
loadgen_ssh 'echo "    loadgen load: $(uptime | sed "s/^.*load average/load average/")"'

gcloud compute scp "kv-loadgen:~/result.json" "$OUT_DIR/nodes-$NODES.json" --zone "$ZONE" --project "$PROJECT" >/dev/null 2>&1
gcloud compute scp "kv-loadgen:~/control.json" "$OUT_DIR/nodes-$NODES-control.json" --zone "$ZONE" --project "$PROJECT" >/dev/null 2>&1
echo "==> [$NODES node(s)] saved: results/nodes-$NODES.json (+ control)"

echo "==> [$NODES node(s)] destroy"
tf destroy -input=false -auto-approve -var "project=$PROJECT" -var "node_count=$NODES" -var kv_rf=1 -var "storage_cpu_quota=$CPU_QUOTA" -var "zone=$ZONE" -var "region=$REGION" >/dev/null
echo "==> [$NODES node(s)] done"
