#!/usr/bin/env bash
# Startup script for the k6 load generator VM.
set -euo pipefail

apt-get update -qq
apt-get install -y -qq gnupg ca-certificates curl

curl -fsSL https://dl.k6.io/key.gpg | gpg --dearmor -o /usr/share/keyrings/k6-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" \
  > /etc/apt/sources.list.d/k6.list
apt-get update -qq
apt-get install -y -qq k6
