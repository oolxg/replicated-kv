#!/usr/bin/env bash
# Startup script for a kv service VM (storage node or router): fetch the
# binary from GCS, write the env file, run under systemd.
set -euo pipefail

# Debian GCE images ship gsutil; install the CLI if a future image drops it.
if ! command -v gsutil >/dev/null; then
  apt-get update -qq && apt-get install -y -qq google-cloud-cli
fi

gsutil cp "gs://${bucket}/${binary}" /usr/local/bin/kv-${binary}
chmod +x /usr/local/bin/kv-${binary}

cat > /etc/kv-${binary}.env <<'ENVEOF'
${env}
ENVEOF

cat > /etc/systemd/system/kv-${binary}.service <<UNITEOF
[Unit]
Description=replicated-kv ${binary}
After=network-online.target
Wants=network-online.target

[Service]
EnvironmentFile=/etc/kv-${binary}.env
ExecStart=/usr/local/bin/kv-${binary}
Restart=on-failure
RestartSec=1

[Install]
WantedBy=multi-user.target
UNITEOF

systemctl daemon-reload
systemctl enable --now kv-${binary}.service
