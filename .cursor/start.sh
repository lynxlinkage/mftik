#!/usr/bin/env bash
# Per-boot startup for a Cursor Cloud Agent.
#
# Brings the Docker daemon up (the base image has no systemd), works around a
# host firewall quirk that blocks compose's bridge networking, and starts the
# full dev stack detached. Idempotent: re-running reconciles the stack instead
# of duplicating it.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

export PATH="$HOME/.local/bin:$PATH"

# --- docker daemon -------------------------------------------------------
# No systemd here, so start dockerd by hand and wait for the socket.
if ! sudo docker info >/dev/null 2>&1; then
  echo "[start] launching dockerd"
  sudo nohup dockerd >/tmp/dockerd.log 2>&1 &
  for _ in $(seq 1 30); do
    if sudo docker info >/dev/null 2>&1; then break; fi
    sleep 1
  done
fi

# --- bridge networking fix ----------------------------------------------
# Docker 29 programs the nftables backend, but this base image also carries a
# stray iptables-legacy ruleset whose FORWARD policy is DROP and which only
# whitelists the default docker0 bridge. That legacy chain also hooks the
# kernel forward path, so it silently drops traffic on compose's br-* bridge
# and containers cannot reach postgres/nats/redis by name. Opening the legacy
# FORWARD policy lets the nftables rules (which do the real filtering) apply.
if command -v iptables-legacy >/dev/null 2>&1; then
  sudo iptables-legacy -P FORWARD ACCEPT || true
fi

# Let the agent user talk to the daemon without sudo (group membership from
# install.sh only applies to fresh logins).
sudo chmod 666 /var/run/docker.sock || true

# --- dev stack -----------------------------------------------------------
# `just up` builds the shared image once, then `up -d`. Compose waits on the
# postgres/nats/redis healthchecks and on migrate+seed completing before the
# planes and api start.
echo "[start] bringing up the compose stack"
just up -d

# --- readiness -----------------------------------------------------------
echo "[start] waiting for the api to answer /health"
for _ in $(seq 1 60); do
  if curl -fsS http://localhost:8000/health >/dev/null 2>&1; then
    echo "[start] api is healthy"
    break
  fi
  sleep 2
done

echo "[start] stack is up — API http://localhost:8000  UI http://localhost:5173"
