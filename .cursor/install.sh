#!/usr/bin/env bash
# Idempotent repository setup for a Cursor Cloud Agent.
#
# Installs the toolchain the monorepo needs that is not in the base image
# (uv, just, docker), materialises a local .env, and syncs the uv workspace
# and the frontend npm deps. Safe to run repeatedly: every step no-ops when
# it is already done.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

export PATH="$HOME/.local/bin:$PATH"

# --- uv (Python workspace manager) ---------------------------------------
if ! command -v uv >/dev/null 2>&1; then
  echo "[install] installing uv"
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi

# --- just (task runner used by the repo) ---------------------------------
if ! command -v just >/dev/null 2>&1; then
  echo "[install] installing just"
  curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh \
    | sudo bash -s -- --to /usr/local/bin
fi

# --- docker engine + compose ---------------------------------------------
# The dev stack (postgres, nats, redis, and every plane) runs via compose.
if ! command -v docker >/dev/null 2>&1; then
  echo "[install] installing docker engine"
  curl -fsSL https://get.docker.com | sudo sh
  sudo usermod -aG docker "$USER" || true
fi

# --- local environment file ----------------------------------------------
if [ ! -f .env ]; then
  echo "[install] creating .env from .env.example"
  cp .env.example .env
fi

# --- workspace + frontend dependencies -----------------------------------
echo "[install] syncing uv workspace and frontend deps"
just sync

echo "[install] done"
