#!/usr/bin/env bash
# Roll the production API compose on the host.
#
# Order, as operations asked for it:
#   1. docker compose down
#   2. copy the live file to docker-compose.bak.<shortsha>.yml
#   3. scp the repo file up, pin MFTIK_VERSION, pull, migrate, up
#   4. if anything after the backup fails, put the bak back and up
#
# Volumes stay. Traefik and the s7n planes are other projects and are
# not named here.

set -euo pipefail

REMOTE_DIR=/opt/mftik/deploy
COMPOSE_FILE=${COMPOSE_FILE:-deployment/docker-compose.yml}
SHORT_SHA=${SHORT_SHA:?SHORT_SHA is required}
VERSION=${VERSION:?VERSION is required}
SSH_HOST=${SSH_HOST:?SSH_HOST is required}
SSH_USER=${SSH_USER:?SSH_USER is required}
SSH_PRIVATE_KEY=${SSH_PRIVATE_KEY:?SSH_PRIVATE_KEY is required}
GHCR_USER=${GHCR_USER:?GHCR_USER is required}
GHCR_TOKEN=${GHCR_TOKEN:?GHCR_TOKEN is required}

BAK="docker-compose.bak.${SHORT_SHA}.yml"
KEY=$(mktemp)
chmod 600 "$KEY"
printf '%s\n' "$SSH_PRIVATE_KEY" >"$KEY"
cleanup_key() { rm -f "$KEY"; }
trap cleanup_key EXIT

ssh_opts=(-o BatchMode=yes -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -i "$KEY")

remote() {
  ssh "${ssh_opts[@]}" "${SSH_USER}@${SSH_HOST}" "$@"
}

echo "previous compose:"
PREV_VERSION=$(remote "grep '^MFTIK_VERSION=' ${REMOTE_DIR}/.env | cut -d= -f2-")
echo "MFTIK_VERSION was ${PREV_VERSION:-<unset>}"

echo "down + backup ${BAK}"
remote "set -euo pipefail
cd ${REMOTE_DIR}
docker compose down
cp -p docker-compose.yml ${BAK}
test -s ${BAK}
ls -l docker-compose.yml ${BAK}
"

rollback() {
  echo "deploy failed — restoring ${BAK} and MFTIK_VERSION=${PREV_VERSION}"
  remote "set -euo pipefail
cd ${REMOTE_DIR}
cp -p ${BAK} docker-compose.yml
if [ -n '${PREV_VERSION}' ]; then
  sed -i 's|^MFTIK_VERSION=.*|MFTIK_VERSION=${PREV_VERSION}|' .env
fi
grep '^MFTIK_VERSION=' .env
docker compose up -d --remove-orphans
docker compose ps
"
}

echo "scp ${COMPOSE_FILE}"
if ! scp "${ssh_opts[@]}" "$COMPOSE_FILE" \
  "${SSH_USER}@${SSH_HOST}:${REMOTE_DIR}/docker-compose.yml"; then
  rollback
  exit 1
fi

if ! remote "set -euo pipefail
cd ${REMOTE_DIR}
cp .env .env.bak-${SHORT_SHA}
sed -i 's|^MFTIK_VERSION=.*|MFTIK_VERSION=${VERSION}|' .env
grep '^MFTIK_VERSION=' .env

echo \"${GHCR_TOKEN}\" | docker login ghcr.io -u \"${GHCR_USER}\" --password-stdin
docker network inspect web >/dev/null 2>&1 || docker network create web
docker compose pull
docker compose --profile tools run --rm migrate
docker compose up -d --remove-orphans
docker logout ghcr.io >/dev/null
docker compose ps
"; then
  rollback
  exit 1
fi

echo "waiting for the site"
ok=0
for attempt in $(seq 1 30); do
  front=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 10 \
    https://mftik.lynkora.com/ || echo 000)
  api=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 10 \
    https://mftik.lynkora.com/api/auth/me || echo 000)
  echo "attempt ${attempt}: frontend=${front} api=${api}"
  if [ "$front" = "200" ] && { [ "$api" = "200" ] || [ "$api" = "401" ]; }; then
    ok=1
    break
  fi
  sleep 5
done

if [ "$ok" != 1 ]; then
  echo "site did not come back"
  rollback
  exit 1
fi

echo "api compose is serving ${VERSION} (bak ${BAK})"
