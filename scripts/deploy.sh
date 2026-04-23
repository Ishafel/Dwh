#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR="${APP_DIR:-/mnt/bulk/dwh_greenplum}"
EXPECTED_DOCKER_ROOT="${EXPECTED_DOCKER_ROOT:-/mnt/bulk/docker}"
NIFI_HEALTH_URL="${NIFI_HEALTH_URL:-https://localhost:8443/nifi/}"
POSTGRES_DB="${POSTGRES_DB:-app}"
POSTGRES_USER="${POSTGRES_USER:-app}"
CLICKHOUSE_DB="${CLICKHOUSE_DB:-dwh}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-dwh}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-dwhpw}"

log() {
  printf '\n[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

run_optional() {
  local description="$1"
  shift

  log "$description"
  if ! "$@"; then
    log "WARNING: '$description' failed; continuing"
  fi
}

wait_for_nifi() {
  local attempts="${NIFI_HEALTH_ATTEMPTS:-20}"
  local delay_seconds="${NIFI_HEALTH_DELAY_SECONDS:-15}"
  local attempt

  log "NiFi health after deploy"
  for attempt in $(seq 1 "$attempts"); do
    if curl -kfsS "$NIFI_HEALTH_URL" >/dev/null; then
      printf 'NiFi is healthy: %s\n' "$NIFI_HEALTH_URL"
      return 0
    fi

    printf 'NiFi is not ready yet, attempt %s/%s\n' "$attempt" "$attempts"
    sleep "$delay_seconds"
  done

  printf 'NiFi health check failed after %s attempts: %s\n' "$attempts" "$NIFI_HEALTH_URL" >&2
  return 1
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'Required command is missing: %s\n' "$1" >&2
    exit 1
  fi
}

require_command docker
require_command git
require_command curl

if [ ! -d "$APP_DIR/.git" ]; then
  printf 'Application directory is not a git checkout: %s\n' "$APP_DIR" >&2
  exit 1
fi

cd "$APP_DIR"

log "Deployment target"
printf 'APP_DIR=%s\n' "$APP_DIR"
printf 'EXPECTED_DOCKER_ROOT=%s\n' "$EXPECTED_DOCKER_ROOT"

docker_root="$(docker info --format '{{.DockerRootDir}}')"
printf 'Docker root: %s\n' "$docker_root"
if [ "$docker_root" != "$EXPECTED_DOCKER_ROOT" ]; then
  printf 'Unexpected DockerRootDir: %s. Expected: %s\n' "$docker_root" "$EXPECTED_DOCKER_ROOT" >&2
  exit 1
fi

log "Git status before deploy"
git status --short --branch

if ! git diff --quiet || ! git diff --cached --quiet; then
  printf '\nTracked local changes detected in %s. Refusing to overwrite them.\n' "$APP_DIR" >&2
  git diff --stat
  exit 1
fi

run_optional "Docker Compose status before deploy" docker compose ps
run_optional "PostgreSQL row counts before deploy" \
  docker compose exec -T postgres \
    psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc \
    "select 'dm_schema=' || coalesce((select schema_name from information_schema.schemata where schema_name = 'dm'), 'missing');"
run_optional "Greenplum row counts before deploy" \
  docker compose exec -T -u gpadmin gpdb \
    /usr/local/greenplum-db/bin/psql -d gpdb -Atc \
    "select 'dwh.channel_notices=' || count(*) from dwh.channel_notices; select 'dwh.chat_messages=' || count(*) from dwh.chat_messages;"
run_optional "ClickHouse row counts before deploy" \
  docker compose exec -T clickhouse clickhouse-client \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    --database "$CLICKHOUSE_DB" \
    --query "SELECT 'channel_notices=' || toString(count()) FROM channel_notices; SELECT 'chat_messages=' || toString(count()) FROM chat_messages;"

log "Pulling latest main revision"
git pull --ff-only origin main

log "Git status after pull"
git status --short --branch
git --no-pager log -1 --oneline

log "Building and starting Docker Compose services"
docker compose up -d --build

log "Docker Compose status after deploy"
docker compose ps

log "PostgreSQL row counts after deploy"
docker compose exec -T postgres \
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc \
  "select 'dm_schema=' || coalesce((select schema_name from information_schema.schemata where schema_name = 'dm'), 'missing');"

log "Greenplum row counts after deploy"
docker compose exec -T -u gpadmin gpdb \
  /usr/local/greenplum-db/bin/psql -d gpdb -Atc \
  "select 'dwh.channel_notices=' || count(*) from dwh.channel_notices; select 'dwh.chat_messages=' || count(*) from dwh.chat_messages;"

log "ClickHouse row counts after deploy"
docker compose exec -T clickhouse clickhouse-client \
  --user "$CLICKHOUSE_USER" \
  --password "$CLICKHOUSE_PASSWORD" \
  --database "$CLICKHOUSE_DB" \
  --query "SELECT 'channel_notices=' || toString(count()) FROM channel_notices; SELECT 'chat_messages=' || toString(count()) FROM chat_messages;"

wait_for_nifi

log "Deploy finished"
