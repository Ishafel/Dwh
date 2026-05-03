#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR="${APP_DIR:-/mnt/bulk/dwh_greenplum}"
EXPECTED_DOCKER_ROOT="${EXPECTED_DOCKER_ROOT:-/mnt/bulk/docker}"
NIFI_HEALTH_URL="${NIFI_HEALTH_URL:-https://localhost:8443/nifi/}"
SUPERSET_HEALTH_URL="${SUPERSET_HEALTH_URL:-http://localhost:8088/health}"

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

wait_for_superset() {
  local attempts="${SUPERSET_HEALTH_ATTEMPTS:-20}"
  local delay_seconds="${SUPERSET_HEALTH_DELAY_SECONDS:-15}"
  local attempt

  log "Superset health after deploy"
  for attempt in $(seq 1 "$attempts"); do
    if curl -fsS "$SUPERSET_HEALTH_URL" >/dev/null; then
      printf 'Superset is healthy: %s\n' "$SUPERSET_HEALTH_URL"
      return 0
    fi

    printf 'Superset is not ready yet, attempt %s/%s\n' "$attempt" "$attempts"
    sleep "$delay_seconds"
  done

  printf 'Superset health check failed after %s attempts: %s\n' "$attempts" "$SUPERSET_HEALTH_URL" >&2
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

log "Pulling latest main revision"
git pull --ff-only origin main

log "Git status after pull"
git status --short --branch
git --no-pager log -1 --oneline

log "Building and starting Docker Compose services"
docker compose up -d --build

log "Docker Compose status after deploy"
docker compose ps

wait_for_nifi
wait_for_superset

log "Deploy finished"
