#!/usr/bin/env bash
set -euo pipefail

superset db upgrade

if ! superset fab list-users | sed -nE 's/^username:([^[:space:]|]+).*/\1/p' | grep -qx "${SUPERSET_ADMIN_USERNAME:-admin}"; then
    superset fab create-admin \
        --username "${SUPERSET_ADMIN_USERNAME:-admin}" \
        --firstname "${SUPERSET_ADMIN_FIRSTNAME:-DWH}" \
        --lastname "${SUPERSET_ADMIN_LASTNAME:-Admin}" \
        --email "${SUPERSET_ADMIN_EMAIL:-admin@example.com}" \
        --password "${SUPERSET_ADMIN_PASSWORD:-SupersetAdmin123}"
fi

superset init

exec superset run \
    --host 0.0.0.0 \
    --port 8088 \
    --with-threads
