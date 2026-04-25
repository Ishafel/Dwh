#!/usr/bin/env bash
set -euo pipefail

if [ -f "${HOME}/.bashrc" ]; then
    source "${HOME}/.bashrc"
fi

gp_hostname="$(hostname)"
database_name="${GREENPLUM_DATABASE_NAME:-gpdb}"
pxf_servers_source="/greenplum/pxf/servers"
pxf_base="${PXF_BASE:-/data/pxf}"
pxf_env="${pxf_base}/conf/pxf-env.sh"
pxf_servers_target="${pxf_base}/servers"
greenplum_password="${GREENPLUM_PASSWORD:-gpadminpw}"
greenplum_pxf_enable="${GREENPLUM_PXF_ENABLE:-false}"

export PGPASSWORD="${greenplum_password}"

mkdir -p /data/00/primary /data/01/primary /data/02/primary /data/03/primary
echo "${gp_hostname}" > /tmp/hostfile_gpinitsystem
echo "*:5432:*:gpadmin:${greenplum_password}" > "${HOME}/.pgpass"
chmod 600 "${HOME}/.pgpass"

is_pxf_base_empty() {
    [ ! -d "${pxf_base}" ] || [ -z "$(find "${pxf_base}" -mindepth 1 -maxdepth 1 -print -quit)" ]
}

is_pxf_base_only_servers() {
    local entry

    [ -d "${pxf_base}" ] || return 1
    [ -n "$(find "${pxf_base}" -mindepth 1 -maxdepth 1 -print -quit)" ] || return 1

    while IFS= read -r entry; do
        [ "$(basename "${entry}")" = "servers" ] || return 1
    done < <(find "${pxf_base}" -mindepth 1 -maxdepth 1)
}

sync_pxf_server_configs() {
    rm -rf "${pxf_servers_target}"
    mkdir -p "${pxf_servers_target}"
    cp -R "${pxf_servers_source}/." "${pxf_servers_target}/"
    if command -v pxf >/dev/null 2>&1 && [ -f "${pxf_env}" ]; then
        pxf cluster sync || true
    fi
}

sync_pxf_server_configs_after_prepare() {
    local attempt

    for attempt in $(seq 1 120); do
        if [ -f "${pxf_env}" ]; then
            sync_pxf_server_configs
            pxf cluster restart || true
            return 0
        fi
        sleep 2
    done

    echo "WARNING - PXF was not prepared within the expected time; skip PXF server config sync"
}

if [ "${greenplum_pxf_enable}" = "true" ] && [ -d "${pxf_servers_source}" ]; then
    if [ -f "${pxf_env}" ]; then
        sync_pxf_server_configs
    elif is_pxf_base_empty; then
        sync_pxf_server_configs_after_prepare &
    elif is_pxf_base_only_servers; then
        echo "WARNING - ${pxf_base} contains only stale server configs without ${pxf_env}; removing server configs before PXF prepare"
        rm -rf "${pxf_servers_target}"
        sync_pxf_server_configs_after_prepare &
    else
        echo "WARNING - ${pxf_base} is not empty but ${pxf_env} is missing; disabling PXF for this start to keep Greenplum available"
        export GREENPLUM_PXF_ENABLE=false
    fi
fi

cat > /tmp/gpinitsystem_config <<EOF
ARRAY_NAME="Greenplum in docker"
DATABASE_NAME=${database_name}
SEG_PREFIX=gpseg
PORT_BASE=6000
MASTER_HOSTNAME=${gp_hostname}
MASTER_DIRECTORY=/data/master
MASTER_PORT=5432
TRUSTED_SHELL=ssh
CHECK_POINT_SEGMENTS=8
ENCODING=UNICODE
MACHINE_LIST_FILE=/data/hostfile_gpinitsystem
declare -a DATA_DIRECTORY=(/data/00/primary /data/01/primary /data/02/primary /data/03/primary)
EOF

exec /start_gpdb.sh
