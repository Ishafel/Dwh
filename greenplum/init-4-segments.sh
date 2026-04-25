#!/usr/bin/env bash
set -euo pipefail

gp_hostname="$(hostname)"
database_name="${GREENPLUM_DATABASE_NAME:-gpdb}"
pxf_servers_source="/greenplum/pxf/servers"
pxf_servers_target="/data/pxf/servers"
greenplum_password="${GREENPLUM_PASSWORD:-gpadminpw}"

export PGPASSWORD="${greenplum_password}"

mkdir -p /data/00/primary /data/01/primary /data/02/primary /data/03/primary
echo "${gp_hostname}" > /tmp/hostfile_gpinitsystem
echo "*:5432:*:gpadmin:${greenplum_password}" > "${HOME}/.pgpass"
chmod 600 "${HOME}/.pgpass"

if [ -d "${pxf_servers_source}" ]; then
    rm -rf "${pxf_servers_target}"
    mkdir -p "${pxf_servers_target}"
    cp -R "${pxf_servers_source}/." "${pxf_servers_target}/"
    if command -v pxf >/dev/null 2>&1 && [ -f /data/pxf/conf/pxf-env.sh ]; then
        pxf cluster sync || true
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
