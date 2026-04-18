#!/usr/bin/env bash
set -euo pipefail

gp_hostname="$(hostname)"
database_name="${GREENPLUM_DATABASE_NAME:-gpdb}"

mkdir -p /data/00/primary /data/01/primary /data/02/primary /data/03/primary
echo "${gp_hostname}" > /tmp/hostfile_gpinitsystem

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
