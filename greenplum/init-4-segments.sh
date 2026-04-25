#!/usr/bin/env bash
set -euo pipefail

gp_hostname="$(hostname)"
database_name="${GREENPLUM_DATABASE_NAME:-gpdb}"
pxf_servers_source="/greenplum/pxf/servers"
pxf_servers_target="/data/pxf/servers"
greenplum_password="${GREENPLUM_PASSWORD:-gpadminpw}"
postgres_db="${POSTGRES_DB:-app}"
postgres_user="${POSTGRES_USER:-app}"
postgres_password="${POSTGRES_PASSWORD:-apppw}"

export PGPASSWORD="${greenplum_password}"
export POSTGRES_DB="${postgres_db}"
export POSTGRES_USER="${postgres_user}"
export POSTGRES_PASSWORD="${postgres_password}"

mkdir -p /data/00/primary /data/01/primary /data/02/primary /data/03/primary
echo "${gp_hostname}" > /tmp/hostfile_gpinitsystem
echo "*:5432:*:gpadmin:${greenplum_password}" > "${HOME}/.pgpass"
chmod 600 "${HOME}/.pgpass"

xml_escape() {
    printf '%s' "$1" \
        | sed \
            -e 's/&/\&amp;/g' \
            -e 's/</\&lt;/g' \
            -e 's/>/\&gt;/g' \
            -e 's/"/\&quot;/g' \
            -e "s/'/\&apos;/g"
}

if [ -d "${pxf_servers_source}" ]; then
    mkdir -p "${pxf_servers_target}"
    cp -R "${pxf_servers_source}/." "${pxf_servers_target}/"
    mkdir -p "${pxf_servers_target}/postgres"
    cat > "${pxf_servers_target}/postgres/jdbc-site.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>jdbc.driver</name>
        <value>org.postgresql.Driver</value>
    </property>
    <property>
        <name>jdbc.url</name>
        <value>jdbc:postgresql://postgres:5432/$(xml_escape "${postgres_db}")</value>
    </property>
    <property>
        <name>jdbc.user</name>
        <value>$(xml_escape "${postgres_user}")</value>
    </property>
    <property>
        <name>jdbc.password</name>
        <value>$(xml_escape "${postgres_password}")</value>
    </property>
    <property>
        <name>jdbc.pool.enabled</name>
        <value>true</value>
    </property>
    <property>
        <name>jdbc.pool.property.maximumPoolSize</name>
        <value>5</value>
    </property>
</configuration>
EOF
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
