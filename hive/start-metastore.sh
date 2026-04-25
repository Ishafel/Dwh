#!/usr/bin/env bash
set -euo pipefail

chown -R hive:hive /opt/hive/metastore_db /opt/hive/data/warehouse

export HIVE_CONF_DIR=/opt/hive/conf
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS:-} -Xmx512m"

if [ ! -d /opt/hive/metastore_db/metastore/seg0 ]; then
    /opt/hive/bin/schematool -dbType derby -initSchema
fi

exec /opt/hive/bin/hive --skiphadoopversion --skiphbasecp --service metastore
