#!/usr/bin/env bash
set -euo pipefail

export HIVE_CONF_DIR=/opt/hive/client-conf
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS:-} -Xmx512m"

warehouse_dir=/opt/hive/data/warehouse/example_hive_customers
changelog_dir=/opt/hive/changelog/migrations

mkdir -p "${warehouse_dir}"

cat > "${warehouse_dir}/data.csv" <<'CSV'
1,Alice Ivanova,alice@example.com,2026-01-10 09:30:00
2,Boris Petrov,boris@example.com,2026-01-11 14:15:00
3,Clara Smirnova,clara@example.com,2026-01-12 18:45:00
CSV

chown -R hive:hive /opt/hive/data/warehouse

if [ ! -d "${changelog_dir}" ]; then
    echo "Hive changelog directory does not exist: ${changelog_dir}" >&2
    exit 1
fi

mapfile -t migrations < <(find "${changelog_dir}" -maxdepth 1 -type f -name '*.hql' | sort)

if [ "${#migrations[@]}" -eq 0 ]; then
    echo "No Hive migrations found in ${changelog_dir}" >&2
    exit 1
fi

for migration in "${migrations[@]}"; do
    echo "Applying Hive migration: ${migration}"
    /opt/hive/bin/hive --skiphadoopversion --skiphbasecp -f "${migration}"
done
