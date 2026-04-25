#!/usr/bin/env bash
set -euo pipefail

source /usr/local/greenplum-db/greenplum_path.sh

mkdir -p /data/landing
exec gpfdist -d /data/landing -p 8081 -l /tmp/gpfdist.log
