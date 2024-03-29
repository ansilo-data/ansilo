#!/bin/bash

set -e

export AWS_PAGER=""
export AWS_DEFAULT_REGION=ap-southeast-2
export AWS_REGION=ap-southeast-2
export RUST_BACKTRACE=1
export ANSILO_TESTS_ECS_USE_PUBLIC_IP=true
export ANSILO_TEST_PG_DIR=/usr/pgsql-15/
export ANSILO_TEST_ECS_TASK_PREFIX="gha-$GHA_RUN_ID"
export ANSILO_GHA_TESTS=true

function track_usage() {
    while true;
    do
        echo "== Track usage =="
        free -m | awk 'NR==2{printf "Memory Usage: %s/%sMB (%.2f%%)\n", $3,$2,$3*100/$2 }'
        df -h | awk '$NF=="/"{printf "Disk Usage: %d/%dGB (%s)\n", $3,$2,$5}'
        top -bn1 | grep load | awk '{printf "CPU Load: %.2f\n", $(NF-2)}' 
        sleep 60
    done
}

track_usage &

echo "----- Installing pgx extension -----"
sudo setfacl -m u:$(id -u):rwx $(pg_config --sharedir)/extension/ $(pg_config --pkglibdir)
cargo pgx install -p ansilo-pgx
rm -rf target/pgx-test-data-* # If this dir is present but empty pgx test craps out
echo ""

echo "----- Running tests -----"
export ANSILO_SKIP_PGX_INSTALL=true
cargo test
echo ""
