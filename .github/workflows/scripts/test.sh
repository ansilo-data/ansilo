#!/bin/bash

set -e

export AWS_PAGER=""
export AWS_DEFAULT_REGION=ap-southeast-2
export AWS_REGION=ap-southeast-2
export RUST_BACKTRACE=1
export ANSILO_TESTS_ECS_USE_PUBLIC_IP=true
export ANSILO_TEST_PG_DIR=/usr/pgsql-14/
export ANSILO_TEST_ECS_TASK_PREFIX="gha-$GHA_RUN_ID"
export ANSILO_GHA_TESTS=true

function clean_old_tmp_files() {
    while true;
    do
        echo "== Cleaning old /tmp files =="
        df -h
        find /tmp/ -type f -mmin +5 -delete >/dev/null 2>&1 || true
        sleep 60
    done
}

clean_old_tmp_files &

echo "----- Running tests -----"
cargo test
echo ""
