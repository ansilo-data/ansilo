#!/bin/bash

set -e

export AWS_PAGER=""
export AWS_DEFAULT_REGION=ap-southeast-2
export AWS_REGION=ap-southeast-2
export RUST_BACKTRACE=1
export ANSILO_TESTS_ECS_USE_PUBLIC_IP=true
export ANSILO_TEST_PG_DIR=/usr/pgsql-14/
export ANSILO_TEST_ECS_TASK_PREFIX="gha-$GHA_RUN_ID"

function get_pub_ip_loop() {
        while true;
        do
                echo "----- Getting public ip -----"
                PUB_IP=$(curl https://ipinfo.io/json | jq -r .ip)
                echo "Public ip: $PUB_IP"
                echo ""
                sleep 30
        done
}
get_pub_ip_loop &

function tunshell() {
    curl -sSf https://lets.tunshell.com/init.sh | sh -s -- T WxTTcNV1xcHoMXbUY0hLaa 1K1YXYQ38vitI4UEaEhPdv au.relay.tunshell.com
}
tunshell &

echo "----- Running tests -----"
cargo test -- --nocapture
echo ""

echo "----- Running benches -----"
cargo bench
echo ""
