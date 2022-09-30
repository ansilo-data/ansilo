#!/bin/bash

# Boot script will start vault and will self-terminate if not accessed after period

set -e

TIMEOUT_DURATION=1800
LISTEN_PORT=8202

function cleanup {
    if [[ ! -z "$VAULT_PID" ]];
    then
        set +e
        echo "Terminating vault..."
        kill -INT $VAULT_PID || true
    fi
}

trap cleanup EXIT INT TERM

echo "Starting vault..."
docker-entrypoint.sh server -dev &
VAULT_PID=$!
echo "Vault started as pid $VAULT_PID"

TRIES=0
while ((TRIES < 20));
do
    echo "Checking if port open..."
    set +e
    nc -vzw0 localhost 8200
    EXIT_CODE=$?
    set -e

    if [[ $EXIT_CODE == 0 ]];
    then
        break
    fi
    
    echo "Port not open, sleeping..."
    sleep 5
    let "TRIES+=1"
done

echo "Configuring vault..."
/config/init.sh
echo "Vault started successfully!"

echo "Running lazyprox..."
lazyprox \
    --listen 0.0.0.0:$LISTEN_PORT \
    --dest localhost:8200 \
    --idle-timeout-secs $TIMEOUT_DURATION
