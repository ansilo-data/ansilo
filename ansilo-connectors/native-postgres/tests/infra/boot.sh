#!/bin/bash

# Boot script will start the database and will self-terminate if not accessed after period

set -e

TIMEOUT_DURATION=1800
LISTEN_PORT=5433

function cleanup {
    if [[ ! -z "$POSTGRES_PID" ]];
    then
        set +e
        echo "Terminating postgres..."
        kill -INT $POSTGRES_PID
    fi
}

trap cleanup EXIT INT TERM

echo "Starting postgres..."
docker-entrypoint.sh postgres &
POSTGRES_PID=$$
echo "postgres started as pid $POSTGRES_PID"

echo "Running lazyprox..."
lazyprox \
    --listen 0.0.0.0:$LISTEN_PORT \
    --dest localhost:5432 \
    --idle-timeout-secs $TIMEOUT_DURATION

