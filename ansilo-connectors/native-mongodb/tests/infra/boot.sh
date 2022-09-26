#!/bin/bash

# Boot script will start the database and will self-terminate if not accessed after period

set -e

TIMEOUT_DURATION=1800
LISTEN_PORT=27018

function cleanup {
    if [[ ! -z "$MONGO_PID" ]];
    then
        set +e
        echo "Terminating mongo..."
        kill -INT $MONGO_PID
    fi
}

trap cleanup EXIT INT TERM

echo "Starting mongo..."
docker-entrypoint.sh mongod &
MONGO_PID=$!
echo "mongo started as pid $MONGO_PID"

echo "Running lazyprox..."
lazyprox \
    --listen 0.0.0.0:$LISTEN_PORT \
    --dest localhost:27017 \
    --idle-timeout-secs $TIMEOUT_DURATION

