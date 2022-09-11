#!/bin/bash

# Boot script will start the database and will self-terminate if not accessed after period

set -e

TIMEOUT_DURATION=1800
LISTEN_PORT=3307

function cleanup {
    if [[ ! -z "$MYSQL_PID" ]];
    then
        set +e
        echo "Terminating mysql..."
        kill -INT $MYSQL_PID
    fi
}

trap cleanup EXIT INT TERM

echo "Starting mysql..."
docker-entrypoint.sh mysqld &
MYSQL_PID=$!
echo "Mysql started as pid $MYSQL_PID"

echo "Running lazyprox..."
lazyprox \
    --listen 0.0.0.0:$LISTEN_PORT \
    --dest localhost:3306 \
    --idle-timeout-secs $TIMEOUT_DURATION

