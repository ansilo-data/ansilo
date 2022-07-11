#!/bin/bash

# Boot script will start the oracle database and will self-terminate if not accessed after period

set -e

TIMEOUT_DURATION=1800
LISTEN_PORT=1522

function cleanup {
    if [[ ! -z "$ORACLE_PID" ]];
    then
        set +e
        echo "Terminating oracle..."
        kill -INT $ORACLE_PID
    fi

    if [[ ! -z "$SOCAT_PID" ]];
    then
        set +e
        echo "Terminating socat..."
        kill -INT $SOCAT_PID
    fi
}

trap cleanup EXIT INT TERM

echo "Starting oracle..."
/bin/sh -c $ORACLE_BASE/$RUN_FILE &
ORACLE_PID=$$
echo "Oracle started as pid $ORACLE_PID"

echo "Running lazyprox"
lazyprox \
    --listen 0.0.0.0:$LISTEN_PORT \
    --dest localhost:1521 \
    --idle-timeout-secs $TIMEOUT_DURATION

