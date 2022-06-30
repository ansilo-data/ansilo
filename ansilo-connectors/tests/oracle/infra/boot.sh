#!/bin/bash

# Boot script will start the oracle database and will self-terminate if not access after period

set -e

TIMEOUT_DURATION=5
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

echo "Starting socat proxy..."
touch ~/access.log
socat -d -d TCP-LISTEN:$LISTEN_PORT,reuseaddr,fork,bind=0.0.0.0 TCP:localhost:1521 2>&1 | tee ~/access.log 2>&1 &
SOCAT_PID=$$
echo "Socat started as pid $SOCAT_PID"

while true;
do
    echo "Waiting for connections on port $LISTEN_PORT..."
    timeout $TIMEOUT_DURATION tail -n0 -f ~/access.log | head -n1 > /tmp/access.log || true

    if [[ ! -z "$(cat /tmp/access.log)" ]];
    then
        echo "Detected access, restarting timeout..."
        sleep 1
    else
        echo "Timed out, exiting..."
        exit 0
    fi
done
