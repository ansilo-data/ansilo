#!/bin/bash

# Boot script will start the database and will self-terminate if not accessed after period

set -e

TIMEOUT_DURATION=1800
LISTEN_PORT=1435

function cleanup {
    if [[ ! -z "$MSSQL_PID" ]];
    then
        set +e
        echo "Terminating mssql..."
        kill -INT $MSSQL_PID
    fi
}

trap cleanup EXIT INT TERM

echo "Starting mssql..."
/opt/mssql/bin/sqlservr &
MSSQL_PID=$!
echo "Mssql started as pid $MSSQL_PID"

TRIES=0
while ((TRIES < 20));
do
    echo "Checking if port open..."
    set +e
    nc -vzw1 localhost 1433
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

sleep 10

echo "Running db init scripts..."
/opt/mssql-tools/bin/sqlcmd -S localhost -l 60 -U SA -P "Ansilo_root[!]" -i /init-sql/01_user.sql 
echo "MSSQL startup successful!"

echo "Running lazyprox..."
lazyprox \
    --listen 0.0.0.0:$LISTEN_PORT \
    --dest localhost:1433 \
    --idle-timeout-secs $TIMEOUT_DURATION
