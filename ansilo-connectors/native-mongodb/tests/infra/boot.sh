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
echo -e "notsosecret" > /key
chmod 400 /key
chown mongodb:mongodb /key
docker-entrypoint.sh mongod --replSet rs0 --keyFile /key &
MONGO_PID=$!
echo "Mongo started as pid $MONGO_PID"

TRIES=0
while ((TRIES < 10));
do
    echo "Checking if started up..."
    set +e
    EXIT_CODE=$(timeout 2 nc -vz localhost 27017; echo $?)
    set -e

    if [[ $EXIT_CODE == 0 ]];
    then 
        break
    fi

    echo "Failed to connect, sleeping..."
    sleep 5
    let "TRIES+=1"
done

sleep 10
echo "Initialising replica set..."
mongosh --username $MONGO_INITDB_ROOT_USERNAME --password $MONGO_INITDB_ROOT_PASSWORD --eval < <(echo "rs.initiate();")
echo "Mongo startup successful!"

echo "Running lazyprox..."
lazyprox \
    --listen 0.0.0.0:$LISTEN_PORT \
    --dest localhost:27017 \
    --idle-timeout-secs $TIMEOUT_DURATION

