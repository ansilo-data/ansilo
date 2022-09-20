#!/bin/bash

set -e

INPUT=$(cat /dev/stdin)
PASS=$(echo $INPUT | jq -r ".password");
CONF=$(echo $INPUT | jq -r ".user_config");

if [ "$PASS" != "password1" ];
then
    echo '{"result": "failure", "message": "incorrect password"}'
    exit 0
fi

echo "{\"result\": \"success\", \"context\": $CONF}"