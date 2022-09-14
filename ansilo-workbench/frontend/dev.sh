#!/bin/bash

DIR=$(realpath $(dirname $0))

export NEXT_PUBLIC_API_ORIGIN="http://localhost:5001"
export ANSILO_CORS_ALLOWED_HOST="http://localhost:5000"

./node_modules/.bin/concurrently \
    "./node_modules/.bin/next dev -p 5000" \
    "cd ../../ansilo-main && cargo run -- dev -c $DIR/nodes/customers/config.yml"
