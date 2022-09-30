#!/bin/bash

DIR=$(realpath $(dirname $0))

export NEXT_PUBLIC_API_ORIGIN="http://localhost:5005"
export ANSILO_CORS_ALLOWED_ORIGIN="http://localhost:5004"
export ANSILO_SKIP_COMPILE_FRONTEND=true

./node_modules/.bin/concurrently \
    "./node_modules/.bin/next dev -p 5004" \
    "cd ../../ansilo-pgx && cargo pgx install && cd ../ansilo-main && cargo run -- dev -c $DIR/nodes/customers/config.yml --config-args PORT=5005"
