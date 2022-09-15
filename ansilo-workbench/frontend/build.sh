#!/bin/bash

set -e

DIR=$(realpath $(dirname $0))

if [[ ! -d $DIR/node_modules ]];
then
    echo "Installing npm deps..."
    npm ci
fi

echo "Building next static html"
npm run build && npm run export

echo "Done!"