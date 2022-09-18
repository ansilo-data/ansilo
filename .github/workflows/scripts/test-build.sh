#!/bin/bash

set -e

$(dirname $0)/build-setup.sh

echo "----- Building debug -----"
cargo build --locked -vvv
echo ""

echo "----- Building release -----"
cargo build --locked --release
echo ""
