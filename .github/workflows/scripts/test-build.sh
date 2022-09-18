#!/bin/bash

set -e

$(dirname $0)/build-setup.sh

echo "----- Building debug -----"
cargo build --locked
echo ""

echo "----- Building release -----"
cargo build --locked --release
echo ""
