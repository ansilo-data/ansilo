#!/bin/bash

set -e

$(dirname $0)/build-setup.sh

echo "----- Building debug -----"
cargo build --frozen --locked
echo ""

echo "----- Building release -----"
cargo build --frozen --locked --release
echo ""
