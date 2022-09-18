#!/bin/bash

set -e

$(dirname $0)/build-setup.sh

echo "----- Building debug -----"
cargo build --locked --tests
echo ""

echo "----- Building release -----"
cargo build --locked --release --tests
echo ""
