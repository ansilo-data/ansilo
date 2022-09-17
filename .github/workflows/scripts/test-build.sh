#!/bin/bash

set -e

$(dirname $0)/build-setup.sh

echo "----- Building debug -----"
cargo build
echo ""

echo "----- Building release -----"
cargo build --release
echo ""
