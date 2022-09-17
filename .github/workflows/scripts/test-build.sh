#!/bin/bash

set -e

echo "----- Building debug -----"
cargo build
echo ""

echo "----- Building release -----"
cargo build --release
echo ""
