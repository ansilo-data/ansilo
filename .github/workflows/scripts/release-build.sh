#!/bin/bash

set -e

$(dirname $0)/build-setup.sh

echo "----- Building release -----"
cargo build --locked --release -vvv
cargo pgx package -p ansilo-pgx --out-dir target/release/ansilo-pgx/
echo ""

echo "----- Copying artifacts -----"
mkdir artifacts
cp target/release/ansilo-main artifacts
cp target/release/*.jar artifacts
cp -r target/release/frontend/out artifacts/frontend 
cp -r target/release/ansilo-pgx artifacts/pgx 
echo ""

echo "----- Summary -----"
du -h artifacts
echo ""
