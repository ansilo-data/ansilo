#!/bin/bash

set -e

# Run build
cargo build --release 
cargo pgx package -p ansilo-pgx --out-dir target/release/ansilo-pgx/

# Copy release artifacts
mkdir artifacts
cp target/release/ansilo-main artifacts
cp target/release/*.jar artifacts
cp -r target/release/frontend/out artifacts/frontend 
cp -r target/release/ansilo-pgx artifacts/pgx 

# List artifacts
du -h artifacts
