#!/bin/bash

set -e

echo "----- Installing cargo pgx -----"
cargo install cargo-pgx --version 0.6.0
cargo pgx init --pg15 /usr/pgsql-15/bin/pg_config
echo ""
