#!/bin/bash

set -e

echo "----- Installing cargo pgx -----"
cargo install cargo-pgx --version 0.5.0-beta.0
cargo pgx init --pg14 /usr/pgsql-14/bin/pg_config
echo ""
