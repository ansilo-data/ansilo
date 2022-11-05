#!/bin/bash

set -e

echo "----- Installing cargo pgx -----"
cargo install cargo-pgx --git https://github.com/tcdi/pgx.git --rev 4ad8a9
cargo pgx init --pg15 /usr/pgsql-15/bin/pg_config
echo ""
