#!/bin/bash

set -e

echo ""
echo "----- Stop sccache server -----"
sccache --stop-server || true
echo ""