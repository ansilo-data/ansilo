#!/bin/bash

set -e

echo "----- Compressing $1 into cached archibe $2 -----"
GZIP=--fast tar -czvf $2 $1
echo ""