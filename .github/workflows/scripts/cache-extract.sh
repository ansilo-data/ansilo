#!/bin/bash

set -e

echo "----- Extracting cached archive $1 to $2 -----"
if [[ -f $1 ]];
then
        tar -C $2 -xzvf $1 .
else 
        echo "Cached file does not exist..."
fi
echo ""
