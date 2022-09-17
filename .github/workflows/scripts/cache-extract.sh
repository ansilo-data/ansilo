#!/bin/bash

set -e

echo "----- Extracting cached archive $1 to $2 -----"
if [[ -f $1 ]];
then
        tar -xzvf $1 $2
else 
        echo "Cached file does not exist..."
fi
echo ""