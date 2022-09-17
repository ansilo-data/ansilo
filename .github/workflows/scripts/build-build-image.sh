#!/bin/bash

set -e

if [[ -f ~/image-build.tgz ]];
then 
    docker load -i ~/image-build.tgz
    echo "Image loaded from cache..."
else
    docker build -t ansilo-build \
        --file .github/workflows/docker/build.ubi9.Dockerfile \
        .
    docker save -o ~/image-build.tgz ansilo-build
    echo "Image saved to ~/image-build.tgz..."
fi;
