#!/bin/bash

set -e

if [[ -f ~/image-build.tgz ]];
then 
    echo "----- Loading image from cache -----"
    docker load -i ~/image-build.tgz
    echo ""
else
    echo "----- Building image -----"
    docker build -t ansilo-build \
        --file .github/workflows/docker/build.ubi9.Dockerfile \
        .
    echo ""
    
    echo "----- Saving image -----"
    docker save -o ~/image-build.tgz ansilo-build
    echo ""
fi;
