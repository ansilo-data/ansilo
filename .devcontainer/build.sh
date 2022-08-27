#/bin/bash

# Builds and pushs a new version of the dev container

set -e

docker build -t ansilo-devcontainer .

docker tag ansilo-devcontainer 635198228996.dkr.ecr.ap-southeast-2.amazonaws.com/devcontainer:latest

aws ecr get-login-password --region ap-southeast-2 | docker login --username AWS --password-stdin 635198228996.dkr.ecr.ap-southeast-2.amazonaws.com

docker push 635198228996.dkr.ecr.ap-southeast-2.amazonaws.com/devcontainer:latest
