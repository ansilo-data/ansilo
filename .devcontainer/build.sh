#/bin/bash

# Builds and pushs a new version of the dev container

set -e

AWS_PROFILE=${1:-"ansilo"}

if [[ -z "$AWS_PROFILE" ]];
then
    echo "Usage: $0 [aws profile=ansilo]"
    exit 1
fi

docker build -t ansilo-devcontainer .

docker tag ansilo-devcontainer 635198228996.dkr.ecr.ap-southeast-2.amazonaws.com/devcontainer:latest

aws ecr get-login-password --profile $AWS_PROFILE --region ap-southeast-2 | docker login --username AWS --password-stdin 635198228996.dkr.ecr.ap-southeast-2.amazonaws.com

docker push 635198228996.dkr.ecr.ap-southeast-2.amazonaws.com/devcontainer:latest