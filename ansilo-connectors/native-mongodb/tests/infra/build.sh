#/bin/bash

# requires docker login for ECR

set -e

VERSION=$1

if [[ -z $VERSION ]];
then
    echo "usage $0 [version tag]"
    exit 1
fi

docker pull mongo:$VERSION
docker tag mongo:$VERSION mongo-base
docker build -t mongo-test .
aws ecr get-login-password --region ap-southeast-2 | docker login --username AWS --password-stdin 635198228996.dkr.ecr.ap-southeast-2.amazonaws.com
docker tag mongo-test 635198228996.dkr.ecr.ap-southeast-2.amazonaws.com/mongo:$VERSION
docker push 635198228996.dkr.ecr.ap-southeast-2.amazonaws.com/mongo:$VERSION

echo "Done!"
