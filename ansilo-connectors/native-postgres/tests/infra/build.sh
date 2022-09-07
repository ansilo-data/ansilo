#/bin/bash

# requires docker login for ECR

set -e

VERSION=$1

if [[ -z $VERSION ]];
then
    echo "usage $0 [version tag]"
    exit 1
fi

docker pull postgres:$VERSION
docker tag postgres:$VERSION postgres-base
docker build -t postgres-test .
aws ecr get-login-password --region ap-southeast-2 | docker login --username AWS --password-stdin 635198228996.dkr.ecr.ap-southeast-2.amazonaws.com
docker tag postgres-test 635198228996.dkr.ecr.ap-southeast-2.amazonaws.com/postgres:$VERSION
docker push 635198228996.dkr.ecr.ap-southeast-2.amazonaws.com/postgres:$VERSION

echo "Done!"
