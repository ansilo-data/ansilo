#/bin/bash

# requires docker login for ECR

set -e

VERSION=$1

if [[ -z $VERSION ]];
then
    echo "usage $0 [version tag]"
    exit 1
fi

docker pull mysql:$VERSION
docker tag mysql:$VERSION mysql-base
docker build -t mysql-test .
aws ecr get-login-password --region ap-southeast-2 | docker login --username AWS --password-stdin 635198228996.dkr.ecr.ap-southeast-2.amazonaws.com
docker tag mysql-test 635198228996.dkr.ecr.ap-southeast-2.amazonaws.com/mysql:$VERSION
docker push 635198228996.dkr.ecr.ap-southeast-2.amazonaws.com/mysql:$VERSION

echo "Done!"
