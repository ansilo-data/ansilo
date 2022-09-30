#/bin/bash

# requires docker login for ECR

set -e

VERSION=$1

if [[ -z $VERSION ]];
then
    echo "usage $0 [version tag]"
    exit 1
fi

docker pull vault:$VERSION
docker tag vault:$VERSION vault-base
docker build -t vault-test .
aws ecr get-login-password --region ap-southeast-2 | docker login --username AWS --password-stdin 635198228996.dkr.ecr.ap-southeast-2.amazonaws.com
docker tag vault-test 635198228996.dkr.ecr.ap-southeast-2.amazonaws.com/vault:$VERSION
docker push 635198228996.dkr.ecr.ap-southeast-2.amazonaws.com/vault:$VERSION

echo "Done!"
