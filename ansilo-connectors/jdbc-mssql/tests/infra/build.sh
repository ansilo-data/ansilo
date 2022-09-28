#/bin/bash

# requires docker login for ECR

set -e

VERSION=$1

if [[ -z $VERSION ]];
then
    echo "usage $0 [version tag]"
    exit 1
fi

docker pull mcr.microsoft.com/mssql/server:$VERSION-latest
docker tag mcr.microsoft.com/mssql/server:$VERSION-latest mssql-base
docker build -t mssql-test .
aws ecr get-login-password --region ap-southeast-2 | docker login --username AWS --password-stdin 635198228996.dkr.ecr.ap-southeast-2.amazonaws.com
docker tag mssql-test 635198228996.dkr.ecr.ap-southeast-2.amazonaws.com/mssql:$VERSION
docker push 635198228996.dkr.ecr.ap-southeast-2.amazonaws.com/mssql:$VERSION

echo "Done!"
