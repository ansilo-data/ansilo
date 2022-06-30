#/bin/bash

# requires docker login using credentials from https://container-registry.oracle.com
# requires docker login for ECR

set -e

VERSION=$1
EDITION=${2:-"enterprise"}

if [[ -z $VERSION ]];
then
    echo "usage $0 [version tag] [edition=enterprise|standard]"
    exit 1
fi

docker pull container-registry.oracle.com/database/$EDITION:$VERSION
docker tag container-registry.oracle.com/database/$EDITION:$VERSION oracle-base
docker build -t oracle-optimised .
aws ecr get-login-password --region ap-southeast-2 --profile ansilo | docker login --username AWS --password-stdin 635198228996.dkr.ecr.ap-southeast-2.amazonaws.com
docker tag oracle-optimised 635198228996.dkr.ecr.ap-southeast-2.amazonaws.com/oracle:$EDITION-$VERSION
docker push 635198228996.dkr.ecr.ap-southeast-2.amazonaws.com/oracle:$EDITION-$VERSION

echo "Done!"
