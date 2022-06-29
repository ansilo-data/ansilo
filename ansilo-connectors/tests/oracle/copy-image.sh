#/bin/bash

# copies an oracle image to our ECR
# requires docker login using credentials from container-registry.oracle.com

set -e

VERSION=$1
EDITION=${2:-"enterprise"}

if [[ -z $VERSION ]];
then
    echo "usage $0 [version tag] [edition=enterprise|standard]"
    exit 1
fi

docker pull container-registry.oracle.com/database/$EDITION:$VERSION
docker tag container-registry.oracle.com/database/$EDITION:$VERSION 635198228996.dkr.ecr.ap-southeast-2.amazonaws.com/oracle:$EDITION-$VERSION
docker push 635198228996.dkr.ecr.ap-southeast-2.amazonaws.com/oracle:$EDITION-$VERSION
echo "Done!"