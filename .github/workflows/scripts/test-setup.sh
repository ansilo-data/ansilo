#!/bin/bash

set -e

export AWS_PAGER=""
export AWS_DEFAULT_REGION=ap-southeast-2
export AWS_REGION=ap-southeast-2

echo "----- Getting public ip -----"
PUB_IP=$(curl https://ipinfo.io/json | jq -r .ip)
echo "Public ip: $PUB_IP"
echo ""

echo "----- Authorizing inbound from $PUB_IP -----"
aws ec2 authorize-security-group-ingress \
    --region ap-southeast-2 \
    --group-id=sg-080dc71fb99e4fcb5 \
    --ip-permissions="IpProtocol=tcp,FromPort=0,ToPort=65535,IpRanges=[{CidrIp=$PUB_IP/32,Description='Authorise traffic from $PUB_IP@github-actions'}]"
echo ""

echo "----- Configuring ecs-cli -----"
ecs-cli configure \
        --cluster dev-cluster \
        --region ap-southeast-2
ecs-cli configure profile \
        --profile-name default \
        --access-key ${AWS_ACCESS_KEY_ID} \
        --secret-key ${AWS_SECRET_ACCESS_KEY}
echo ""

echo "----- Freeing disk space -----"
df -h
# Workaround to provide additional free space for testing.
#   https://github.com/actions/virtual-environments/issues/2840
sudo rm -rf /usr/share/dotnet
sudo rm -rf /usr/local/lib/android
sudo rm -rf /opt/ghc
sudo rm -rf "/usr/local/share/boost"
sudo rm -rf "$AGENT_TOOLSDIRECTORY"
df -h
echo ""