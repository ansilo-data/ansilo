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
