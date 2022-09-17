#!/bin/bash

set -e

echo "----- Getting public ip -----"
PUB_IP=$(curl https://ipinfo.io/json | jq -r .ip)
echo "Public ip: $PUB_IP"
echo ""

echo "----- Authorizing inbound from $PUB_IP -----"
aws ec2 authorize-security-group-ingress \
    --group-id=sg-080dc71fb99e4fcb5 \
    --ip-permissions="IpProtocol=tcp,FromPort=0,ToPort=65535,IpRanges=[{CidrIp=$PUB_IP/32,Description='Authorise traffic from $PUB_IP@github-actions'}]"
echo ""

echo "----- Configuring ecs-cli -----"
aws ec2 authorize-security-group-ingress \
ecs-cli configure \
        --cluster dev-cluster \
        --region ${AWS_REGION}
ecs-cli configure profile \
        --profile-name default \
        --access-key ${AWS_ACCESS_KEY_ID} \
        --secret-key ${AWS_SECRET_ACCESS_KEY}
echo ""

echo "----- Installing cargo pgx -----"
cargo install cargo-pgx --version 0.5.0-beta.0 
cargo pgx init --pg14 /usr/pgsql-14/bin/pg_config
echo ""
