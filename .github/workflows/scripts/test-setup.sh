#!/bin/bash

set -e

export AWS_PAGER=""
export AWS_DEFAULT_REGION=ap-southeast-2
export AWS_REGION=ap-southeast-2
export RUST_BACKTRACE=1
export ANSILO_TESTS_ECS_USE_PUBLIC_IP=true
export ANSILO_TEST_PG_DIR=/usr/pgsql-14/

echo "----- Getting public ip -----"
PUB_IP=$(curl https://ipinfo.io/json | jq -r .ip)
echo "Public ip: $PUB_IP"
echo ""

echo "----- Authorizing inbound from $PUB_IP -----"
aws ec2 authorize-security-group-ingress \
    --group-id=sg-080dc71fb99e4fcb5 \
    --ip-permissions="IpProtocol=tcp,FromPort=0,ToPort=65535,IpRanges=[{CidrIp=$PUB_IP/32,Description='Authorise traffic from $PUB_IP@github-actions'}]"
echo ""

echo "----- Running tests -----"
cargo test
echo ""

echo "----- Running benches -----"
cargo bench
echo ""

echo "----- Revoking inbound from $PUB_IP -----"
aws ec2 revoke-security-group-ingress \
    --group-id=sg-080dc71fb99e4fcb5 \
    --ip-permissions="IpProtocol=tcp,FromPort=0,ToPort=65535,IpRanges=[{CidrIp=$PUB_IP/32}]" || true
echo ""
