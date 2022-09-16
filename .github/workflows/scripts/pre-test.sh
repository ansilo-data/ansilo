#!/bin/bash

set -e

echo "----- Getting public ip -----"
PUB_IP=$(curl https://ipinfo.io/json | jq -r .ip)
echo "Public ip: $PUB_IP"
echo "PUB_IP=$PUB_IP" >> $GITHUB_ENV
echo ""

echo "----- Authorizing inbound from $PUB_IP -----"
aws ec2 authorize-security-group-ingress \
    --group-id=sg-080dc71fb99e4fcb5 \
    --ip-permissions="IpProtocol=tcp,FromPort=0,ToPort=65536,IpRanges=[{CidrIp=$PUB_IP/32,Description='Authorise traffic from $PUB_IP@github-actions'}]"
echo ""
