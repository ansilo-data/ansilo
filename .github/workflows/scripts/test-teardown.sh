#!/bin/bash

set -e

echo "----- Getting public ip -----"
PUB_IP=$(curl https://ipinfo.io/json | jq -r .ip)
echo "Public ip: $PUB_IP"
echo ""

echo "----- Revoking inbound from $PUB_IP -----"
aws ec2 revoke-security-group-ingress \
    --group-id=sg-080dc71fb99e4fcb5 \
    --ip-permissions="IpProtocol=tcp,FromPort=0,ToPort=65535,IpRanges=[{CidrIp=$PUB_IP/32}]" || true
echo ""
