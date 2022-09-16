#!/bin/bash

set -e

echo "----- Authorizing inbound from $PUB_IP -----"
aws ec2 revoke-security-group-ingress \
    --group-id=sg-080dc71fb99e4fcb5 \
    --ip-permissions="IpProtocol=tcp,FromPort=0,ToPort=65536,IpRanges=[{CidrIp=$PUB_IP/32}]" || true
echo ""
