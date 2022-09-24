#!/bin/bash

set -e

export AWS_PAGER=""
export AWS_DEFAULT_REGION=ap-southeast-2
export AWS_REGION=ap-southeast-2

echo "----- Getting public ip -----"
PUB_IP=$(curl https://ipinfo.io/json | jq -r .ip)
echo "Public ip: $PUB_IP"
echo ""

echo "----- Revoking inbound from $PUB_IP -----"
aws ec2 revoke-security-group-ingress \
    --region ap-southeast-2 \
    --group-id=sg-080dc71fb99e4fcb5 \
    --ip-permissions="IpProtocol=tcp,FromPort=0,ToPort=65535,IpRanges=[{CidrIp=$PUB_IP/32}]" || true
echo ""

echo "----- Stopping ecs tasks with prefix gha-$GHA_RUN_ID -----"
TASK_ARNS=$(aws ecs list-tasks \
    --cluster dev-cluster \
    --query 'taskArns' \
    --output text)
echo "Running tasks: $TASK_ARNS"

if [[ ! -z $TASK_ARNS ]];
then
    FILTERED_ARNS=$(aws ecs describe-tasks \
        --cluster dev-cluster \
        --tasks $TASK_ARNS \
        --query "tasks[?starts_with(group, \`task:gha-$GHA_RUN_ID\`)].taskArn" \
        --output text)
    echo "Tasks from current action: $FILTERED_ARNS"

    for TASK_ARN in $FILTERED_ARNS;
    do
        echo "Stopping task $TASK_ARN"
        aws ecs stop-task \
            --cluster dev-cluster \
            --task $TASK_ARN || true
    done
    echo ""
fi