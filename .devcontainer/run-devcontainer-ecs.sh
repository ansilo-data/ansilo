#/bin/bash

# Runs the container on the ECS cluster for remote development

set -e

DIR=$(realpath $(dirname $0))
AWS_PROFILE=default
ECS_CLUSTER=dev-cluster
NETWORK_CONF=awsvpcConfiguration={subnets=[subnet-52bdaa25],securityGroups=[sg-0107f28eb0e25a4c2],assignPublicIp=ENABLED}
LOCAL_PORT=8222
export AWS_PAGER=""

cleanup() {
    if [[ ! -z $TASK_ARN ]];
    then
        echo "Stopping task $TASK_ARN..."
        aws ecs stop-task \
            --cluster $ECS_CLUSTER \
            --task $TASK_ARN >/dev/null 2>&1
        echo "Task stopped!"
    fi

    echo "Done!"
}
trap cleanup EXIT

echo "Creating task definition"
TASK_DEF=$(aws ecs register-task-definition \
    --cli-input-json "$(cat $DIR/ecs-task-definition.json)")

TASK_DEF_ARN=$(echo $TASK_DEF | jq -r .taskDefinition.taskDefinitionArn)
echo "Created task def: $TASK_DEF_ARN"

echo "Running task..."
TASK=$(aws ecs run-task \
    --cluster $ECS_CLUSTER \
    --capacity-provider-strategy capacityProvider=FARGATE_SPOT,weight=1 \
    --network-configuration $NETWORK_CONF \
    --task-definition $TASK_DEF_ARN)

TASK_ARN=$(echo $TASK | jq -r .tasks[0].taskArn)
echo "Task started: $TASK_ARN"
sleep 5

echo "Getting ENI ID..."
TASK_ENI_ID=$(aws ecs describe-tasks \
     --cluster $ECS_CLUSTER \
     --task $TASK_ARN \
     --query 'tasks[0].attachments[0].details[?name==`networkInterfaceId`].value' \
     --output text)

echo "Getting public IP from $TASK_ENI_ID..."
PUB_IP=$(aws ec2 describe-network-interfaces \
     --network-interface-ids "$TASK_ENI_ID" \
     --query 'NetworkInterfaces[0].Association.PublicIp' \
     --output text)

echo "Fowarding local port $LOCAL_PORT to task container at $PUB_IP:22"
TRIES=0
while [[ $TRIES -le 10 ]];
do
    echo "Connecting..."
    SSH_OUT=$(ssh -oStrictHostKeyChecking=no -NL $LOCAL_PORT:localhost:22 vscode@$PUB_IP 2>&1 || true)
    if [[ $SSH_OUT != *"Connection refused"* ]];
    then 
        exit 0
    fi
    echo "Failed to connect, sleeping..."
    sleep 10
done
