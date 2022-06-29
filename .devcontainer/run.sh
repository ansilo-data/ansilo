#/bin/bash

# Runs the container on the ECS cluster for remote development

set -e

DEV_USER=${1:-"elliot"}
export AWS_PROFILE=${2:-"ansilo"}
export AWS_DEFAULT_REGION=${3:-"ap-southeast-2"}

if [[ -z "$DEV_USER" ]] || [[ -z "$AWS_PROFILE" ]];
then
    echo "Usage: $0 [user] [aws profile=ansilo] [region]"
    exit 1
fi

DIR=$(realpath $(dirname $0))
ECS_CLUSTER=dev-cluster
SECURITY_GROUP_NAME=dev-container-sg-$DEV_USER
VPC_ID=vpc-0eca650e59ad5b70e
LOCAL_PORT=8222
export AWS_PAGER=""

echo "Retrieving security group $SECURITY_GROUP_NAME..."
SECURITY_GROUP_JSON=$(aws ec2 describe-security-groups --filters="Name=group-name,Values=$SECURITY_GROUP_NAME" --query='SecurityGroups[0]')

if [[ -z $SECURITY_GROUP_JSON ]] || [[ "$SECURITY_GROUP_JSON" == "null" ]];
then
    echo "Could not find security group $SECURITY_GROUP_NAME, creating..."
    SECURITY_GROUP_JSON=$(aws ec2 create-security-group --group-name $SECURITY_GROUP_NAME --description "My security group" --vpc-id $VPC_ID)
fi

SECURITY_GROUP_ID=$(echo $SECURITY_GROUP_JSON | jq -r '.GroupId')
echo "Found security group $SECURITY_GROUP_NAME ($SECURITY_GROUP_ID)"

CURRENT_IP_ADDRESS=$(curl -sSf https://ipinfo.io/json | jq -r '.ip')
echo "Current outbound IP: $CURRENT_IP_ADDRESS"

echo "Revoking existing ingress..."
aws ec2 revoke-security-group-ingress --group-id $SECURITY_GROUP_ID --ip-permissions \
        "$(aws ec2 describe-security-groups --output json --group-ids $SECURITY_GROUP_ID --query "SecurityGroups[0].IpPermissions")"

echo "Will authorise access from $CURRENT_IP_ADDRESS/32 over 22/tcp to $SECURITY_GROUP_NAME"
aws ec2 authorize-security-group-ingress \
    --group-id=$SECURITY_GROUP_ID \
    --ip-permissions="IpProtocol=tcp,FromPort=22,ToPort=22,IpRanges=[{CidrIp=$CURRENT_IP_ADDRESS/32,Description='Authorise port $PORT/$PROTOCOL from $DEV_USER@$HOSTNAME'}]"
echo "Authorised!"

NETWORK_CONF=awsvpcConfiguration={subnets=[subnet-044878cdd1f4b0d3d],securityGroups=[sg-080dc71fb99e4fcb5,$SECURITY_GROUP_ID],assignPublicIp=ENABLED}

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
    --cli-input-json "$(cat $DIR/ecs-task-definition.json | sed s/{{USER}}/$DEV_USER/g)")

TASK_DEF_ARN=$(echo $TASK_DEF | jq -r .taskDefinition.taskDefinitionArn)
echo "Created task def: $TASK_DEF_ARN"

echo "Running task..."
TASK=$(aws ecs run-task \
    --cluster $ECS_CLUSTER \
    --capacity-provider-strategy capacityProvider=FARGATE_SPOT,weight=1 \
    --network-configuration $NETWORK_CONF \
    --task-definition $TASK_DEF_ARN)

echo $TASK
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

echo "Waiting for sshd on remote task container at $PUB_IP"
TRIES=0
while [[ $TRIES -le 10 ]];
do
    echo "Connecting..."
    set +e
    CON_CODE=$(timeout 2 nc -vz $PUB_IP 22; echo $?)
    set -e

    if [[ $CON_CODE == 0 ]];
    then 
        break
    fi
    echo "Failed to connect, sleeping..."
    sleep 10
done

echo "SSH is listening on devcontainer..."
echo "Updating ~/.ssh/config"

cp -a ~/.ssh/config ~/.ssh/config.bk

set +e
if [[ $(grep ansilodev ~/.ssh/config > /dev/null; echo $?) == "0" ]];
then
    set -e
    sed -i.bk "s/.*#ansilodevcontainerip/HostName $PUB_IP #ansilodevcontainerip/" ~/.ssh/config
else
    set -e
    echo "
Host ansilodev
HostName $PUB_IP #ansilodevcontainerip
User vscode
StrictHostKeyChecking no
" >> ~/.ssh/config
fi

echo "Ready to ssh ansilodev! sleeping..."
while :; do sleep 100; done