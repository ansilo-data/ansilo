#/bin/bash

# Runs the container on the ECS cluster for remote development

set -e

DEV_USER=${1:-"elliot"}
export AWS_PROFILE=${2:-"ansilo"}
export AWS_DEFAULT_REGION=${3:-"ap-southeast-2"}
export AWS_PAGER=""

if [[ -z "$DEV_USER" ]] || [[ -z "$AWS_PROFILE" ]];
then
    echo "Usage: $0 [user] [aws profile=ansilo] [region]"
    exit 1
fi

DIR=$(realpath $(dirname $0))
SECURITY_GROUP_NAME="dev-container-sg-$DEV_USER"
VPC_ID="vpc-0eca650e59ad5b70e"
AVAILABILITY_ZONE="${AWS_DEFAULT_REGION}a"
EBS_NAME=devcontainer-storage-$DEV_USER
EBS_SIZE=150

echo "Retrieving EBS Volume $EBS_NAME..."
EBS_VOLUME_JSON=$(aws ec2 describe-volumes --filters="Name=tag:Name,Values=$EBS_NAME" --query='Volumes[0]')

if [[ -z $EBS_VOLUME_JSON ]] || [[ "$EBS_VOLUME_JSON" == "null" ]];
then
    echo "Could not find EBS volume $EBS_NAME, creating..."
    EBS_VOLUME_JSON=$(aws ec2 create-volume \
        --availability-zone $AVAILABILITY_ZONE \
        --volume-type gp3 \
        --size $EBS_SIZE \
        --tag-specifications "ResourceType=volume,Tags=[{Key=Name,Value=$EBS_NAME}]")
fi

EBS_ID=$(echo $EBS_VOLUME_JSON | jq -r '.VolumeId')
echo "Found EBS volume: $EBS_ID ($EBS_NAME)"

echo "Retrieving security group $SECURITY_GROUP_NAME..."
SECURITY_GROUP_JSON=$(aws ec2 describe-security-groups --filters="Name=group-name,Values=$SECURITY_GROUP_NAME" --query='SecurityGroups[0]')

if [[ -z $SECURITY_GROUP_JSON ]] || [[ "$SECURITY_GROUP_JSON" == "null" ]];
then
    echo "Could not find security group $SECURITY_GROUP_NAME, creating..."
    SECURITY_GROUP_JSON=$(aws ec2 create-security-group --group-name $SECURITY_GROUP_NAME --description "Devcontainer security group - $DEV_USER" --vpc-id $VPC_ID)
fi

SECURITY_GROUP_ID=$(echo $SECURITY_GROUP_JSON | jq -r '.GroupId')
echo "Found security group $SECURITY_GROUP_NAME ($SECURITY_GROUP_ID)"

CURRENT_IP_ADDRESS=$(curl -sSf https://ipinfo.io/json | jq -r '.ip')
echo "Current outbound IP: $CURRENT_IP_ADDRESS"

echo "Revoking existing ingress..."
aws ec2 revoke-security-group-ingress --group-id $SECURITY_GROUP_ID --ip-permissions \
        "$(aws ec2 describe-security-groups --output json --group-ids $SECURITY_GROUP_ID --query "SecurityGroups[0].IpPermissions")"


echo "Will authorise access from $CURRENT_IP_ADDRESS/32 over 22/tcp and 2222/tcp to $SECURITY_GROUP_NAME"
aws ec2 authorize-security-group-ingress \
    --group-id=$SECURITY_GROUP_ID \
    --ip-permissions="IpProtocol=tcp,FromPort=22,ToPort=22,IpRanges=[{CidrIp=$CURRENT_IP_ADDRESS/32,Description='Authorise ssh from $DEV_USER@$HOSTNAME'}]"
aws ec2 authorize-security-group-ingress \
    --group-id=$SECURITY_GROUP_ID \
    --ip-permissions="IpProtocol=tcp,FromPort=2222,ToPort=2222,IpRanges=[{CidrIp=$CURRENT_IP_ADDRESS/32,Description='Authorise ssh from $DEV_USER@$HOSTNAME'}]"
echo "Authorised!"

cleanup() {
    if [[ ! -z $SPOT_FLEET_ID ]];
    then
        echo "Stopping spot fleet $SPOT_FLEET_ID..."
        aws ec2 cancel-spot-fleet-requests \
            --spot-fleet-request-ids $SPOT_FLEET_ID \
            >/dev/null 2>&1
        echo "Fleet stopped!"
    fi

    echo "Done!"
}
trap cleanup EXIT

echo "Requesting spot fleet..."
SPOT_FLEET_ID=$(aws ec2 request-spot-fleet \
    --spot-fleet-request-config file://${DIR}/spot-fleet-config.json \
    --query 'SpotFleetRequestId' \
    --output text)
echo "Requested spot fleet: $SPOT_FLEET_ID"

echo "Waiting for spot fleet instance on in $SPOT_FLEET_ID"
TRIES=0
while [[ $TRIES -le 10 ]];
do
    SPOT_INSTANCE_JSON=$(aws ec2 describe-spot-fleet-instances \
        --spot-fleet-request-id $SPOT_FLEET_ID \
        --query 'ActiveInstances[0]')

    if [[ -z $SPOT_INSTANCE_JSON ]] || [[ "$SPOT_INSTANCE_JSON" == "null" ]];
    then 
        echo "No instance found, sleeping..."
        sleep 10
        continue
    fi

    echo "Found instance: $SPOT_INSTANCE_JSON"
    SPOT_INSTANCE_ID=$(echo $SPOT_INSTANCE_JSON | jq -r '.InstanceId')
    echo "Spot instance ID: $SPOT_INSTANCE_ID"
    break
done

echo "Getting public IP from $SPOT_INSTANCE_ID..."
PUB_IP=$(aws ec2 describe-instances \
    --filters "Name=instance-id,Values=$SPOT_INSTANCE_ID" \
    --query 'Reservations[*].Instances[*].PublicIpAddress' \
    --output text)
echo "Found instance ip: $PUB_IP"

echo "Waiting for sshd on spot instance at $PUB_IP"
TRIES=0
while [[ $TRIES -le 20 ]];
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
    sleep 5
done

echo "SSH is listening on instance..."

echo "Attaching volume $EBS_ID to instance $SPOT_INSTANCE_ID..."
aws ec2 attach-volume \
    --instance-id $SPOT_INSTANCE_ID \
    --volume-id $EBS_ID \
    --device /dev/xvdf
echo "Volume attached"

echo "Updating ~/.ssh/config with ansilodevinstance"
cp -a ~/.ssh/config ~/.ssh/config.bk

set +e
if [[ $(grep ansilodevinstanceip ~/.ssh/config > /dev/null; echo $?) == "0" ]];
then
    set -e
    sed -i.bk "s/.*#ansilodevinstanceip/HostName $PUB_IP #ansilodevinstanceip/" ~/.ssh/config
else
    set -e
    echo "
Host ansilodevinstance
HostName $PUB_IP #ansilodevinstanceip
User ubuntu
StrictHostKeyChecking no
" >> ~/.ssh/config
fi

echo "Provisioning instance..."
ssh ansilodevinstance \
    SPOT_FLEET_ID=$SPOT_FLEET_ID \
    EBS_ID=$EBS_ID \
    DEV_USER=$DEV_USER \
    "bash -s" < $DIR/init-instance.sh
echo "Instance provisioned!"

echo "Waiting for sshd on devcontainer at $PUB_IP"
TRIES=0
while [[ $TRIES -le 20 ]];
do
    echo "Connecting..."
    set +e
    CON_CODE=$(timeout 2 nc -vz $PUB_IP 2222; echo $?)
    set -e

    if [[ $CON_CODE == 0 ]];
    then 
        break
    fi
    echo "Failed to connect, sleeping..."
    sleep 5
done

echo "Updating ~/.ssh/config with ansilodev"
cp -a ~/.ssh/config ~/.ssh/config.bk

set +e
if [[ $(grep ansilodevcontainerip ~/.ssh/config > /dev/null; echo $?) == "0" ]];
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
Post 2222
" >> ~/.ssh/config
fi

echo "Ready to ssh ansilodev! sleeping..."
while :; do sleep 100; done