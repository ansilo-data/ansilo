#/bin/bash

# Dev instance startup script

set -e

export LC_CTYPE=C.UTF-8
export LC_ALL=C.UTF-8

ECR_REPO="635198228996.dkr.ecr.ap-southeast-2.amazonaws.com"
DEVCONTAINER_IMAGE="$ECR_REPO/devcontainer:latest"

TRIES=0
while ((TRIES < 10));
do
    echo "Installing deps..."
    set +e
    sudo apt-get update
    DEBIAN_FRONTEND=noninteractive sudo apt-get install -y zip netcat socat
    EXIT_CODE=$?
    set -e

    if [[ $EXIT_CODE == 0 ]];
    then
        break
    fi
    
    sleep 5
    let "TRIES+=1"
done

echo "Finding device for EBS $EBS_ID..."
VOL_SERIAL=$(echo $EBS_ID | cut -d'-' -f2)
EBS_DEV=$(lsblk -o +SERIAL | grep $VOL_SERIAL | awk '{print $1}')
echo "Found device $EBS_DEV..."

echo "Ensuring xfs present..."
sudo mkfs -t xfs /dev/$EBS_DEV || true 

echo "Mounting EBS $EBS_DEV..."
sudo mkdir -p /storage
sudo mount /dev/$EBS_DEV /storage
echo "Mounted!"

echo "Installing docker..."
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
echo "Docker installed"

echo "Installing awscli..."
# Install aws cli
curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
unzip /tmp/awscliv2.zip 
sudo ./aws/install
rm -rf /tmp/awscliv2.zip 

echo "Pulling devcontainer ($DEVCONTAINER_IMAGE) ..."
aws ecr get-login-password | sudo docker login --username AWS --password-stdin $ECR_REPO
sudo docker pull $DEVCONTAINER_IMAGE

echo "Starting devcontainer..."
sudo docker run \
    --detach \
    --privileged \
    -p 2222:22 \
    --tmpfs /tmp:exec \
    --volume /storage:/store \
    --entrypoint /bin/bash \
    $DEVCONTAINER_IMAGE /boot.sh $DEV_USER $SPOT_FLEET_ID
echo "Container started..."

echo "Instance provisioned!"
