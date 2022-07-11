#/bin/bash

# Dev-container startup script

set -e

DEV_USER=$1
LISTEN_PORT=${2:-"22"}
TIMEOUT_DURATION=1800

if [[ -z "$DEV_USER" ]];
then
    echo "Usage $0 [user] [ssh listen port=22]"
    exit 1
fi

echo "Setup EFS homedir for user $DEV_USER..."
sudo mkdir -p /efs/workspace/$DEV_USER/ansilo
sudo ln -s /efs/workspace/$DEV_USER /workspace
sudo chown $USER:$USER /efs/workspace/$DEV_USER
sudo chown $USER:$USER /efs/workspace/$DEV_USER/ansilo
sudo chown $USER:$USER /workspace

echo "Running sshd on port 2222 ..."
sudo /usr/sbin/sshd -De -o ListenAddress=127.0.0.1 -p 2222 &
SSHD_PID=$$
echo "Started as pid $SSHD_PID"

echo "Running lazyprox"
sudo lazyprox \
    --listen 0.0.0.0:22 \
    --dest localhost:2222 \
    --idle-timeout-secs $TIMEOUT_DURATION

