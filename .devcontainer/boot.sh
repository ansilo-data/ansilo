#/bin/bash

# Dev-container startup script

set -e

DEV_USER=$1

if [[ -z "$DEV_USER" ]];
then
    echo "Usage $0 [user]"
    exit 1
fi

echo "Setup EFS homedir for user $DEV_USER..."
sudo mkdir -p /efs/workspace/$DEV_USER/ansilo
sudo ln -s /efs/workspace/$DEV_USER /workspace
sudo chown $USER:$USER /efs/workspace/$DEV_USER
sudo chown $USER:$USER /efs/workspace/$DEV_USER/ansilo
sudo chown $USER:$USER /workspace

echo "Running sshd in foreground..."
sudo /usr/sbin/sshd -De
