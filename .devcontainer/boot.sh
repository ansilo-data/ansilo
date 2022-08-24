#/bin/bash

# Dev-container startup script

set -e

DEV_USER=$1
SPOT_FLEET_ID=$2
LISTEN_PORT=${3:-"22"}
TIMEOUT_DURATION=1800

if [[ -z "$DEV_USER" ]];
then
    echo "Usage $0 [user] [spot fleet id] [ssh listen port=22]"
    exit 1
fi

echo "Setup init env scripts for user $DEV_USER..."
echo "export DEV_USER=\"$DEV_USER\"" | tee -a ~/.initenvrc
echo "export WORKSPACE_HOME=\"/store/workspace/$DEV_USER\"" | tee -a ~/.initenvrc

echo "Setup homedir for user $DEV_USER..."
sudo mkdir -p /store/workspace/$DEV_USER/ansilo
sudo chown $USER:$USER /store/workspace/$DEV_USER
sudo chown $USER:$USER /store/workspace/$DEV_USER/ansilo

echo "Installing SSH keys..."
echo 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDOIgym/c2xk7m+WbgHrm0joP87hQohUjNpxkkSsF5bCdIxAH4F8FsBkRYTnKZUB0eaTnf798lFSQ2IDgzDUOM/9FO/7efzhD30+d+ZBYNYDj9+vXHCe/7XfrwYrWZyLUlV0CAmBhqgCwbL5jPoWRDRDj6yYcM8QWVyOeNOq/Cqqisfi41xztj+q8c93p00rqCT/RytRxnxFzt961Tq2jQOl4Zh0d5i9czr1QNMQl4d1mIwgWitcTXUKAeWLmhaZdIhD1ePxXX5yrH9IKcd04InM1FJxm4nlPLKedFZ0n31Y8U4UTyWPl5yB9dCPbekFsP+Z9PTVElVIcxDKianvJtH elliotlevin@MacBook-Pro' \
    >> ~/.ssh/authorized_keys

echo "Running sshd on port 2222 ..."
sudo /usr/sbin/sshd -De -o ListenAddress=127.0.0.1 -p 2222 &
SSHD_PID=$$
echo "Started as pid $SSHD_PID"

echo "Running lazyprox"
sudo lazyprox \
    --listen 0.0.0.0:22 \
    --dest localhost:2222 \
    --idle-timeout-secs $TIMEOUT_DURATION || true

if [[ ! -z "$SPOT_FLEET_ID" ]];
then
    echo "Terminating spot instance fleet $SPOT_FLEET_ID..."
    aws ec2 cancel-spot-fleet-requests \
            --spot-fleet-request-ids $SPOT_FLEET_ID
    echo "Terminated!"
fi
