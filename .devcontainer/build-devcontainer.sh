#/bin/bash

docker build -t rustdevconatiner --env-file .env .
docker tag rustdevcontainer:latest 059160323628.dkr.ecr.ap-southeast-2.amazonaws.com/rust-devcontainer:latest
aws ecr get-login-password --region ap-southeast-2 | docker login --username AWS --password-stdin 059160323628.dkr.ecr.ap-southeast-2.amazonaws.com
docker push 059160323628.dkr.ecr.ap-southeast-2.amazonaws.com/rust-devcontainer:latest