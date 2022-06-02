#/bin/bash

set -e

docker build -t rust-devcontainer \
 --build-arg CARGO_HOME=/workspace/ansilo/.cargo \
 --build-arg RUSTUP_HOME=/workspace/ansilo/.rustup \
 .

docker tag rust-devcontainer 059160323628.dkr.ecr.ap-southeast-2.amazonaws.com/rust-devcontainer:latest

aws ecr get-login-password --region ap-southeast-2 | docker login --username AWS --password-stdin 059160323628.dkr.ecr.ap-southeast-2.amazonaws.com

docker push 059160323628.dkr.ecr.ap-southeast-2.amazonaws.com/rust-devcontainer:latest