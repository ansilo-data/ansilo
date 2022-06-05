ARG VARIANT="bullseye"
FROM mcr.microsoft.com/vscode/devcontainers/rust:1-${VARIANT}

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive && \
    apt-get -y install --no-install-recommends \
    curl vim htop ssh openssl libssl-dev sudo \
    # postges reqs
    build-essential pkg-config \
    libreadline-dev zlib1g-dev flex bison libxml2-dev libxslt-dev libssl-dev libxml2-utils xsltproc clang

RUN mkdir -p /run/sshd

USER vscode
RUN mkdir -p ~/.ssh
RUN echo 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDOIgym/c2xk7m+WbgHrm0joP87hQohUjNpxkkSsF5bCdIxAH4F8FsBkRYTnKZUB0eaTnf798lFSQ2IDgzDUOM/9FO/7efzhD30+d+ZBYNYDj9+vXHCe/7XfrwYrWZyLUlV0CAmBhqgCwbL5jPoWRDRDj6yYcM8QWVyOeNOq/Cqqisfi41xztj+q8c93p00rqCT/RytRxnxFzt961Tq2jQOl4Zh0d5i9czr1QNMQl4d1mIwgWitcTXUKAeWLmhaZdIhD1ePxXX5yrH9IKcd04InM1FJxm4nlPLKedFZ0n31Y8U4UTyWPl5yB9dCPbekFsP+Z9PTVElVIcxDKianvJtH elliotlevin@MacBook-Pro' \
    >> ~/.ssh/authorized_keys

RUN cargo install cargo-pgx
RUN cargo pgx init --pg14 download

RUN cargo install cargo-edit

RUN echo 'export CARGO_HOME="/workspace/ansilo/.cargo"' >> ~/.bashrc && \
    echo 'if [[ "${PATH}" != *"${CARGO_HOME}/bin"* ]]; then export PATH="${CARGO_HOME}/bin:${PATH}"; fi' >> ~/.bashrc
RUN echo 'export CARGO_HOME="/workspace/ansilo/.cargo"' >> ~/.zshrc && \
    echo 'if [[ "${PATH}" != *"${CARGO_HOME}/bin"* ]]; then export PATH="${CARGO_HOME}/bin:${PATH}"; fi' >> ~/.zshrc
